import { spawn, type ChildProcess } from "node:child_process";
import { createInterface } from "node:readline";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import WebSocket from "ws";
import { makeClient, Metadata, type ServiceClient } from "../src/index.js";
import { EchoDefinition } from "./gen/echo.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
// node/packages/client/test -> repo root is four levels up; the Cargo workspace lives in rust/.
const repoRoot = path.resolve(__dirname, "../../../..");
const rustRoot = path.join(repoRoot, "rust");

let proc: ChildProcess;
let client: ServiceClient<typeof EchoDefinition>;
let baseUrl: string;

beforeAll(async () => {
  proc = spawn("cargo", ["run", "--quiet", "-p", "devserver"], {
    cwd: rustRoot,
    stdio: ["ignore", "pipe", "inherit"],
  });
  baseUrl = await new Promise<string>((resolve, reject) => {
    const rl = createInterface({ input: proc.stdout! });
    rl.on("line", (line) => {
      const m = line.match(/^LISTENING (\S+)$/);
      if (m) resolve(m[1]);
    });
    proc.on("exit", (code) => reject(new Error(`devserver exited early: ${code}`)));
  });
  // Pin the custom transports: this suite exercises the Fetch-unary + custom-WS paths
  // (proto now defaults to h2ts).
  client = makeClient(EchoDefinition, {
    baseUrl,
    webSocketImpl: WebSocket as unknown as typeof globalThis.WebSocket,
    unary: "fetch",
    streaming: "ws",
  });
}, 60_000);

afterAll(() => {
  client?.close();
  proc?.kill("SIGKILL");
});

describe("generated client -> proxy -> echo", () => {
  it("unary over Fetch", async () => {
    const res = await new Promise<{ message: string }>((resolve, reject) => {
      client.unary({ message: "hello" }, (err, value) =>
        err ? reject(err) : resolve(value!),
      );
    });
    expect(res.message).toBe("hello");
  });

  it("unary with metadata argument", async () => {
    const md = new Metadata();
    md.set("x-trace", "abc");
    const res = await new Promise<{ message: string }>((resolve, reject) => {
      client.unary({ message: "meta" }, md, (err, value) =>
        err ? reject(err) : resolve(value!),
      );
    });
    expect(res.message).toBe("meta");
  });

  it("bidi streaming over WebSocket", async () => {
    const received: string[] = [];
    await new Promise<void>((resolve, reject) => {
      const stream = client.stream();
      stream.on("data", (r) => received.push(r.message));
      stream.on("end", () => resolve());
      stream.on("error", reject);
      stream.write({ message: "a" });
      stream.write({ message: "b" });
      stream.write({ message: "c" });
      stream.end();
    });
    expect(received).toEqual(["a", "b", "c"]);
  });

  it("bidi stream is async-iterable", async () => {
    const stream = client.stream();
    stream.write({ message: "x" });
    stream.write({ message: "y" });
    stream.end();
    const got: string[] = [];
    for await (const r of stream) got.push(r.message);
    expect(got).toEqual(["x", "y"]);
  });

  it("multiplex: two concurrent streams share one socket", async () => {
    const mux = makeClient(EchoDefinition, {
      baseUrl,
      multiplex: true,
      unary: "fetch",
      streaming: "ws",
      webSocketImpl: WebSocket as unknown as typeof globalThis.WebSocket,
    });
    const run = (msgs: string[]) =>
      new Promise<string[]>((resolve, reject) => {
        const got: string[] = [];
        const s = mux.stream();
        s.on("data", (r) => got.push(r.message));
        s.on("end", () => resolve(got));
        s.on("error", reject);
        for (const m of msgs) s.write({ message: m });
        s.end();
      });
    try {
      const [a, b] = await Promise.all([run(["a1", "a2"]), run(["b1", "b2"])]);
      expect(a).toEqual(["a1", "a2"]);
      expect(b).toEqual(["b1", "b2"]);
    } finally {
      mux.close();
    }
  });
});
