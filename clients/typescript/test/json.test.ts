//! TS client with the JSON codec, end-to-end against the native greeter server.

import { spawn, type ChildProcess } from "node:child_process";
import { createInterface } from "node:readline";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import WebSocket from "ws";
import { makePromiseClient, type PromiseServiceClient } from "../src/index.js";
import { GreeterDefinition } from "../examples/gen/greeter.js";

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");

let proc: ChildProcess;
let client: PromiseServiceClient<typeof GreeterDefinition>;

async function* fromArray<T>(items: T[]): AsyncIterable<T> {
  for (const item of items) yield item;
}

beforeAll(async () => {
  proc = spawn("cargo", ["run", "--quiet", "-p", "example-greeter-server"], {
    cwd: repoRoot,
    stdio: ["ignore", "pipe", "inherit"],
  });
  const baseUrl = await new Promise<string>((resolve, reject) => {
    const rl = createInterface({ input: proc.stdout! });
    rl.on("line", (line) => {
      const m = line.match(/^LISTENING (\S+)$/);
      if (m) resolve(m[1]);
    });
    proc.on("exit", (code) => reject(new Error(`server exited early: ${code}`)));
  });
  // The JSON codec: messages go over the wire as JSON, not binary protobuf.
  client = makePromiseClient(GreeterDefinition, {
    baseUrl,
    codec: "json",
    webSocketImpl: WebSocket as unknown as typeof globalThis.WebSocket,
  });
}, 60_000);

afterAll(() => {
  client?.close();
  proc?.kill("SIGKILL");
});

describe("json codec", () => {
  it("unary over Fetch (JSON body)", async () => {
    const reply = await client.sayHello({ name: "json" });
    expect(reply.message).toBe("Hello, json!");
  });

  it("server streaming over WebSocket (JSON payloads)", async () => {
    const ticks: number[] = [];
    for await (const t of client.countdown({ from: 3 })) ticks.push(t.value);
    expect(ticks).toEqual([3, 2, 1, 0]);
  });

  it("bidi streaming over WebSocket (JSON payloads)", async () => {
    const got: string[] = [];
    for await (const m of client.chat(fromArray([{ text: "hi" }, { text: "yo" }]))) {
      got.push(m.text);
    }
    expect(got).toEqual(["echo: hi", "echo: yo"]);
  });
});
