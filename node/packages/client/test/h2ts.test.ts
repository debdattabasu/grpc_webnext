//! End-to-end proof of the binary path over **real gRPC via h2ts** (HTTP/2 tunneled
//! over one WebSocket). Both server surfaces are exercised: the in-process greeter
//! (h2ts `serve_h2` straight into tonic) and the standalone proxy (h2ts `bridge`
//! byte-pumping to the upstream). All four RPC cardinalities, plus deadline + cancel.

import { spawn, type ChildProcess } from "node:child_process";
import { createInterface } from "node:readline";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import WebSocket from "ws";
import { makePromiseClient, Status, type PromiseServiceClient } from "../src/index.js";
import { GreeterDefinition } from "../examples/gen/greeter.js";
import { EchoDefinition } from "./gen/echo.js";

// node/packages/client/test -> repo root is four levels up; the Cargo workspace lives in rust/.
const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../../..");
const rustRoot = path.join(repoRoot, "rust");
const wsImpl = WebSocket as unknown as typeof globalThis.WebSocket;

async function* fromArray<T>(items: T[]): AsyncIterable<T> {
  for (const item of items) yield item;
}

/** Spawn a Rust server package and resolve once it prints its LISTENING address. */
function spawnServer(pkg: string): Promise<{ proc: ChildProcess; baseUrl: string }> {
  const proc = spawn("cargo", ["run", "--quiet", "-p", pkg], {
    cwd: rustRoot,
    stdio: ["ignore", "pipe", "inherit"],
  });
  return new Promise((resolve, reject) => {
    const rl = createInterface({ input: proc.stdout! });
    rl.on("line", (line) => {
      const m = line.match(/^LISTENING (\S+)$/);
      if (m) resolve({ proc, baseUrl: m[1] });
    });
    proc.on("exit", (code) => reject(new Error(`${pkg} exited early: ${code}`)));
  });
}

describe("in-process greeter over h2ts (real gRPC via serve_h2)", () => {
  let proc: ChildProcess;
  let client: PromiseServiceClient<typeof GreeterDefinition>;

  beforeAll(async () => {
    const server = await spawnServer("example-greeter-server");
    proc = server.proc;
    client = makePromiseClient(GreeterDefinition, {
      baseUrl: server.baseUrl,
      webSocketImpl: wsImpl,
      h2ts: true,
    });
  }, 60_000);

  afterAll(() => {
    client?.close();
    proc?.kill("SIGKILL");
  });

  it("unary", async () => {
    const reply = await client.sayHello({ name: "h2ts" });
    expect(reply.message).toBe("Hello, h2ts!");
  });

  it("server streaming", async () => {
    const ticks: number[] = [];
    for await (const t of client.countdown({ from: 3 })) ticks.push(t.value);
    expect(ticks).toEqual([3, 2, 1, 0]);
  });

  it("client streaming", async () => {
    const reply = await client.concat(fromArray([{ text: "a" }, { text: "b" }, { text: "c" }]));
    expect(reply.message).toBe("a b c");
  });

  it("bidi streaming", async () => {
    const got: string[] = [];
    for await (const m of client.chat(fromArray([{ text: "one" }, { text: "two" }]))) {
      got.push(m.text);
    }
    expect(got).toEqual(["echo: one", "echo: two"]);
  });

  it("unary deadline fires DEADLINE_EXCEEDED", async () => {
    await expect(
      client.sleep({ millis: 5000 }, { deadline: Date.now() + 150 }),
    ).rejects.toMatchObject({ code: Status.DEADLINE_EXCEEDED });
  });

  it("cancellation via AbortSignal ends the stream with CANCELLED", async () => {
    const ac = new AbortController();
    const source = (async function* () {
      yield { text: "hi" };
      await new Promise<never>(() => {});
    })();
    const responses = client.chat(source, { signal: ac.signal });
    ac.abort();
    await expect(
      (async () => {
        for await (const _ of responses) {
          /* drain */
        }
      })(),
    ).rejects.toMatchObject({ code: Status.CANCELLED });
  });
});

describe("proxy -> echo over h2ts (real gRPC via bridge byte-pump)", () => {
  let proc: ChildProcess;
  let client: PromiseServiceClient<typeof EchoDefinition>;

  beforeAll(async () => {
    const server = await spawnServer("devserver");
    proc = server.proc;
    client = makePromiseClient(EchoDefinition, {
      baseUrl: server.baseUrl,
      webSocketImpl: wsImpl,
      h2ts: true,
    });
  }, 60_000);

  afterAll(() => {
    client?.close();
    proc?.kill("SIGKILL");
  });

  it("unary through the byte-pump proxy", async () => {
    const reply = await client.unary({ message: "through-proxy" });
    expect(reply.message).toBe("through-proxy");
  });

  it("bidi streaming through the byte-pump proxy", async () => {
    const got: string[] = [];
    const source = fromArray([{ message: "a" }, { message: "b" }, { message: "c" }]);
    for await (const m of client.stream(source)) got.push(m.message);
    expect(got).toEqual(["a", "b", "c"]);
  });
});
