import { spawn, type ChildProcess } from "node:child_process";
import { createInterface } from "node:readline";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import WebSocket from "ws";
import { makePromiseClient, Metadata, Status, type PromiseServiceClient } from "../src/index.js";
import { GreeterDefinition } from "../examples/gen/greeter.js";

// node/packages/client/test -> repo root is four levels up; the Cargo workspace lives in rust/.
const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../../..");
const rustRoot = path.join(repoRoot, "rust");

let proc: ChildProcess;
let client: PromiseServiceClient<typeof GreeterDefinition>;

async function* fromArray<T>(items: T[]): AsyncIterable<T> {
  for (const item of items) yield item;
}

beforeAll(async () => {
  proc = spawn("cargo", ["run", "--quiet", "-p", "example-greeter-server"], {
    cwd: rustRoot,
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
  // Pin the custom transports: this suite exercises the Fetch-unary + custom-WS paths
  // (proto now defaults to h2ts).
  client = makePromiseClient(GreeterDefinition, {
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

describe("promise client", () => {
  it("unary returns a promise", async () => {
    const reply = await client.sayHello({ name: "promise" });
    expect(reply.message).toBe("Hello, promise!");
  });

  it("unary surfaces trailing metadata via onTrailer", async () => {
    let trailer: Metadata | undefined;
    await client.sayHello({ name: "x" }, { onTrailer: (md) => (trailer = md) });
    expect(trailer).toBeInstanceOf(Metadata);
  });

  it("server streaming is async-iterable", async () => {
    const ticks: number[] = [];
    for await (const t of client.countdown({ from: 3 })) ticks.push(t.value);
    expect(ticks).toEqual([3, 2, 1, 0]);
  });

  it("client streaming returns a promise", async () => {
    const reply = await client.concat(fromArray([{ text: "a" }, { text: "b" }, { text: "c" }]));
    expect(reply.message).toBe("a b c");
  });

  it("bidi takes an async source and yields an async iterable", async () => {
    const got: string[] = [];
    for await (const m of client.chat(fromArray([{ text: "one" }, { text: "two" }]))) {
      got.push(m.text);
    }
    expect(got).toEqual(["echo: one", "echo: two"]);
  });

  it("unary deadline fires DEADLINE_EXCEEDED (Fetch path)", async () => {
    await expect(
      client.sleep({ millis: 5000 }, { deadline: Date.now() + 150 }),
    ).rejects.toMatchObject({ code: Status.DEADLINE_EXCEEDED });
  });

  it("streaming deadline fires DEADLINE_EXCEEDED (WebSocket path)", async () => {
    // Chat echoes the first message then waits; the source never ends.
    const source = (async function* () {
      yield { text: "hi" };
      await new Promise<never>(() => {});
    })();
    const responses = client.chat(source, { deadline: Date.now() + 150 });
    await expect(
      (async () => {
        for await (const _ of responses) {
          /* drain */
        }
      })(),
    ).rejects.toMatchObject({ code: Status.DEADLINE_EXCEEDED });
  });

  it("cancellation via AbortSignal ends the stream with CANCELLED", async () => {
    const ac = new AbortController();
    // A source that sends one message then blocks forever.
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
