/**
 * grpc-webnext end-to-end demo.
 *
 * Spawns the example Greeter server (native grpc-webnext server, serving Fetch +
 * WebSocket + native gRPC on one port), then drives every RPC cardinality with
 * the TypeScript client. Run with: `npm run demo` from node/packages/client.
 */
import { spawn, type ChildProcess } from "node:child_process";
import { createInterface } from "node:readline";
import path from "node:path";
import { fileURLToPath } from "node:url";
import WebSocket from "ws";
import { makeClient, Metadata } from "../src/index.js";
import { GreeterDefinition } from "./gen/greeter.js";

// node/packages/client/examples -> repo root is four levels up; the Cargo workspace lives in rust/.
const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../../..");
const rustRoot = path.join(repoRoot, "rust");

async function startServer(): Promise<{ baseUrl: string; proc: ChildProcess }> {
  const proc = spawn("cargo", ["run", "--quiet", "-p", "example-greeter-server"], {
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
  return { baseUrl, proc };
}

async function main() {
  console.log("Building & starting the Greeter server…");
  const { baseUrl, proc } = await startServer();
  console.log(`Server listening at ${baseUrl}\n`);

  const client = makeClient(GreeterDefinition, {
    baseUrl,
    webSocketImpl: WebSocket as unknown as typeof globalThis.WebSocket,
  });

  // 1) Unary over Fetch.
  const reply = await new Promise<{ message: string }>((resolve, reject) => {
    client.sayHello({ name: "world" }, new Metadata(), {}, (err, res) =>
      err ? reject(err) : resolve(res!),
    );
  });
  console.log(`[unary]  SayHello -> "${reply.message}"`);

  // 2) Server streaming over WebSocket.
  console.log(`[server-stream]  Countdown(3):`);
  await new Promise<void>((resolve, reject) => {
    const stream = client.countdown({ from: 3 });
    stream.on("data", (t) => console.log(`   tick ${t.value}`));
    stream.on("end", resolve);
    stream.on("error", reject);
  });

  // 3) Bidi streaming over WebSocket.
  console.log(`[bidi]  Chat:`);
  await new Promise<void>((resolve, reject) => {
    const chat = client.chat();
    chat.on("data", (m) => console.log(`   server: "${m.text}"`));
    chat.on("end", resolve);
    chat.on("error", reject);
    for (const text of ["hi", "how are you", "bye"]) {
      console.log(`   client: "${text}"`);
      chat.write({ text });
    }
    chat.end();
  });

  console.log("\nDemo complete. ✅");
  client.close();
  proc.kill("SIGKILL");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
