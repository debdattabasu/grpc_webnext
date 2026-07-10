//! The grpc-webnext conformance runner (TS client driver).
//!
//! Loads the language-neutral cases in `conformance/cases/*.yaml` and drives each one
//! against the Rust `conformance-server` (which serves `ConformanceService` over
//! grpc-webnext) under every applicable transport profile — the h2ts binary path, the
//! custom `Frame` binary path, and the JSON custom path — asserting the observed wire
//! behavior. This is the cross-implementation anti-drift guard, exercised over the real wire.

import { spawn, type ChildProcess } from "node:child_process";
import { createInterface } from "node:readline";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { load } from "js-yaml";
import WebSocket from "ws";
import { makeClient, Metadata, Status, type ClientOptions } from "../src/index.js";
import {
  ConformanceServiceDefinition,
  type ConformancePayload,
  type Metadatum,
  type ResponseDefinition,
} from "./gen/conformance.js";
import type { StatusResult } from "../src/transport.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = path.resolve(__dirname, "../../../..");
const rustRoot = path.join(repoRoot, "rust");
const casesDir = path.join(repoRoot, "conformance", "cases");
const wsImpl = WebSocket as unknown as typeof globalThis.WebSocket;

// --- case types (the YAML shape; see conformance/schema/case.schema.json) ---------------
type Bytes = { text?: string; b64?: string };
type Meta = { key: string; ascii?: string; b64?: string };
type RD = {
  status_code?: number;
  status_message?: string;
  headers?: Meta[];
  trailers?: Meta[];
  payload?: Bytes;
  stream_messages?: { payload?: Bytes; delay_ms?: number }[];
  delay_ms?: number;
  oversize_response_bytes?: number;
};
type Msg = { payload?: Bytes; response_definition?: RD };
type Matcher = {
  payload?: Bytes;
  request_info?: { request_headers_contain?: Meta[]; timeout_present?: boolean; json?: boolean };
};
type Case = {
  name: string;
  rpc: "Unary" | "ServerStream" | "ClientStream" | "BidiStream";
  codecs?: ("proto" | "json")[];
  timeout_millis?: number;
  request_metadata?: Meta[];
  requires?: { max_message_bytes?: number; transcoder?: boolean };
  request?: Msg;
  requests?: Msg[];
  cancel_after_messages?: number;
  expect: {
    status: { code: number; message_contains?: string };
    response?: Matcher;
    messages?: Matcher[];
    message_count?: number;
    headers_contain?: Meta[];
    trailers_contain?: Meta[];
  };
};
type Suite = { suite: string; cases: Case[] };

// --- converters: case value -> proto / Metadata -----------------------------------------
const toBytes = (b?: Bytes): Uint8Array =>
  !b
    ? new Uint8Array()
    : b.text !== undefined
      ? new TextEncoder().encode(b.text)
      : new Uint8Array(Buffer.from(b.b64 ?? "", "base64"));

const metaValue = (m: Meta): string | Uint8Array =>
  m.b64 !== undefined ? new Uint8Array(Buffer.from(m.b64, "base64")) : (m.ascii ?? "");

function toMetadata(items?: Meta[]): Metadata {
  const md = new Metadata();
  for (const m of items ?? []) md.set(m.key, metaValue(m));
  return md;
}

function toMetadatumList(items?: Meta[]): Metadatum[] {
  return (items ?? []).map((m) =>
    m.b64 !== undefined
      ? { key: m.key, asciiValue: undefined, binValue: new Uint8Array(Buffer.from(m.b64, "base64")) }
      : { key: m.key, asciiValue: m.ascii ?? "", binValue: undefined },
  );
}

function toRD(rd?: RD): ResponseDefinition {
  return {
    statusCode: rd?.status_code ?? 0,
    statusMessage: rd?.status_message ?? "",
    headers: toMetadatumList(rd?.headers),
    trailers: toMetadatumList(rd?.trailers),
    payload: toBytes(rd?.payload),
    streamMessages: (rd?.stream_messages ?? []).map((s) => ({
      payload: toBytes(s.payload),
      delayMs: s.delay_ms ?? 0,
    })),
    delayMs: rd?.delay_ms ?? 0,
    oversizeResponseBytes: rd?.oversize_response_bytes ?? 0,
  };
}

// --- transport profiles + server config profiles ----------------------------------------
type Profile = { name: string; config: Partial<ClientOptions> };
function profilesFor(c: Case): Profile[] {
  const codecs = c.codecs ?? ["proto", "json"];
  const out: Profile[] = [];
  if (codecs.includes("proto")) {
    out.push({ name: "proto/h2ts", config: {} });
    out.push({ name: "proto/ws", config: { unary: "fetch", streaming: "ws" } });
  }
  if (codecs.includes("json")) out.push({ name: "json", config: { codec: "json" } });
  return out;
}

const requiresKey = (r?: Case["requires"]): string => {
  const p: string[] = [];
  if (r?.max_message_bytes) p.push(`max:${r.max_message_bytes}`);
  if (r?.transcoder === false) p.push("notranscoder");
  return p.length ? p.join(",") : "default";
};
function requiresEnv(r?: Case["requires"]): Record<string, string> {
  const env: Record<string, string> = {};
  if (r?.max_message_bytes) env.CONFORMANCE_MAX_MESSAGE_BYTES = String(r.max_message_bytes);
  if (r?.transcoder === false) env.CONFORMANCE_TRANSCODER = "0";
  return env;
}

// --- driving a single call ---------------------------------------------------------------
interface Result {
  headers: Metadata;
  messages: ConformancePayload[];
  response?: ConformancePayload;
  status: StatusResult;
}
type Client = ReturnType<typeof makeClient<typeof ConformanceServiceDefinition>>;

function callOptions(c: Case) {
  return c.timeout_millis ? { deadline: Date.now() + c.timeout_millis } : {};
}

function runUnary(client: Client, c: Case): Promise<Result> {
  const req = { payload: toBytes(c.request?.payload), responseDefinition: toRD(c.request?.response_definition) };
  return new Promise((resolve) => {
    let headers = new Metadata();
    let status: StatusResult | undefined;
    const call = client.unary(req, toMetadata(c.request_metadata), callOptions(c), (err, value) => {
      // The error path (deadline/cancel/backend error) surfaces via the callback, not a
      // `status` event — derive the status from the ServiceError when it didn't fire.
      if (!status) {
        status = err
          ? { code: err.code, details: err.details ?? "", metadata: err.metadata ?? new Metadata() }
          : { code: Status.OK, details: "", metadata: new Metadata() };
      }
      resolve({ headers, messages: [], response: value as ConformancePayload | undefined, status });
    });
    call.on("metadata", (md) => (headers = md));
    call.on("status", (st) => (status = st));
  });
}

function runServerStream(client: Client, c: Case): Promise<Result> {
  const req = { responseDefinition: toRD(c.request?.response_definition) };
  return new Promise((resolve) => {
    let headers = new Metadata();
    const messages: ConformancePayload[] = [];
    let status: StatusResult;
    const stream = client.serverStream(req, toMetadata(c.request_metadata), callOptions(c));
    stream.on("metadata", (md: Metadata) => (headers = md));
    stream.on("data", (m: ConformancePayload) => messages.push(m));
    stream.on("status", (st: StatusResult) => (status = st));
    const done = () => resolve({ headers, messages, status });
    stream.on("end", done);
    stream.on("error", done);
  });
}

function runClientStream(client: Client, c: Case): Promise<Result> {
  const reqs = (c.requests ?? []).map((r, i) => ({
    payload: toBytes(r.payload),
    responseDefinition: toRD(i === 0 ? r.response_definition : undefined),
  }));
  return new Promise((resolve) => {
    const stream = client.clientStream(toMetadata(c.request_metadata), callOptions(c), (err, value) => {
      const status: StatusResult = err
        ? { code: err.code, details: err.details ?? "", metadata: new Metadata() }
        : { code: Status.OK, details: "", metadata: new Metadata() };
      const cs = value as { payload?: ConformancePayload } | undefined;
      resolve({ headers: new Metadata(), messages: [], response: cs?.payload, status });
    });
    for (const r of reqs) stream.write(r);
    stream.end();
  });
}

function runBidiStream(client: Client, c: Case): Promise<Result> {
  const reqs = (c.requests ?? []).map((r, i) => ({
    payload: toBytes(r.payload),
    responseDefinition: toRD(i === 0 ? r.response_definition : undefined),
  }));
  return new Promise((resolve) => {
    let headers = new Metadata();
    const messages: ConformancePayload[] = [];
    let status: StatusResult;
    const stream = client.bidiStream(toMetadata(c.request_metadata), callOptions(c));
    stream.on("metadata", (md: Metadata) => (headers = md));
    stream.on("data", (m: ConformancePayload) => {
      messages.push(m);
      if (c.cancel_after_messages && messages.length >= c.cancel_after_messages) stream.cancel();
    });
    stream.on("status", (st: StatusResult) => (status = st));
    const done = () => resolve({ headers, messages, status });
    stream.on("end", done);
    stream.on("error", done);
    for (const r of reqs) stream.write(r);
    // A cancel case keeps the stream open (so the server doesn't complete first) and
    // cancels from the data handler once it has seen `cancel_after_messages` messages.
    if (!c.cancel_after_messages) stream.end();
  });
}

function runCase(client: Client, c: Case): Promise<Result> {
  switch (c.rpc) {
    case "Unary":
      return runUnary(client, c);
    case "ServerStream":
      return runServerStream(client, c);
    case "ClientStream":
      return runClientStream(client, c);
    case "BidiStream":
      return runBidiStream(client, c);
  }
}

// --- assertions --------------------------------------------------------------------------
const bytesEq = (a: Uint8Array | undefined, b: Uint8Array) =>
  expect(Array.from(a ?? new Uint8Array())).toEqual(Array.from(b));

function assertMetaContains(md: Metadata, items: Meta[]) {
  for (const m of items) {
    const values = md.get(m.key);
    if (m.b64 !== undefined) {
      const want = Array.from(new Uint8Array(Buffer.from(m.b64, "base64")));
      const got = values.map((v) => (typeof v === "string" ? v : Array.from(v)));
      expect(got, `metadata ${m.key} (bin)`).toContainEqual(want);
    } else {
      expect(values, `metadata ${m.key}`).toContain(m.ascii ?? "");
    }
  }
}

function assertPayload(matcher: Matcher, cp: ConformancePayload | undefined) {
  if (matcher.payload) bytesEq(cp?.payload, toBytes(matcher.payload));
  const ri = matcher.request_info;
  if (ri?.request_headers_contain) {
    const echoed = cp?.requestInfo?.requestHeaders ?? [];
    for (const want of ri.request_headers_contain) {
      const hit = echoed.some(
        (m) =>
          m.key === want.key &&
          (want.b64 !== undefined
            ? m.binValue !== undefined &&
              Buffer.from(m.binValue).toString("base64") === want.b64
            : m.asciiValue === (want.ascii ?? "")),
      );
      expect(hit, `server echoed request header ${want.key}`).toBe(true);
    }
  }
}

function assertCase(c: Case, r: Result) {
  expect(r.status.code, `status (details: "${r.status.details}")`).toBe(c.expect.status.code);
  if (c.expect.status.message_contains) expect(r.status.details).toContain(c.expect.status.message_contains);
  if (c.expect.response) assertPayload(c.expect.response, r.response);
  if (c.expect.messages) {
    expect(r.messages.length).toBeGreaterThanOrEqual(c.expect.messages.length);
    c.expect.messages.forEach((pm, i) => assertPayload(pm, r.messages[i]));
  }
  if (c.expect.message_count !== undefined) expect(r.messages.length).toBe(c.expect.message_count);
  if (c.expect.headers_contain) assertMetaContains(r.headers, c.expect.headers_contain);
  if (c.expect.trailers_contain) assertMetaContains(r.status.metadata, c.expect.trailers_contain);
}

// --- harness -----------------------------------------------------------------------------
const suites: Suite[] = fs
  .readdirSync(casesDir)
  .filter((f) => f.endsWith(".yaml"))
  .sort()
  .map((f) => load(fs.readFileSync(path.join(casesDir, f), "utf8")) as Suite);

const serverProfiles = new Map<string, Record<string, string>>();
for (const s of suites) for (const c of s.cases) serverProfiles.set(requiresKey(c.requires), requiresEnv(c.requires));

const servers = new Map<string, { proc: ChildProcess; baseUrl: string }>();

function spawnServer(env: Record<string, string>): Promise<{ proc: ChildProcess; baseUrl: string }> {
  const proc = spawn("cargo", ["run", "--quiet", "-p", "conformance-server"], {
    cwd: rustRoot,
    stdio: ["ignore", "pipe", "inherit"],
    env: { ...process.env, ...env },
  });
  return new Promise((resolve, reject) => {
    const rl = createInterface({ input: proc.stdout! });
    rl.on("line", (line) => {
      const m = line.match(/^LISTENING (\S+)$/);
      if (m) resolve({ proc, baseUrl: m[1] });
    });
    proc.on("exit", (code) => reject(new Error(`conformance-server exited early: ${code}`)));
  });
}

beforeAll(async () => {
  await Promise.all(
    [...serverProfiles].map(async ([key, env]) => servers.set(key, await spawnServer(env))),
  );
}, 120_000);

afterAll(() => {
  for (const { proc } of servers.values()) proc.kill("SIGKILL");
});

for (const suite of suites) {
  describe(`conformance: ${suite.suite}`, () => {
    for (const c of suite.cases) {
      for (const profile of profilesFor(c)) {
        it(`${c.name} [${profile.name}]`, async () => {
          const server = servers.get(requiresKey(c.requires))!;
          const client = makeClient(ConformanceServiceDefinition, {
            baseUrl: server.baseUrl,
            webSocketImpl: wsImpl,
            ...profile.config,
          });
          try {
            assertCase(c, await runCase(client, c));
          } finally {
            client.close();
          }
        });
      }
    }
  });
}
