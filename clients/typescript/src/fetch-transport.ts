import { decodeFetchResponseBody } from "./frame.js";
import { Metadata } from "./metadata.js";
import { Status } from "./status.js";
import type {
  StreamCall,
  StreamHandlers,
  Transport,
  TransportCallOptions,
  UnaryResponse,
} from "./transport.js";

export const CT_PROTO = "application/grpc-webnext+proto";
export const CT_JSON = "application/grpc-webnext+json";

export interface FetchTransportOptions {
  /** Base URL of the grpc-webnext endpoint, e.g. "https://host:443". */
  baseUrl: string;
  /** Max response body bytes to buffer. Default 4 MiB. */
  maxMessageBytes?: number;
  /** Injectable fetch (defaults to global fetch). */
  fetch?: typeof fetch;
  /** Message codec: "proto" (default) or "json". */
  codec?: "proto" | "json";
}

/**
 * Unary transport over HTTP Fetch. Streaming is not supported here — use a
 * WebSocket transport (grpc-webnext sends all streaming RPCs over WebSocket).
 */
export class FetchTransport implements Transport {
  private readonly baseUrl: string;
  private readonly maxMessageBytes: number;
  private readonly fetchImpl: typeof fetch;
  private readonly contentType: string;

  constructor(options: FetchTransportOptions) {
    this.baseUrl = options.baseUrl.replace(/\/$/, "");
    this.maxMessageBytes = options.maxMessageBytes ?? 4 * 1024 * 1024;
    this.fetchImpl = options.fetch ?? globalThis.fetch;
    this.contentType = options.codec === "json" ? CT_JSON : CT_PROTO;
  }

  async unary(
    path: string,
    request: Uint8Array,
    options: TransportCallOptions,
  ): Promise<UnaryResponse> {
    const headers = options.metadata.toHeaders();
    headers.set("content-type", this.contentType);
    if (options.timeoutMillis && options.timeoutMillis > 0) {
      headers.set("grpc-timeout", `${Math.ceil(options.timeoutMillis)}m`);
    }

    const response = await this.fetchImpl(`${this.baseUrl}${path}`, {
      method: "POST",
      headers,
      body: request as BodyInit,
      signal: options.signal,
    });

    if (!response.ok) {
      // Transport-level (non-gRPC) failure.
      return {
        message: new Uint8Array(),
        headers: new Metadata(),
        status: {
          code: Status.UNAVAILABLE,
          details: `HTTP ${response.status}: ${await safeText(response)}`,
          metadata: new Metadata(),
        },
      };
    }

    const bodyBytes = new Uint8Array(await response.arrayBuffer());
    if (bodyBytes.byteLength > this.maxMessageBytes) {
      throw new RangeError(`response body exceeds size limit (${this.maxMessageBytes} bytes)`);
    }

    if (this.contentType === CT_JSON) {
      // Native JSON: bare message body; status + metadata in HTTP headers.
      const code = Number(response.headers.get("grpc-status") ?? "0") as Status;
      const details = decodeURIComponent(response.headers.get("grpc-message") ?? "");
      return {
        message: bodyBytes,
        headers: Metadata.fromHeaders(response.headers),
        status: { code, details, metadata: Metadata.fromHeaders(response.headers) },
      };
    }

    // Binary proto: `[len|message][len|trailer]` framed body.
    const { message, trailer } = decodeFetchResponseBody(bodyBytes, this.maxMessageBytes);
    return {
      message,
      headers: Metadata.fromHeaders(response.headers),
      status: {
        code: trailer.statusCode as Status,
        details: trailer.statusMessage,
        metadata: Metadata.fromMetadatumList(trailer.trailers),
      },
    };
  }

  startStream(_path: string, _options: TransportCallOptions, _handlers: StreamHandlers): StreamCall {
    throw new Error(
      "FetchTransport does not support streaming; use WebSocketTransport for streaming RPCs",
    );
  }

  close(): void {}
}

async function safeText(response: Response): Promise<string> {
  try {
    return await response.text();
  } catch {
    return "";
  }
}
