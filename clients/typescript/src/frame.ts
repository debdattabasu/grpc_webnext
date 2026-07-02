import { Frame, Trailer } from "./generated/grpc_webnext.js";

/** Encode a `Frame` into the bytes of one WebSocket binary message. */
export function encodeFrame(frame: Frame): Uint8Array {
  return Frame.encode(frame).finish();
}

/** Decode one WebSocket binary message into a `Frame`. */
export function decodeFrame(bytes: Uint8Array): Frame {
  return Frame.decode(bytes);
}

/**
 * Parse a buffered Fetch unary response body:
 *
 * ```
 * [ u32 len | message bytes ]
 * [ u32 len | Trailer bytes ]
 * ```
 *
 * `limit` bounds the total body size the caller will buffer.
 */
export function decodeFetchResponseBody(
  body: Uint8Array,
  limit: number,
): { message: Uint8Array; trailer: Trailer } {
  if (body.byteLength > limit) {
    throw new RangeError(`response body exceeds size limit (${limit} bytes)`);
  }
  const view = new DataView(body.buffer, body.byteOffset, body.byteLength);
  let offset = 0;

  const message = takeBlock(body, view, offset);
  offset = message.next;
  const trailerBlock = takeBlock(body, view, offset);

  return { message: message.bytes, trailer: Trailer.decode(trailerBlock.bytes) };
}

function takeBlock(
  body: Uint8Array,
  view: DataView,
  offset: number,
): { bytes: Uint8Array; next: number } {
  if (offset + 4 > body.byteLength) {
    throw new RangeError("truncated response: missing length prefix");
  }
  const len = view.getUint32(offset, false); // big-endian
  const start = offset + 4;
  const end = start + len;
  if (end > body.byteLength) {
    throw new RangeError(`truncated response: expected ${len} bytes`);
  }
  return { bytes: body.subarray(start, end), next: end };
}
