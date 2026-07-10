import { describe, expect, it } from "vitest";
import { Trailer } from "./generated/grpc_webnext.js";
import {
  decodeFetchResponseBody,
  decodeFrame,
  encodeFetchRequestBody,
  encodeFrame,
} from "./frame.js";
import { Metadata } from "./metadata.js";

describe("frame codec", () => {
  it("round-trips a Subscribe frame", () => {
    const bytes = encodeFrame({
      subscribe: {
        method: "/echo.v1.Echo/Stream",
        headers: new Metadata().toMetadatumList(),
        timeoutMillis: 100,
        initialPayload: new Uint8Array([1, 2, 3]),
      },
    });
    const frame = decodeFrame(bytes);
    expect(frame.subscribe?.method).toBe("/echo.v1.Echo/Stream");
    expect(frame.subscribe?.initialPayload).toEqual(new Uint8Array([1, 2, 3]));
  });

  it("parses a Fetch response body (message + trailer)", () => {
    const message = new Uint8Array([9, 8, 7]);
    const trailer: Trailer = {
      statusCode: 0,
      statusMessage: "OK",
      trailers: [],
    };
    const trailerBytes = Trailer.encode(trailer).finish();

    const body = new Uint8Array(4 + message.length + 4 + trailerBytes.length);
    const view = new DataView(body.buffer);
    let o = 0;
    view.setUint32(o, message.length, false);
    o += 4;
    body.set(message, o);
    o += message.length;
    view.setUint32(o, trailerBytes.length, false);
    o += 4;
    body.set(trailerBytes, o);

    const decoded = decodeFetchResponseBody(body, 1024);
    expect(decoded.message).toEqual(message);
    expect(decoded.trailer.statusCode).toBe(0);
    expect(decoded.trailer.statusMessage).toBe("OK");
  });

  it("enforces the size limit", () => {
    expect(() => decodeFetchResponseBody(new Uint8Array(100), 10)).toThrow(/size limit/);
  });

  it("frames a request body as [u32 len | message]", () => {
    const message = new Uint8Array([5, 6, 7, 8, 9]);
    const framed = encodeFetchRequestBody(message);
    expect(framed.length).toBe(4 + message.length);
    const len = new DataView(framed.buffer).getUint32(0, false); // big-endian
    expect(len).toBe(message.length);
    expect(framed.subarray(4)).toEqual(message);
  });

  it("frames an empty request body as a bare length prefix", () => {
    const framed = encodeFetchRequestBody(new Uint8Array(0));
    expect(framed).toEqual(new Uint8Array([0, 0, 0, 0]));
  });
});
