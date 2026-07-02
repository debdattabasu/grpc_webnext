import { describe, expect, it } from "vitest";
import { Trailer } from "./generated/grpc_webnext.js";
import { decodeFetchResponseBody, decodeFrame, encodeFrame } from "./frame.js";
import { Metadata } from "./metadata.js";

describe("frame codec", () => {
  it("round-trips a Subscribe frame", () => {
    const bytes = encodeFrame({
      subscribe: {
        streamId: 7,
        method: "/echo.v1.Echo/Stream",
        headers: new Metadata().toMetadatumList(),
        timeoutMillis: 100,
        initialPayload: new Uint8Array([1, 2, 3]),
      },
    });
    const frame = decodeFrame(bytes);
    expect(frame.subscribe?.streamId).toBe(7);
    expect(frame.subscribe?.method).toBe("/echo.v1.Echo/Stream");
    expect(frame.subscribe?.initialPayload).toEqual(new Uint8Array([1, 2, 3]));
  });

  it("parses a Fetch response body (message + trailer)", () => {
    const message = new Uint8Array([9, 8, 7]);
    const trailer: Trailer = {
      streamId: 0,
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
});
