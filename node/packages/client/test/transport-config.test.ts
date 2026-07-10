//! The per-client transport selection: proto defaults to h2ts, JSON is locked to the
//! plaintext custom paths, and the unsupported combo is rejected.
import { describe, expect, it } from "vitest";
import { resolveTransportSelection } from "../src/index.js";

describe("resolveTransportSelection", () => {
  it("proto defaults to h2ts for both surfaces", () => {
    expect(resolveTransportSelection({})).toEqual({ unary: "h2ts", streaming: "h2ts" });
    expect(resolveTransportSelection({ codec: "proto" })).toEqual({
      unary: "h2ts",
      streaming: "h2ts",
    });
  });

  it("json is locked to fetch + ws", () => {
    expect(resolveTransportSelection({ codec: "json" })).toEqual({
      unary: "fetch",
      streaming: "ws",
    });
  });

  it("json + an h2ts knob throws", () => {
    expect(() => resolveTransportSelection({ codec: "json", unary: "h2ts" })).toThrow(/json/i);
    expect(() => resolveTransportSelection({ codec: "json", streaming: "h2ts" })).toThrow(/json/i);
  });

  it("opt-out: fetch unary + h2ts streaming", () => {
    expect(resolveTransportSelection({ unary: "fetch", streaming: "h2ts" })).toEqual({
      unary: "fetch",
      streaming: "h2ts",
    });
  });

  it("opt-out: fetch unary + ws streaming (pure custom protocol)", () => {
    expect(resolveTransportSelection({ unary: "fetch", streaming: "ws" })).toEqual({
      unary: "fetch",
      streaming: "ws",
    });
  });

  it("streaming: 'ws' alone defaults unary to fetch (its only valid pairing)", () => {
    expect(resolveTransportSelection({ streaming: "ws" })).toEqual({
      unary: "fetch",
      streaming: "ws",
    });
  });

  it("unary: 'h2ts' alone defaults streaming to h2ts", () => {
    expect(resolveTransportSelection({ unary: "h2ts" })).toEqual({
      unary: "h2ts",
      streaming: "h2ts",
    });
  });

  it("explicit { unary: 'h2ts', streaming: 'ws' } is rejected", () => {
    expect(() => resolveTransportSelection({ unary: "h2ts", streaming: "ws" })).toThrow(
      /unsupported/i,
    );
  });
});
