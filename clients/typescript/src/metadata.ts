import type { Metadatum } from "./generated/grpc_webnext.js";

export type MetadataValue = string | Uint8Array;

/**
 * gRPC call metadata, modeled after @grpc/grpc-js `Metadata`.
 *
 * Keys are case-insensitive. Binary values are allowed only for keys ending in
 * `-bin`; ASCII values for all others.
 */
export class Metadata {
  private readonly map = new Map<string, MetadataValue[]>();

  set(key: string, value: MetadataValue): void {
    this.map.set(normalizeKey(key), [value]);
  }

  add(key: string, value: MetadataValue): void {
    const k = normalizeKey(key);
    const existing = this.map.get(k);
    if (existing) existing.push(value);
    else this.map.set(k, [value]);
  }

  remove(key: string): void {
    this.map.delete(normalizeKey(key));
  }

  get(key: string): MetadataValue[] {
    return this.map.get(normalizeKey(key)) ?? [];
  }

  getMap(): Record<string, MetadataValue> {
    const out: Record<string, MetadataValue> = {};
    for (const [k, v] of this.map) if (v.length) out[k] = v[0];
    return out;
  }

  /** Serialize to protobuf `Metadatum[]` for a WebSocket frame. */
  toMetadatumList(): Metadatum[] {
    const out: Metadatum[] = [];
    for (const [key, values] of this.map) {
      for (const value of values) {
        if (typeof value === "string") {
          out.push({ key, asciiValue: value, binValue: undefined });
        } else {
          out.push({ key, asciiValue: undefined, binValue: value });
        }
      }
    }
    return out;
  }

  /** Serialize to HTTP headers for a Fetch request. */
  toHeaders(): Headers {
    const headers = new Headers();
    for (const [key, values] of this.map) {
      for (const value of values) {
        headers.append(key, typeof value === "string" ? value : base64(value));
      }
    }
    return headers;
  }

  static fromMetadatumList(items: Metadatum[]): Metadata {
    const md = new Metadata();
    for (const m of items) {
      if (m.binValue !== undefined) md.add(m.key, m.binValue);
      else if (m.asciiValue !== undefined) md.add(m.key, m.asciiValue);
    }
    return md;
  }

  static fromHeaders(headers: Headers): Metadata {
    const md = new Metadata();
    headers.forEach((value, key) => {
      if (key.endsWith("-bin")) md.add(key, unbase64(value));
      else md.add(key, value);
    });
    return md;
  }
}

function normalizeKey(key: string): string {
  return key.toLowerCase();
}

function base64(bytes: Uint8Array): string {
  let s = "";
  for (const b of bytes) s += String.fromCharCode(b);
  return typeof btoa !== "undefined" ? btoa(s) : Buffer.from(bytes).toString("base64");
}

function unbase64(value: string): Uint8Array {
  if (typeof atob !== "undefined") {
    const s = atob(value);
    const out = new Uint8Array(s.length);
    for (let i = 0; i < s.length; i++) out[i] = s.charCodeAt(i);
    return out;
  }
  return new Uint8Array(Buffer.from(value, "base64"));
}
