import { Emitter } from "./emitter.js";
import type { Metadata } from "./metadata.js";
import { ServiceError, Status } from "./status.js";
import type { StatusResult, StreamCall } from "./transport.js";

/** grpc-js-style callback for unary / client-streaming responses. */
export type RequestCallback<Res> = (
  error: ServiceError | null,
  value?: Res,
) => void;

interface CallEvents {
  metadata: (metadata: Metadata) => void;
  status: (status: StatusResult) => void;
}

/** Handle for a unary call; mirrors grpc-js `ClientUnaryCall`. */
export class ClientUnaryCall extends Emitter<CallEvents> {
  constructor(private readonly canceller: () => void) {
    super();
  }
  cancel(): void {
    this.canceller();
  }
}

interface ReadableEvents<Res> extends CallEvents {
  data: (message: Res) => void;
  end: () => void;
  error: (error: ServiceError) => void;
}

/** Server -> client message stream; mirrors grpc-js `ClientReadableStream`. */
export class ClientReadableStream<Res> extends Emitter<ReadableEvents<Res>> {
  private readonly buffer: Res[] = [];
  private ended = false;
  private errored: ServiceError | null = null;
  private waiter: ((r: IteratorResult<Res>) => void) | null = null;

  constructor(private readonly canceller: () => void) {
    super();
    this.on("data", (m) => this.push(m));
    this.on("end", () => this.finish());
    this.on("error", (e) => this.fail(e));
  }

  cancel(): void {
    this.canceller();
  }

  private push(m: Res): void {
    if (this.waiter) {
      const w = this.waiter;
      this.waiter = null;
      w({ value: m, done: false });
    } else {
      this.buffer.push(m);
    }
  }
  private finish(): void {
    this.ended = true;
    if (this.waiter) {
      this.waiter({ value: undefined, done: true });
      this.waiter = null;
    }
  }
  private fail(e: ServiceError): void {
    this.errored = e;
    this.finish();
  }

  [Symbol.asyncIterator](): AsyncIterator<Res> {
    return {
      next: (): Promise<IteratorResult<Res>> => {
        if (this.errored) return Promise.reject(this.errored);
        if (this.buffer.length) {
          return Promise.resolve({ value: this.buffer.shift() as Res, done: false });
        }
        if (this.ended) return Promise.resolve({ value: undefined, done: true });
        return new Promise((resolve) => (this.waiter = resolve));
      },
    };
  }
}

/** Client -> server message stream; mirrors grpc-js `ClientWritableStream`. */
export class ClientWritableStream<Req> extends Emitter<CallEvents> {
  constructor(
    private readonly call: StreamCall,
    private readonly serialize: (req: Req) => Uint8Array,
  ) {
    super();
  }
  write(message: Req): void {
    this.call.send(this.serialize(message));
  }
  end(): void {
    this.call.halfClose();
  }
  cancel(): void {
    this.call.cancel();
  }
}

/** Bidi stream; mirrors grpc-js `ClientDuplexStream`. */
export class ClientDuplexStream<Req, Res> extends ClientReadableStream<Res> {
  constructor(
    private readonly duplex: StreamCall,
    private readonly serializeReq: (req: Req) => Uint8Array,
  ) {
    super(() => duplex.cancel());
  }
  write(message: Req): void {
    this.duplex.send(this.serializeReq(message));
  }
  end(): void {
    this.duplex.halfClose();
  }
}

/** Convert a non-OK status into a ServiceError, or null if OK. */
export function statusToError(status: StatusResult): ServiceError | null {
  if (status.code === Status.OK) return null;
  return new ServiceError(status.code, status.details, status.metadata);
}
