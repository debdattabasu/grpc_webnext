/** gRPC status codes, matching @grpc/grpc-js `status`. */
export enum Status {
  OK = 0,
  CANCELLED = 1,
  UNKNOWN = 2,
  INVALID_ARGUMENT = 3,
  DEADLINE_EXCEEDED = 4,
  NOT_FOUND = 5,
  ALREADY_EXISTS = 6,
  PERMISSION_DENIED = 7,
  RESOURCE_EXHAUSTED = 8,
  FAILED_PRECONDITION = 9,
  ABORTED = 10,
  OUT_OF_RANGE = 11,
  UNIMPLEMENTED = 12,
  INTERNAL = 13,
  UNAVAILABLE = 14,
  DATA_LOSS = 15,
  UNAUTHENTICATED = 16,
}

/** A gRPC error surfaced to callers, mirroring grpc-js `ServiceError`. */
export class ServiceError extends Error {
  constructor(
    public readonly code: Status,
    public readonly details: string,
    public readonly metadata?: import("./metadata.js").Metadata,
  ) {
    super(`${Status[code] ?? code}: ${details}`);
    this.name = "ServiceError";
  }
}
