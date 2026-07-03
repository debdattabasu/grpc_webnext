import { Metadata } from "./metadata.js";
import { Status } from "./status.js";
import type { StatusResult } from "./transport.js";

/**
 * The call "context" — the cancellation + deadline half of Go's context.Context.
 *
 * A call's deadline and user `AbortSignal` are merged into one internal signal.
 * When the deadline timer fires, the signal aborts with `DEADLINE_REASON` so the
 * termination is reported as DEADLINE_EXCEEDED; any other abort is CANCELLED.
 */
export const DEADLINE_REASON = Symbol("grpc-webnext.deadline");

export interface CallContext {
  /** Effective signal to hand to the transport. */
  signal: AbortSignal;
  /** Relative deadline in ms (sent as grpc-timeout / timeout_millis), if any. */
  timeoutMillis?: number;
  /** Abort the call (explicit cancel -> CANCELLED). */
  abort(): void;
  /** Clear the timer / listeners; call once the call settles. */
  dispose(): void;
}

export function createCallContext(deadline?: Date | number, userSignal?: AbortSignal): CallContext {
  const controller = new AbortController();
  const timeoutMillis = resolveTimeout(deadline);
  let timer: ReturnType<typeof setTimeout> | undefined;
  let onUserAbort: (() => void) | undefined;

  if (userSignal) {
    if (userSignal.aborted) {
      controller.abort(userSignal.reason);
    } else {
      onUserAbort = () => controller.abort(userSignal.reason);
      userSignal.addEventListener("abort", onUserAbort, { once: true });
    }
  }
  if (timeoutMillis !== undefined && !controller.signal.aborted) {
    timer = setTimeout(() => controller.abort(DEADLINE_REASON), timeoutMillis);
  }

  return {
    signal: controller.signal,
    timeoutMillis,
    abort: () => controller.abort(),
    dispose: () => {
      if (timer) clearTimeout(timer);
      if (onUserAbort && userSignal) userSignal.removeEventListener("abort", onUserAbort);
    },
  };
}

/** The gRPC status for an aborted call, distinguishing deadline from cancel. */
export function statusForAbort(signal: AbortSignal): StatusResult {
  const deadline = signal.reason === DEADLINE_REASON;
  return {
    code: deadline ? Status.DEADLINE_EXCEEDED : Status.CANCELLED,
    details: deadline ? "deadline exceeded" : "cancelled",
    metadata: new Metadata(),
  };
}

function resolveTimeout(deadline?: Date | number): number | undefined {
  if (deadline === undefined) return undefined;
  const ms = (deadline instanceof Date ? deadline.getTime() : deadline) - Date.now();
  return ms > 0 ? ms : 1;
}
