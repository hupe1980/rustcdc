// One way to talk to the admin API. The read token goes with every request, the write token only
// with actions. A 401 forgets the token that was refused and asks for it again.
import { token, forget, type Scope } from "./session";

export class ApiError extends Error {
  constructor(
    readonly status: number,
    message: string,
  ) {
    super(message);
  }
}

let onUnauthorized: (scope: Scope) => void = () => {};
export function whenUnauthorized(handler: (scope: Scope) => void) {
  onUnauthorized = handler;
}

async function call<T>(path: string, init: RequestInit = {}, scope: Scope = "read"): Promise<T> {
  const bearer = token(scope) ?? (scope === "write" ? token("read") : null);
  let response: Response;
  try {
    response = await fetch(path, {
      ...init,
      credentials: "omit",
      cache: "no-store",
      headers: {
        ...(bearer ? { Authorization: `Bearer ${bearer}` } : {}),
        ...(init.body ? { "Content-Type": "application/json" } : {}),
      },
    });
  } catch {
    throw new ApiError(0, "The server did not answer.");
  }
  if (response.status === 401 || response.status === 403) {
    forget(scope);
    onUnauthorized(scope);
    throw new ApiError(response.status, scope === "write" ? "This needs the admin write token." : "Sign in with the admin read token.");
  }
  const text = await response.text();
  const data = text ? (JSON.parse(text) as unknown) : null;
  if (!response.ok) {
    const message = (data as { error?: string; message?: string } | null)?.error ?? (data as { message?: string } | null)?.message;
    throw new ApiError(response.status, message ?? `${response.status} ${response.statusText}`);
  }
  return data as T;
}

export const get = <T>(path: string) => call<T>(path);
export const post = <T>(path: string, body: unknown) => call<T>(path, { method: "POST", body: JSON.stringify(body) }, "write");

/** The notification stream, for `sse.read`. */
export async function stream(path: string, signal: AbortSignal): Promise<ReadableStream<Uint8Array>> {
  const bearer = token("read");
  const response = await fetch(path, { signal, credentials: "omit", headers: bearer ? { Authorization: `Bearer ${bearer}` } : {} });
  if (response.status === 401) {
    forget("read");
    onUnauthorized("read");
  }
  if (!response.ok || !response.body) throw new ApiError(response.status, "The notification stream is unavailable.");
  return response.body;
}

// ── Shapes this console reads ──────────────────────────────────────────────

export type Owner = "source" | "destination" | "rustcdc";

export type Health = {
  verdict: string | null;
  stall_cause: string | null;
  stalled_for_seconds: number | null;
  owner: Owner | string | null;
  restart_helps: boolean | null;
};

export type SnapshotTable = { table: string; rows_emitted: number; chunks_emitted: number; is_complete: boolean; condition?: string };
export type SnapshotProgress = { snapshot_id: string; paused: boolean; stopped?: boolean; tables: SnapshotTable[] };

export type AuditEntry = { sequence: number; at: string; action: string; result: string; detail: string; actor_token_id?: string | null };

export type Status = {
  state: string;
  health: Health;
  started_at: string | null;
  last_batch_at: string | null;
  events_processed: number;
  batches_processed: number;
  slo: { checkpoint_age_seconds?: number | null; source_lag_seconds?: number | null; readiness_rate?: number | null };
  incremental_snapshot?: SnapshotProgress | null;
  /** A transaction the source is receiving before its commit. */
  source?: { open_transaction?: OpenTransaction | null };
  audit: { entries_total: number; recent_entries: AuditEntry[] };
};

export type OpenTransaction = { xid: number; changes: number; bytes: number; spilled: boolean; started_at_ms: number };

export type SignalDetail = { message?: string; additional_data?: { error?: string } | null };
/** One step of a signal, as `/notifications` lists it. */
export type Notification = { id: number; action_type: string; state: string; signal_id?: string; at: string; detail?: SignalDetail };
/** The same step on the stream: a CloudEvent, the time on the envelope. */
export type CloudEvent = { id: string; time: string; data: Omit<Notification, "id" | "at"> };

export type ShapeColumn = { name: string; data_type: string; nullable: boolean };
export type RowShape = { id: string; schema: string; table: string; columns?: ShapeColumn[]; primary_key?: string[] };

export type SignalAction = "execute_snapshot" | "pause_snapshot" | "resume_snapshot" | "stop_snapshot" | "log_marker";
export type SignalAccepted = { signal_id: string; action_type: SignalAction; state: string };
