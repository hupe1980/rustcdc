// A signal is accepted before it runs: `POST /signals` only says it was queued. Its outcome arrives
// on the audit trail, under the same signal id. These read it back.
import type { AuditEntry } from "./api";

type Detail = { signal_id?: string; state?: string; message?: string; additional_data?: { error?: string } | null };

const TERMINAL = new Set(["COMPLETED", "ABORTED", "PAUSED", "RESUMED", "SKIPPED"]);

function parse(detail: string): Detail | null {
  try {
    const value = JSON.parse(detail) as unknown;
    return value && typeof value === "object" ? (value as Detail) : null;
  } catch {
    return null;
  }
}

export type Outcome = { done: boolean; ok: boolean; state: string; error?: string };

/** Where a signal stands, from the audit entries; `null` until the trail mentions it. */
export function outcome(entries: AuditEntry[], signalId: string): Outcome | null {
  const mine = entries
    .map((e) => ({ entry: e, detail: parse(e.detail) }))
    .filter(({ detail }) => detail?.signal_id === signalId)
    .sort((a, b) => b.entry.sequence - a.entry.sequence);
  const latest = mine[0];
  if (!latest?.detail) return null;
  const state = latest.detail.state ?? "";
  const error = mine.map(({ detail }) => detail?.additional_data?.error).find(Boolean);
  if (error) return { done: true, ok: false, state, error };
  return { done: TERMINAL.has(state), ok: state !== "ABORTED" && state !== "SKIPPED", state };
}

/** An audit detail as one line: the error, else the message, else the state, else the text. */
export function describe(detail: string): string {
  const d = parse(detail);
  if (!d) return detail;
  return d.additional_data?.error ?? (d.message || undefined) ?? d.state?.toLowerCase().replaceAll("_", " ") ?? detail;
}

/**
 * A signal id the console chooses itself. The server treats a repeated id as the same signal, so
 * a double click, or the retry after the write token is entered, cannot run an action twice.
 */
export function newSignalId(): string {
  return `console-${crypto.randomUUID()}`;
}

/** Poll the audit trail until the signal ends, reporting each step; `null` if it outlasts `tries`. */
export async function follow(
  signalId: string,
  entries: () => Promise<AuditEntry[]>,
  onstep: (step: Outcome) => void,
  tries = 60,
  everyMs = 1000,
): Promise<Outcome | null> {
  for (let i = 0; i < tries; i++) {
    await new Promise((r) => setTimeout(r, everyMs));
    const result = outcome(await entries(), signalId);
    if (result?.done) return result;
    if (result) onstep(result);
  }
  return null;
}
