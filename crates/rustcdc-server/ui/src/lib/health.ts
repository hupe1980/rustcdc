// The health verdict, in words. Who owns a stall and whether a restart helps come from the server
// (`health.owner`, `health.restart_helps`, from the library's one routing table); this file only
// says them. A cause it has no sentence for still gets one, and the runbook link.
import type { Health, Status } from "./api";
import { count, duration } from "./format";

export type Tone = "healthy" | "attention" | "stalled";
export type Hop = "source" | "rustcdc" | "destination";

export type Reading = {
  tone: Tone;
  /** One sentence, the headline. */
  title: string;
  /** What is wrong, in an operator's words. */
  detail?: string;
  /** Who can fix it and what to do. */
  next?: string;
  /** The pipeline hop to light. */
  hop?: Hop;
  runbook?: string;
};

const RUNBOOK = "https://hupe1980.github.io/rustcdc/docs/runbook/";

const OWNERS: Record<string, { hop: Hop; who: string }> = {
  source: { hop: "source", who: "the source database" },
  destination: { hop: "destination", who: "the destination" },
  rustcdc: { hop: "rustcdc", who: "this process" },
};

/** Per cause: what is wrong, what to do, and the runbook section that handles it. */
export const CAUSES: Record<string, { what: string; next: string; section: string }> = {
  source_position_lost: {
    what: "The source discarded the log this pipeline had not read yet.",
    next: "Recreate the slot and re-snapshot the affected tables; deletes made in the gap cannot be recovered.",
    section: "slot-invalidated",
  },
  unconfirmed_source_position: {
    what: "Progress is saved but cannot be confirmed to the source, so it keeps its log.",
    next: "Check the replication connection. Until this clears, the source's disk fills.",
    section: "4-replication-slot-growth-postgresql",
  },
  delivery_failing: {
    what: "The destination keeps refusing a batch.",
    next: "Fix the destination; the batch is held and retried, nothing is skipped.",
    section: "6-sink-is-failing",
  },
  poll_loop_not_turning: {
    what: "The capture loop stopped turning.",
    next: "If it persists, /livez fails and the orchestrator restarts the process.",
    section: "3-checkpoint-is-not-advancing",
  },
  consumer_not_acknowledging: {
    what: "Events were delivered but not acknowledged.",
    next: "Look at the destination's write latency; nothing is lost while this holds.",
    section: "6-sink-is-failing",
  },
};

export function read(status: Status | null, eventsPerSecond: number | null, reachable: boolean): Reading {
  if (!reachable) return { tone: "attention", title: "The server is not answering", detail: "What you see below is the last known state." };
  if (!status) return { tone: "attention", title: "Connecting…" };
  const h: Health = status.health;
  const rate = eventsPerSecond !== null && eventsPerSecond >= 0.05 ? ` · ${count(eventsPerSecond)} events/s` : "";

  if (h.verdict === "stalled" || h.stall_cause) {
    const owner = OWNERS[h.owner ?? ""];
    const cause = CAUSES[h.stall_cause ?? ""];
    const since = h.stalled_for_seconds != null ? ` for ${duration(h.stalled_for_seconds)}` : "";
    const restart = h.restart_helps === false ? " Restarting will not help." : "";
    return {
      tone: "stalled",
      title: `Stalled${since}${owner ? ` — ${owner.who} needs attention` : ""}`,
      detail: cause?.what ?? `The runtime reports ${h.stall_cause ?? "a stall"}.`,
      next: `${cause?.next ?? "See the runbook for this cause."}${restart}`,
      hop: owner?.hop,
      runbook: cause ? `${RUNBOOK}#${cause.section}` : RUNBOOK,
    };
  }
  if (status.state === "starting") return { tone: "attention", title: "Starting…" };
  if (status.state !== "running" || h.verdict === "not_running")
    return { tone: "attention", title: `Not running (${status.state})`, next: "The runtime stopped. Check the process log." };
  if (h.verdict === "idle") return { tone: "healthy", title: `Capturing — the source is quiet${rate}` };
  return { tone: "healthy", title: `Capturing${rate || " — waiting for changes"}` };
}

/** Events per second from two `/status` samples. */
export function rate(before: { at: number; events: number } | null, now: { at: number; events: number }): number | null {
  if (!before || now.at <= before.at || now.events < before.events) return null;
  return (now.events - before.events) / ((now.at - before.at) / 1000);
}
