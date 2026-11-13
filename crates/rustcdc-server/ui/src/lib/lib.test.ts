import { describe, expect, it } from "vitest";
import type { Status } from "./api";
import { CAUSES, rate, read } from "./health";
import { parser, type Frame } from "./sse";
import { describe as describeDetail, outcome } from "./signals";
import { curl } from "./curl";

const status = (health: Partial<Status["health"]>, state = "running"): Status => ({
  state,
  health: { verdict: "healthy", stall_cause: null, stalled_for_seconds: null, owner: null, restart_helps: null, ...health },
  started_at: null,
  last_batch_at: null,
  events_processed: 0,
  batches_processed: 0,
  slo: {},
  audit: { entries_total: 0, recent_entries: [] },
});

describe("the health verdict, in words", () => {
  const routed: [string, string, string, boolean][] = [
    ["source_position_lost", "source", "the source database", false],
    ["unconfirmed_source_position", "source", "the source database", false],
    ["delivery_failing", "destination", "the destination", false],
    ["poll_loop_not_turning", "rustcdc", "this process", true],
    ["consumer_not_acknowledging", "destination", "the destination", false],
  ];

  it.each(routed)("%s names its owner, lights its hop and says whether a restart helps", (cause, owner, who, restart) => {
    const r = read(status({ verdict: "stalled", stall_cause: cause, owner, restart_helps: restart, stalled_for_seconds: 240 }), null, true);
    expect(r.tone).toBe("stalled");
    expect(r.title).toBe(`Stalled for 4 min — ${who} needs attention`);
    expect(r.hop).toBe(owner);
    expect(r.detail).toBe(CAUSES[cause]!.what);
    expect(r.next?.includes("Restarting will not help")).toBe(!restart);
    expect(r.runbook).toContain(`#${CAUSES[cause]!.section}`);
  });

  it("still says something useful about a cause it has never heard of", () => {
    const r = read(status({ verdict: "stalled", stall_cause: "a_future_cause", owner: "someone_new" }), null, true);
    expect(r.tone).toBe("stalled");
    expect(r.title).toMatch(/^Stalled/);
    expect(r.detail).toContain("a_future_cause");
    expect(r.next).toContain("runbook");
    expect(r.runbook).toMatch(/runbook\/$/);
  });

  it("never shows the last good state as current when the server is gone", () => {
    expect(read(status({}), 12, false)).toMatchObject({ tone: "attention", title: "The server is not answering" });
  });

  it("reads healthy, idle and stopped", () => {
    expect(read(status({}), 1240, true).title).toBe("Capturing · 1,240 events/s".replace(",", (1000).toLocaleString().charAt(1)));
    expect(read(status({ verdict: "idle" }), 0, true).title).toBe("Capturing — the source is quiet");
    expect(read(status({ verdict: "not_running" }, "stopped"), null, true).tone).toBe("attention");
  });

  it("says what an otherwise idle pipeline is doing while a transaction streams in", () => {
    const s = status({ verdict: "idle" });
    s.source = {
      open_transaction: { xid: 812, changes: 2_100_000, bytes: 1_300_000_000, spilled: true, started_at_ms: Date.now() - 240_000 },
    };
    const r = read(s, 0, true);
    expect(r.tone).toBe("healthy");
    expect(r.title).toBe(`Reading a transaction in progress — ${(2_100_000).toLocaleString(undefined, { notation: "compact", maximumFractionDigits: 1 })} changes so far`);
    expect(r.detail).toBe("Open for 4 min, 1.3 GB held on disk. Its rows are delivered when it commits.");
  });

  it("puts a stall before an open transaction", () => {
    const s = status({ verdict: "stalled", stall_cause: "delivery_failing", owner: "destination" });
    s.source = { open_transaction: { xid: 1, changes: 1, bytes: 1, spilled: false, started_at_ms: Date.now() } };
    expect(read(s, 0, true).tone).toBe("stalled");
  });

  it("measures events per second between two samples, and refuses a counter that went back", () => {
    expect(rate({ at: 0, events: 0 }, { at: 2000, events: 10 })).toBe(5);
    expect(rate({ at: 0, events: 10 }, { at: 2000, events: 5 })).toBeNull();
    expect(rate(null, { at: 2000, events: 5 })).toBeNull();
  });
});

describe("the notification stream", () => {
  it("assembles frames however the bytes are cut", () => {
    const text = 'id: 1\nevent: notification\ndata: {"a":1}\n\n: keep-alive\n\nevent: notification\ndata: {"b":\ndata: 2}\n\n';
    for (const size of [1, 3, 7, text.length]) {
      const frames: Frame[] = [];
      const feed = parser((f) => frames.push(f));
      for (let i = 0; i < text.length; i += size) feed(text.slice(i, i + size));
      expect(frames).toEqual([
        { event: "notification", data: '{"a":1}', id: "1" },
        { event: "notification", data: '{"b":\n2}' },
      ]);
    }
  });
});

describe("a signal's outcome", () => {
  const entry = (sequence: number, detail: object) => ({ sequence, at: "", action: "", result: "", detail: JSON.stringify(detail) });

  it("is unknown until the audit trail mentions it", () => {
    expect(outcome([entry(1, { signal_id: "other", state: "COMPLETED" })], "mine")).toBeNull();
  });
  it("is running, then done", () => {
    expect(outcome([entry(1, { signal_id: "s", state: "IN_PROGRESS" })], "s")).toMatchObject({ done: false });
    expect(outcome([entry(1, { signal_id: "s", state: "IN_PROGRESS" }), entry(2, { signal_id: "s", state: "COMPLETED" })], "s")).toEqual({
      done: true,
      ok: true,
      state: "COMPLETED",
    });
  });
  // Accepted is not done: the case this exists for.
  it("carries the error of a signal the pipeline refused", () => {
    const refused = { signal_id: "s", state: "ABORTED", additional_data: { error: "no [incremental_snapshot] section is configured" } };
    expect(outcome([entry(1, { signal_id: "s", state: "STARTED" }), entry(2, refused)], "s")).toMatchObject({
      done: true,
      ok: false,
      error: "no [incremental_snapshot] section is configured",
    });
    expect(describeDetail(JSON.stringify(refused))).toBe("no [incremental_snapshot] section is configured");
    expect(describeDetail("from=Starting,to=Running")).toBe("from=Starting,to=Running");
  });
});

describe("the curl an action shows", () => {
  it("names the token's variable, never its value", () => {
    sessionStorage.setItem("rustcdc.write-token", "s3cret");
    const text = curl("POST", "/signals", { action_type: "execute_snapshot", tables: ["public.orders"] }, "write");
    expect(text).toContain("$RUSTCDC_ADMIN_WRITE_TOKEN");
    expect(text).toContain('"tables":["public.orders"]');
    expect(text).not.toContain("s3cret");
  });
});
