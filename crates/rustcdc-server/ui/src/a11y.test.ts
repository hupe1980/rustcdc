// The real App, mounted in happy-dom with the admin API stubbed: axe on every place and sheet, the
// click budgets, and that everything a pointer can do a keyboard can reach. Contrast needs
// layout, which happy-dom has not got; contrast.test.ts checks it from the tokens instead.
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { flushSync, mount, unmount } from "svelte";
import axe from "axe-core";
import App from "./App.svelte";
import { forget, remember } from "./lib/session";

let health: Record<string, unknown> = { verdict: "healthy", stall_cause: null, stalled_for_seconds: null, owner: null, restart_helps: null };
const signals: unknown[] = [];
const attempts: string[] = [];

const status = () => ({
  state: "running",
  health,
  started_at: new Date(Date.now() - 3_600_000).toISOString(),
  last_batch_at: new Date().toISOString(),
  events_processed: 1240,
  batches_processed: 31,
  slo: { source_lag_seconds: 1.2 },
  // After a snapshot signal: the signal completes (tables queued), and the table reports done.
  incremental_snapshot: signals.length
    ? { snapshot_id: "incremental-1", paused: false, tables: [{ table: "public.orders", rows_emitted: 12, chunks_emitted: 3, is_complete: true }] }
    : null,
  audit: {
    entries_total: 1,
    recent_entries: [
      { sequence: 1, at: new Date().toISOString(), action: "admin_state_transition", result: "accepted", detail: "from=Starting,to=Running" },
      ...(signals as { signal_id: string }[]).map((sig, i) => ({
        sequence: 2 + i,
        at: new Date().toISOString(),
        action: "signal_execute_snapshot_completed",
        result: "completed",
        detail: JSON.stringify({ signal_id: sig.signal_id, state: "COMPLETED" }),
      })),
    ],
  },
});

function route(method: string, path: string, body: string | undefined, token: string | undefined): [number, unknown] {
  if (!token) return [401, { error: "missing bearer token" }];
  if (path === "/status") return [200, status()];
  if (path === "/config") return [200, { source: { type: "postgres", table_include_list: ["public.orders", "public.customers"] }, sink: { type: "kafka" }, incremental_snapshot: { tables: [] } }];
  if (path === "/notifications")
    return [200, { notifications: [{ id: 1, action_type: "log_marker", state: "COMPLETED", at: new Date().toISOString(), detail: { message: "deploy 4.2 starts" } }] }];
  if (path === "/signals" && method === "POST") {
    attempts.push((JSON.parse(body!) as { signal_id: string }).signal_id);
    if (token !== "write") return [401, { error: "write scope required" }];
    const request = JSON.parse(body!) as { signal_id: string; action_type: string };
    signals.push(request);
    return [200, { signal_id: request.signal_id, action_type: request.action_type, state: "STARTED" }];
  }
  if (path.startsWith("/schemas/")) return [200, { id: "abc", schema: "public", table: "orders", columns: [{ name: "id", data_type: "bigint", nullable: false }], primary_key: ["id"] }];
  return [404, { error: `no fixture for ${method} ${path}` }];
}

let app: ReturnType<typeof mount> | undefined;
let clicks = 0;

async function settle() {
  for (let i = 0; i < 20; i++) {
    await new Promise((r) => setTimeout(r, 0));
    flushSync();
  }
}
async function open(hash: string) {
  location.hash = hash;
  window.dispatchEvent(new HashChangeEvent("hashchange"));
  await settle();
}
function button(name: string | RegExp): HTMLButtonElement {
  const found = [...document.querySelectorAll("button")].find((b) =>
    typeof name === "string" ? b.textContent?.trim() === name : name.test(b.textContent ?? ""),
  );
  if (!found) throw new Error(`no button ${name}`);
  return found;
}
async function click(target: string | RegExp | HTMLElement) {
  clicks++;
  (typeof target === "object" && "click" in target && !(target instanceof RegExp) ? target : button(target as string | RegExp)).click();
  await settle();
}
async function violations() {
  const result = await axe.run(document.body, { rules: { "color-contrast": { enabled: false } } });
  return result.violations.map((v) => `${v.id}: ${v.nodes.map((n) => n.target.join(" ")).join(", ")}`);
}

beforeEach(() => {
  vi.stubGlobal("fetch", async (path: string, init?: RequestInit) => {
    const token = (init?.headers as Record<string, string> | undefined)?.Authorization?.replace("Bearer ", "");
    if (path === "/notifications/stream") return new Response(new ReadableStream({ start() {} }), { status: 200 });
    const [code, data] = route(init?.method ?? "GET", path, init?.body as string | undefined, token);
    return new Response(JSON.stringify(data), { status: code, headers: { "Content-Type": "application/json" } });
  });
  vi.stubGlobal("matchMedia", (q: string) => ({ matches: false, media: q, addEventListener() {}, removeEventListener() {} }));
  location.hash = "";
  health = { verdict: "healthy", stall_cause: null, stalled_for_seconds: null, owner: null, restart_helps: null };
  signals.length = 0;
  attempts.length = 0;
  clicks = 0;
});
afterEach(() => {
  if (app) unmount(app);
  app = undefined;
  forget();
  document.body.innerHTML = "";
  vi.unstubAllGlobals();
});

async function start(signedIn: boolean) {
  if (signedIn) remember("read", "read");
  app = mount(App, { target: document.body.appendChild(document.createElement("div")) });
  await settle();
}

describe("accessibility (axe, every place and sheet)", () => {
  it("sign-in", async () => {
    await start(false);
    expect(document.body.textContent).toContain("Sign in with the admin read token");
    expect(await violations()).toEqual([]);
  });

  it("health, snapshots, activity, shapes, the write-token sheet and ⌘K", async () => {
    await start(true);
    for (const place of ["#/health", "#/snapshots", "#/activity", "#/shapes/abc"]) {
      await open(place);
      expect(await violations(), place).toEqual([]);
    }
    await open("#/snapshots");
    await click(document.querySelector<HTMLInputElement>('input[value="public.orders"]')!);
    await click(/^Snapshot 1 table/);
    expect(document.querySelector('[role="dialog"]')?.textContent).toContain("Admin write token");
    expect(await violations()).toEqual([]);
    await click("Cancel");
    window.dispatchEvent(new KeyboardEvent("keydown", { key: "k", metaKey: true }));
    await settle();
    expect(document.querySelector('[role="dialog"][aria-label="Go to"]')).not.toBeNull();
    expect(await violations()).toEqual([]);
  });
});

describe("click budgets", () => {
  it("the verdict, its owner and the remedy need no click", async () => {
    health = { verdict: "stalled", stall_cause: "delivery_failing", stalled_for_seconds: 90, owner: "destination", restart_helps: false };
    await start(true);
    const text = document.body.textContent ?? "";
    expect(text).toContain("Stalled for 1 min — the destination needs attention");
    expect(text).toContain("Restarting will not help");
    expect(document.querySelector(".hop.stalled")?.textContent).toContain("Kafka");
    expect(clicks).toBe(0);
  });

  it("a table's snapshot starts in three clicks, and the write token is asked for once and used", async () => {
    remember("write", "write");
    await start(true);
    await click(/Snapshots/);
    await click(document.querySelector<HTMLInputElement>('input[value="public.orders"]')!);
    await click(/^Snapshot 1 table/);
    expect(clicks).toBeLessThanOrEqual(3);
    expect(signals).toEqual([{ action_type: "execute_snapshot", tables: ["public.orders"], signal_id: expect.stringMatching(/^console-/) }]);
  });

  // A retry must not become a second snapshot: the same id goes out again.
  it("asks for the write token, then sends the same signal again by itself", async () => {
    await start(true);
    await open("#/snapshots");
    await click(document.querySelector<HTMLInputElement>('input[value="public.orders"]')!);
    await click(/^Snapshot 1 table/);
    const field = document.querySelector<HTMLInputElement>("#write-token")!;
    field.value = "write";
    field.dispatchEvent(new Event("input"));
    await settle();
    await click("Use it");
    expect(attempts).toHaveLength(2);
    expect(attempts[0]).toMatch(/^console-/);
    expect(attempts[1]).toBe(attempts[0]);
    expect(signals).toHaveLength(1);
  });
});

describe("outcomes", () => {
  // Accepted is not done, and neither is the signal's own COMPLETED: the tables are.
  it("says a snapshot is complete only when its tables are, with the rows read", async () => {
    remember("write", "write");
    await start(true);
    await open("#/snapshots");
    await click(document.querySelector<HTMLInputElement>('input[value="public.orders"]')!);
    await click(/^Snapshot 1 table/);
    await new Promise((r) => setTimeout(r, 2600));
    await settle();
    expect(document.querySelector('[role="status"]')?.textContent).toBe("Snapshot complete — 12 rows.");
  });
});

describe("markers", () => {
  it("a marker is a log_marker signal carrying the text", async () => {
    remember("write", "write");
    await start(true);
    await open("#/activity");
    const field = document.querySelector<HTMLInputElement>("#marker")!;
    field.value = "deploy 4.2 starts";
    field.dispatchEvent(new Event("input"));
    await settle();
    await click("Mark");
    expect(signals).toEqual([{ action_type: "log_marker", message: "deploy 4.2 starts", signal_id: expect.stringMatching(/^console-/) }]);
    expect(await violations()).toEqual([]);
  });
});

describe("keyboard", () => {
  it("every control is a native, focusable control", async () => {
    await start(true);
    for (const place of ["#/health", "#/snapshots", "#/activity", "#/shapes"]) {
      await open(place);
      const clickable = [...document.querySelectorAll<HTMLElement>("[onclick], .btn, button, a, input, summary")];
      for (const el of clickable) {
        expect(["BUTTON", "A", "INPUT", "SUMMARY"], `${place}: ${el.outerHTML.slice(0, 80)}`).toContain(el.tagName);
        expect(el.getAttribute("tabindex"), el.outerHTML.slice(0, 80)).not.toBe("-1");
      }
    }
  });

  it("⌘1–4 move between places", async () => {
    await start(true);
    for (const [key, place] of [["2", "Snapshots"], ["3", "Activity"], ["4", "Shapes"], ["1", "Health"]]) {
      window.dispatchEvent(new KeyboardEvent("keydown", { key: key!, metaKey: true }));
      await settle();
      expect(document.querySelector(".where")?.textContent).toBe(place);
    }
  });
});
