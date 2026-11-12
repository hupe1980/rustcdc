<script lang="ts">
  // What happened: the live notification stream and the audit trail, newest first.
  import Symbol from "../components/Symbol.svelte";
  import { ApiError, stream, get, post, type AuditEntry, type CloudEvent, type Notification, type SignalAccepted, type Status } from "../lib/api";
  import { read } from "../lib/sse";
  import { ago, secondsSince } from "../lib/format";
  import { describe, follow, newSignalId } from "../lib/signals";

  let { audit, onneedwrite }: { audit: AuditEntry[]; onneedwrite: (retry: () => void) => void } = $props();

  // A marker is a line in the stream and the audit trail — "deploy 4.2 starts now" — that
  // makes the moment findable later, downstream as well as here.
  let marker = $state("");
  let marking = $state(false);
  let markResult = $state<{ ok: boolean; text: string } | null>(null);

  async function mark(e?: SubmitEvent, signalId = newSignalId(), text = marker.trim()) {
    e?.preventDefault();
    if (!text) return;
    marking = true;
    markResult = null;
    try {
      const accepted = await post<SignalAccepted>("/signals", { action_type: "log_marker", message: text, signal_id: signalId });
      marker = "";
      const result = await follow(accepted.signal_id, async () => (await get<Status>("/status")).audit.recent_entries, () => {}, 15);
      markResult = result?.ok === false ? { ok: false, text: result.error ?? "The marker was refused." } : { ok: true, text: `Marked: ${text}` };
    } catch (err) {
      const error = err as ApiError;
      if (error.status === 401 || error.status === 403) onneedwrite(() => void mark(undefined, signalId, text));
      else markResult = { ok: false, text: error.message };
    } finally {
      marking = false;
    }
  }

  type Item = { key: string; at: string; action: string; result: string; detail?: string; kind: "event" | "audit"; signal?: string };
  let live = $state<Item[]>([]);
  let paused = $state(false);
  let filter = $state("");
  let connected = $state<"connecting" | "live" | "lost">("connecting");
  let held = $state<Item[]>([]);

  // A marker's text, a refusal's reason, or nothing: what a person reading the feed wants.
  const toItem = (n: Omit<Notification, "id">, key: string): Item => ({
    key,
    at: n.at,
    action: n.action_type,
    result: n.state.toLowerCase(),
    detail: n.detail?.message || n.detail?.additional_data?.error || undefined,
    kind: "event",
    signal: n.signal_id,
  });

  function add(item: Item) {
    if (paused) held = [item, ...held];
    else live = [item, ...live].slice(0, 300);
  }

  $effect(() => {
    const abort = new AbortController();
    (async () => {
      try {
        const first = await get<{ notifications: Notification[] }>("/notifications");
        live = first.notifications
          .slice()
          .reverse()
          .map((n) => toItem(n, `n${n.id}`));
        const body = await stream("/notifications/stream", abort.signal);
        connected = "live";
        await read(
          body,
          (frame) => {
            if (frame.event !== "notification") return;
            const ce = JSON.parse(frame.data) as CloudEvent;
            add(toItem({ ...ce.data, at: ce.time }, `n${ce.id}`));
          },
          abort.signal,
        );
        if (!abort.signal.aborted) connected = "lost";
      } catch {
        if (!abort.signal.aborted) connected = "lost";
      }
    })();
    return () => abort.abort();
  });

  function resume() {
    paused = false;
    live = [...held, ...live].slice(0, 300);
    held = [];
  }

  const items = $derived.by(() => {
    // Signal steps are in the stream already; the audit trail adds everything else.
    const fromAudit: Item[] = audit.filter((a) => !a.action.startsWith("signal_")).map((a) => ({
      key: `a${a.sequence}`,
      at: a.at,
      action: a.action,
      result: a.result,
      detail: describe(a.detail),
      kind: "audit" as const,
    }));
    // One row per signal, at its latest step: started → in progress → completed is one thing that
    // happened, not three.
    const seen = new Set<string>();
    const all = [...live, ...fromAudit]
      .sort((a, b) => Date.parse(b.at) - Date.parse(a.at) || b.key.localeCompare(a.key, undefined, { numeric: true }))
      .filter((i) => {
        const id = i.signal ?? i.key;
        if (seen.has(id)) return false;
        seen.add(id);
        return true;
      });
    const q = filter.trim().toLowerCase();
    return q ? all.filter((i) => `${i.action} ${i.result} ${i.detail ?? ""}`.toLowerCase().includes(q)) : all;
  });

  const tone = (result: string) => (/(fail|error|reject|denied)/i.test(result) ? "stalled" : /(complet|succe|ok|accept)/i.test(result) ? "healthy" : "attention");
</script>

<div class="activity">
  <div class="bar">
    <span class="state {connected === 'live' ? 'healthy' : connected === 'lost' ? 'stalled' : 'attention'}">
      <Symbol name={connected === "live" ? "pulse" : connected === "lost" ? "warning" : "info"} size={12} />
      {connected === "live" ? "Live" : connected === "lost" ? "Stream ended — reload to reconnect" : "Connecting"}
    </span>
    <label class="search">
      <Symbol name="search" size={14} />
      <span class="sr-only">Filter</span>
      <input type="search" bind:value={filter} placeholder="Filter by table, action or result" />
    </label>
    <form class="marker" onsubmit={mark}>
      <label for="marker" class="sr-only">Marker text</label>
      <input id="marker" type="text" bind:value={marker} placeholder="Add a marker — e.g. deploy 4.2 starts" />
      <button class="btn" type="submit" disabled={marking || !marker.trim()}>Mark</button>
    </form>
    {#if paused}
      <button class="btn" onclick={resume}><Symbol name="play" size={13} />Resume{held.length ? ` (${held.length} new)` : ""}</button>
    {:else}
      <button class="btn" onclick={() => (paused = true)}><Symbol name="pause" size={13} />Pause</button>
    {/if}
  </div>

  {#if markResult}<p class="mark-result {markResult.ok ? 'ok' : 'bad'}" role="status">{markResult.text}</p>{/if}
  {#if items.length}
    <ol class="feed">
      {#each items as item (item.key)}
        <li>
          <time class="muted num" datetime={item.at} title={item.at}>{ago(secondsSince(item.at) ?? 0)}</time>
          <span class="what"><strong>{item.action.replaceAll("_", " ")}</strong>{#if item.detail}<span class="muted mono detail">{item.detail}</span>{/if}</span>
          <span class="state {tone(item.result)}">{item.result}</span>
          <span class="kind muted">{item.kind === "audit" ? "audit" : "event"}</span>
        </li>
      {/each}
    </ol>
  {:else}
    <p class="muted empty">Nothing yet. Snapshot steps, signals and schema changes appear here as they happen.</p>
  {/if}
</div>

<style>
  .activity {
    height: 100%;
    display: flex;
    flex-direction: column;
    min-height: 0;
  }
  .bar {
    display: flex;
    align-items: center;
    gap: var(--s3);
    padding: var(--s3) var(--s5);
    border-bottom: 0.5px solid var(--separator);
  }
  .search {
    flex: 1;
    display: flex;
    align-items: center;
    gap: 6px;
    color: var(--muted);
    max-width: 420px;
  }
  .search input { flex: 1; }
  .marker { display: flex; gap: 6px; }
  .marker input { width: 260px; }
  .mark-result { margin: 0; padding: 6px var(--s5); }
  .mark-result.ok { color: var(--healthy); }
  .mark-result.bad { color: var(--stalled); }
  .feed {
    list-style: none;
    margin: 0;
    padding: var(--s2) var(--s5) var(--s5);
    overflow: auto;
  }
  .feed li {
    display: grid;
    grid-template-columns: 88px minmax(0, 1fr) auto 48px;
    gap: var(--s3);
    align-items: center;
    padding: 8px 0;
    border-bottom: 0.5px solid var(--separator);
  }
  .what {
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .detail { margin-left: 8px; }
  .kind { font-size: 11px; text-align: right; }
  .empty { padding: var(--s5); margin: 0; }
</style>
