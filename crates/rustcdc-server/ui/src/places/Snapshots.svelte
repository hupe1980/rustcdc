<script lang="ts">
  // Drive an incremental snapshot and watch it. Every action shows the request it sends.
  import Symbol from "../components/Symbol.svelte";
  import CurlLine from "../components/CurlLine.svelte";
  import { ApiError, get, post, type SignalAction, type SignalAccepted, type SnapshotProgress, type Status } from "../lib/api";
  import { follow, newSignalId } from "../lib/signals";
  import { curl } from "../lib/curl";
  import { count } from "../lib/format";

  let {
    progress,
    tables,
    enabled,
    onneedwrite,
  }: {
    progress: SnapshotProgress | null | undefined;
    tables: string[];
    /** `false` when the config has no `[incremental_snapshot]`; `null` when unknown. */
    enabled: boolean | null;
    /** Ask for the write token, then run `retry`. */
    onneedwrite: (retry: () => void) => void;
  } = $props();

  let chosen = $state<string[]>([]);
  let typed = $state("");
  /** Per-table SQL row filters (`conditions`), keyed by table. */
  let filters = $state<Record<string, string>>({});
  let busy = $state(false);
  let message = $state<{ tone: "healthy" | "attention" | "stalled"; text: string } | null>(null);

  const requested = $derived([...new Set([...chosen, ...typed.split(/[\s,]+/).filter(Boolean)])]);
  const running = $derived(!!progress && !progress.stopped && progress.tables.some((t) => !t.is_complete));
  const conditions = $derived(
    Object.fromEntries(requested.map((t) => [t, (filters[t] ?? "").trim()]).filter(([, sql]) => sql)) as Record<string, string>,
  );
  const startBody = $derived({
    action_type: "execute_snapshot",
    tables: requested,
    ...(Object.keys(conditions).length ? { conditions } : {}),
  });

  const DONE: Record<string, string> = {
    pause_snapshot: "Paused.",
    resume_snapshot: "Resumed.",
    stop_snapshot: "Stopped.",
  };

  async function watchTables(names: string[]): Promise<{ tone: "healthy" | "attention" | "stalled"; text: string }> {
    for (let i = 0; i < 1800; i++) {
      const snap = (await get<Status>("/status")).incremental_snapshot;
      const mine = snap?.tables.filter((t) => names.includes(t.table)) ?? [];
      const rows = mine.reduce((n, t) => n + t.rows_emitted, 0);
      if (snap?.stopped) return { tone: "attention", text: `Stopped after ${count(rows)} rows.` };
      if (mine.length === names.length && mine.every((t) => t.is_complete))
        return { tone: "healthy", text: `Snapshot complete — ${count(rows)} ${rows === 1 ? "row" : "rows"}.` };
      message = { tone: "attention", text: `Reading — ${count(rows)} rows so far…` };
      await new Promise((r) => setTimeout(r, 1000));
    }
    return { tone: "attention", text: "Still reading. The card above shows each table." };
  }

  async function act(action: SignalAction, body: Record<string, unknown> = {}) {
    busy = true;
    message = null;
    // Chosen once: a retry after the write token is entered resends the same signal.
    const request = { signal_id: newSignalId(), ...body, action_type: action };
    try {
      const accepted = await post<SignalAccepted>("/signals", request);
      message = { tone: "attention", text: "Accepted — waiting for the pipeline…" };
      if (action === "execute_snapshot") {
        chosen = [];
        typed = "";
      }
      if (action === "execute_snapshot") filters = {};
      // Accepted is not done: follow the signal on the audit trail to what actually happened.
      const result = await follow(
        accepted.signal_id,
        async () => (await get<Status>("/status")).audit.recent_entries,
        (step) => (message = { tone: "attention", text: `Running — ${step.state.toLowerCase().replaceAll("_", " ")}…` }),
      );
      if (result?.ok && action === "execute_snapshot") {
        // The signal completes when the tables are queued, not when they are read: follow the
        // snapshot itself until every requested table is done.
        message = await watchTables(body.tables as string[]);
        return;
      }
      message = !result
        ? { tone: "attention", text: "Still running. Activity shows how it ends." }
        : result.ok
          ? { tone: "healthy", text: DONE[action] ?? "Done." }
          : { tone: "stalled", text: result.error ?? `The pipeline answered ${result.state.toLowerCase()}.` };
    } catch (e) {
      const err = e as ApiError;
      if (err.status === 401 || err.status === 403) onneedwrite(() => void act(action, { ...body, signal_id: request.signal_id }));
      else message = { tone: "stalled", text: err.message };
    } finally {
      busy = false;
    }
  }
</script>

<div class="snapshots">
  <section class="card">
    <h2 class="section-title">Latest snapshot</h2>
    {#if progress && progress.tables.length}
      <div class="head">
        <span class="state {progress.stopped ? 'stalled' : progress.paused ? 'attention' : running ? 'healthy' : 'healthy'}">
          <Symbol name={progress.stopped ? "stop" : progress.paused ? "pause" : running ? "play" : "check"} size={12} />
          {progress.stopped ? "Stopped" : progress.paused ? "Paused" : running ? "Running" : "Complete"}
        </span>
        <span class="muted mono">{progress.snapshot_id}</span>
        <span class="spacer"></span>
        {#if running && !progress.paused}
          <button class="btn" disabled={busy} onclick={() => act("pause_snapshot")}><Symbol name="pause" size={13} />Pause</button>
        {/if}
        {#if progress.paused}
          <button class="btn" disabled={busy} onclick={() => act("resume_snapshot")}><Symbol name="play" size={13} />Resume</button>
        {/if}
        {#if running || progress.paused}
          <button class="btn" disabled={busy} onclick={() => act("stop_snapshot")}><Symbol name="stop" size={13} />Stop</button>
        {/if}
      </div>
      <ul class="tables">
        {#each progress.tables as t (t.table)}
          <li>
            <span class="mono">{t.table}</span>
            <!-- No total is known while a table is read, so a running table is indeterminate. -->
            <progress max="1" value={t.is_complete ? 1 : undefined} aria-label="{t.table}: {t.is_complete ? 'done' : 'reading'}"></progress>
            <span class="num muted stats">{count(t.rows_emitted)} {t.rows_emitted === 1 ? "row" : "rows"} · {t.chunks_emitted} {t.chunks_emitted === 1 ? "chunk" : "chunks"}{t.is_complete ? " · done" : ""}</span>
          </li>
        {/each}
      </ul>
    {:else}
      <p class="muted empty">No snapshot is running. Start one below — capture keeps streaming while it reads.</p>
    {/if}
  </section>

  <section class="card">
    <h2 class="section-title">Start a snapshot</h2>
    {#if enabled === false}
      <p class="off">
        <Symbol name="info" size={14} />
        <span>Incremental snapshots are off for this pipeline. Add an <code>[incremental_snapshot]</code> section to the configuration
        (its <code>tables</code> may be empty) and restart; then tables can be re-read here while capture keeps streaming.</span>
      </p>
    {:else}
    {#if tables.length}
      <fieldset>
        <legend class="sr-only">Tables to snapshot</legend>
        {#each tables as table (table)}
          <label class="pick"><input type="checkbox" bind:group={chosen} value={table} /><span class="mono">{table}</span></label>
        {/each}
      </fieldset>
    {/if}
    <label class="other">
      <span>{tables.length ? "Or name tables" : "Tables"}</span>
      <input type="text" bind:value={typed} placeholder="schema.table, schema.other" spellcheck="false" />
    </label>
    <div class="go">
      <button
        class="btn primary"
        disabled={busy || !requested.length}
        onclick={() => act("execute_snapshot", { tables: requested, ...(Object.keys(conditions).length ? { conditions } : {}) })}
      >
        <Symbol name="snapshot" size={14} />Snapshot {requested.length || ""} {requested.length === 1 ? "table" : "tables"}
      </button>
      {#if message}<p class="message {message.tone}" role="status">{message.text}</p>{/if}
    </div>
    {#if requested.length}
      <details class="filters">
        <summary>Only some rows <span class="muted">(optional)</span></summary>
        <p class="muted note">
          A SQL condition per table, appended to each chunk's <code>WHERE</code>: <code>updated_at &gt; now() - interval '1 day'</code>.
          It is raw SQL with the trust of the connection string — not a tenancy boundary.
        </p>
        {#each requested as table (table)}
          <label class="filter">
            <span class="mono">{table}</span>
            <input type="text" bind:value={filters[table]} placeholder="every row" spellcheck="false" />
          </label>
        {/each}
      </details>
    {/if}
    <details>
      <summary>The request this sends</summary>
      <CurlLine command={curl("POST", "/signals", startBody, "write")} />
    </details>
    {/if}
  </section>
</div>

<style>
  .snapshots {
    height: 100%;
    overflow: auto;
    padding: var(--s5);
    display: grid;
    gap: var(--s4);
    align-content: start;
    max-width: 960px;
  }
  .head {
    display: flex;
    align-items: center;
    gap: var(--s3);
    margin-bottom: var(--s3);
  }
  .spacer { flex: 1; }
  .tables {
    list-style: none;
    margin: 0;
    padding: 0;
    display: grid;
    gap: var(--s2);
  }
  .tables li {
    display: grid;
    grid-template-columns: minmax(140px, 1fr) 2fr 200px;
    align-items: center;
    gap: var(--s3);
  }
  .stats { text-align: right; }
  progress {
    width: 100%;
    height: 6px;
    accent-color: var(--accent);
  }
  .empty { margin: 0; }
  fieldset {
    border: 0;
    margin: 0 0 var(--s3);
    padding: 0;
    display: flex;
    flex-wrap: wrap;
    gap: var(--s2) var(--s4);
  }
  .pick {
    display: inline-flex;
    gap: 6px;
    align-items: center;
  }
  .other {
    display: grid;
    gap: 4px;
    max-width: 420px;
  }
  .go {
    display: flex;
    align-items: center;
    gap: var(--s3);
    margin: var(--s4) 0 var(--s3);
    flex-wrap: wrap;
  }
  .message { margin: 0; }
  .message.healthy { color: var(--healthy); }
  .message.attention { color: var(--attention); }
  .off { display: flex; gap: 8px; align-items: flex-start; margin: 0; max-width: 70ch; color: var(--text-2); }
  .message.stalled { color: var(--stalled); }
  .filters { margin-bottom: var(--s3); }
  .note { margin: 0 0 var(--s2); max-width: 70ch; }
  .filter { display: grid; grid-template-columns: minmax(140px, 220px) 1fr; gap: var(--s3); align-items: center; margin-bottom: 6px; }
  .filter input { font-family: var(--mono); }
  summary {
    cursor: default;
    color: var(--accent-ink);
    margin-bottom: var(--s2);
  }
</style>
