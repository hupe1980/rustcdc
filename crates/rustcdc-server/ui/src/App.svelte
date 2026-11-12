<script lang="ts">
  // The window: a glass sidebar with the four places and a live status light, a toolbar, solid
  // content. `/status` is read every two seconds; everything on screen is derived from it.
  import Symbol from "./components/Symbol.svelte";
  import Palette, { type Command } from "./components/Palette.svelte";
  import Sheet from "./components/Sheet.svelte";
  import Health from "./places/Health.svelte";
  import Snapshots from "./places/Snapshots.svelte";
  import Activity from "./places/Activity.svelte";
  import Shapes from "./places/Shapes.svelte";
  import { ApiError, get, whenUnauthorized, type Status } from "./lib/api";
  import { read as reading, rate } from "./lib/health";
  import { forget, remember, token } from "./lib/session";

  type Place = "health" | "snapshots" | "activity" | "shapes";
  const PLACES: { id: Place; label: string; icon: string }[] = [
    { id: "health", label: "Health", icon: "pulse" },
    { id: "snapshots", label: "Snapshots", icon: "snapshot" },
    { id: "activity", label: "Activity", icon: "activity" },
    { id: "shapes", label: "Shapes", icon: "shape" },
  ];
  const POLL_MS = 2000;
  const KEEP = 150; // five minutes of samples

  let phase = $state<"starting" | "signin" | "ready">("starting");
  let open = $state(false); // the server asks for no token
  let signinError = $state("");
  let signinValue = $state("");

  let place = $state<Place>("health");
  let shapeId = $state("");
  let paletteOpen = $state(false);
  let writeOpen = $state(false);
  let writeValue = $state("");

  let status = $state<Status | null>(null);
  let reachable = $state(true);
  let samples = $state<{ at: number; events: number }[]>([]);
  let source = $state("Source");
  let destination = $state("Destination");
  let tables = $state<string[]>([]);
  let snapshotsEnabled = $state<boolean | null>(null);
  let retryAfterWrite: (() => void) | null = null;

  // ── Session ──────────────────────────────────────────────────────────────
  async function start() {
    try {
      status = await get<Status>("/status");
      open = !token("read");
      phase = "ready";
      route();
      void loadConfig();
    } catch (e) {
      const err = e as ApiError;
      if (err.status !== 401 && err.status !== 403) signinError = err.message;
      phase = "signin";
    }
  }
  whenUnauthorized((scope) => {
    if (scope === "write") {
      writeOpen = true;
      return;
    }
    if (phase === "ready") signinError = "The read token was not accepted any more. Sign in again.";
    phase = "signin";
  });
  $effect(() => {
    void start();
  });

  async function signIn(e: SubmitEvent) {
    e.preventDefault();
    if (!signinValue.trim()) return;
    remember("read", signinValue);
    signinValue = "";
    signinError = "";
    await start();
  }

  function signOut() {
    forget();
    status = null;
    samples = [];
    phase = "signin";
  }

  function saveWrite(e: SubmitEvent) {
    e.preventDefault();
    if (!writeValue.trim()) return;
    remember("write", writeValue);
    writeValue = "";
    writeOpen = false;
    // Carry on with what the operator was doing when the token was asked for.
    const retry = retryAfterWrite;
    retryAfterWrite = null;
    retry?.();
  }

  // ── Live state ───────────────────────────────────────────────────────────
  $effect(() => {
    if (phase !== "ready") return;
    const timer = setInterval(async () => {
      try {
        status = await get<Status>("/status");
        reachable = true;
        samples = [...samples, { at: Date.now(), events: status.events_processed }].slice(-KEEP);
      } catch (e) {
        if ((e as ApiError).status === 0 || (e as ApiError).status >= 500) reachable = false;
      }
    }, POLL_MS);
    return () => clearInterval(timer);
  });

  async function loadConfig() {
    try {
      const config = await get<Record<string, unknown>>("/config");
      const src = (config.source ?? {}) as { type?: string; table_include_list?: unknown };
      const names: Record<string, string> = { postgres: "PostgreSQL", mysql: "MySQL", mariadb: "MariaDB", sqlserver: "SQL Server", snowflake: "Snowflake" };
      if (src.type) source = names[src.type] ?? src.type;
      const sink = (config.sink ?? {}) as { type?: string };
      if (sink.type) destination = sink.type === "http" ? "HTTP" : sink.type[0]!.toUpperCase() + sink.type.slice(1);
      snapshotsEnabled = config.incremental_snapshot != null;
      if (Array.isArray(src.table_include_list)) tables = src.table_include_list.filter((t): t is string => typeof t === "string");
    } catch {
      /* the console works without it: generic names, typed table names */
    }
  }

  const rates = $derived(samples.slice(1).map((s, i) => rate(samples[i]!, s) ?? 0));
  const eventsPerSecond = $derived(samples.length >= 2 ? (rate(samples[samples.length - 2]!, samples[samples.length - 1]!) ?? null) : null);
  const verdict = $derived(reading(status, eventsPerSecond, reachable));

  // The tab is a status light: its title and icon follow the verdict, readable from another tab.
  $effect(() => {
    const mark = verdict.tone === "stalled" ? "▲" : verdict.tone === "healthy" ? "●" : "◆";
    const word = verdict.tone === "stalled" ? "Stalled" : verdict.tone === "healthy" ? "Healthy" : "Attention";
    document.title = phase === "ready" ? `${mark} ${word} — rustcdc` : "rustcdc";
    const fill = { healthy: "%2334c759", attention: "%23ff9500", stalled: "%23ff3b30" }[verdict.tone];
    const icon = document.querySelector<HTMLLinkElement>('link[rel="icon"]');
    if (icon && phase === "ready")
      icon.href = `data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 32 32'><rect width='32' height='32' rx='8' fill='${fill}'/><path d='M7 16h4l2.5-6 5 12 2.5-6h4' fill='none' stroke='white' stroke-width='2.6' stroke-linecap='round' stroke-linejoin='round'/></svg>`;
  });

  // ── Routing: #/health · #/snapshots · #/activity · #/shapes[/<id>] ──────
  function route() {
    const [, p, id] = location.hash.split("/");
    place = (PLACES.find((x) => x.id === p)?.id ?? "health") as Place;
    shapeId = place === "shapes" && id ? decodeURIComponent(id) : "";
  }
  function go(hash: string) {
    if (location.hash === hash) route();
    else location.hash = hash;
  }

  const commands = $derived<Command[]>([
    ...PLACES.map((p, i) => ({ id: `p-${p.id}`, title: p.label, hint: `⌘${i + 1}`, icon: p.icon, run: () => go(`#/${p.id}`) })),
    ...tables.map((t) => ({ id: `t-${t}`, title: t, hint: "Snapshot", icon: "snapshot", run: () => go("#/snapshots") })),
    { id: "a-write", title: "Enter the write token", icon: "key", run: () => (writeOpen = true) },
    ...(open ? [] : [{ id: "a-signout", title: "Sign out", icon: "signout", run: signOut }]),
  ]);

  function keys(e: KeyboardEvent) {
    if (phase !== "ready" || !(e.metaKey || e.ctrlKey)) return;
    const k = e.key.toLowerCase();
    if (k === "k") {
      e.preventDefault();
      paletteOpen = !paletteOpen;
    } else if (["1", "2", "3", "4"].includes(k)) {
      e.preventDefault();
      go(`#/${PLACES[Number(k) - 1]!.id}`);
    }
  }

  const title = $derived(PLACES.find((p) => p.id === place)!.label);
</script>

<svelte:window onhashchange={route} onkeydown={keys} />

{#if phase === "starting"}
  <div class="splash" aria-busy="true"><Symbol name="pulse" size={40} weight={1.2} /></div>
{:else if phase === "signin"}
  <main class="signin-page">
    <div class="mark" aria-hidden="true"><Symbol name="pulse" size={34} weight={1.8} /></div>
    <h1>rustcdc</h1>
    <p class="tagline">Sign in with the admin read token.</p>
    <form onsubmit={signIn} class="signin card">
      <label for="token" class="sr-only">Admin read token</label>
      <input id="token" type="password" bind:value={signinValue} placeholder="Admin read token" autocomplete="off" spellcheck="false" />
      <button class="btn primary large" type="submit" disabled={!signinValue.trim()}>Sign in</button>
      {#if signinError}<p class="error" role="alert"><Symbol name="warning" size={14} />{signinError}</p>{/if}
    </form>
    <p class="fine">
      Kept in this tab only — never in a cookie or a URL — and gone when the tab closes.<br />
      The token is the one in <code>admin.read_token_env</code>.
    </p>
  </main>
{:else}
  <div class="shell">
    <aside class="sidebar glass">
      <div class="brand"><Symbol name="pulse" size={18} /><span>rustcdc</span></div>
      <nav aria-label="Places">
        <ul class="places">
          {#each PLACES as p, i (p.id)}
            <li>
              <button class:on={place === p.id} aria-current={place === p.id ? "page" : undefined} onclick={() => go(`#/${p.id}`)}>
                <Symbol name={p.icon} size={16} /><span>{p.label}</span><span class="hint">⌘{i + 1}</span>
              </button>
            </li>
          {/each}
        </ul>
      </nav>
      <button class="light {verdict.tone}" onclick={() => go("#/health")} aria-label="Health: {verdict.title}">
        <span class="dot" aria-hidden="true"></span><span class="text">{verdict.title}</span>
      </button>
    </aside>

    <div class="main">
      <header class="toolbar glass">
        <h2 class="where">{title}</h2>
        <button class="btn find" onclick={() => (paletteOpen = true)} aria-label="Go to (⌘K)">
          <Symbol name="search" size={14} /><span>Go to…</span><kbd>⌘K</kbd>
        </button>
        {#if open}
          <span class="chip open" title="The server asks for no admin token"><Symbol name="warning" size={13} />No admin token configured</span>
        {:else}
          <button class="btn plain" onclick={signOut}><Symbol name="signout" size={14} />Sign out</button>
        {/if}
      </header>

      {#if !reachable}
        <p class="offline" role="alert"><Symbol name="warning" size={14} />The server is not answering. Showing the last known state.</p>
      {/if}

      <main class="content" class:stale={!reachable}>
        {#if place === "health"}
          <Health {status} reading={verdict} {rates} {eventsPerSecond} {source} {destination} />
        {:else if place === "snapshots"}
          <Snapshots
            progress={status?.incremental_snapshot}
            {tables}
            enabled={snapshotsEnabled}
            onneedwrite={(retry) => {
              retryAfterWrite = retry;
              writeOpen = true;
            }}
          />
        {:else if place === "activity"}
          <Activity
            audit={status?.audit.recent_entries ?? []}
            onneedwrite={(retry) => {
              retryAfterWrite = retry;
              writeOpen = true;
            }}
          />
        {:else}
          <Shapes id={shapeId} ongo={(id) => go(`#/shapes/${encodeURIComponent(id)}`)} />
        {/if}
      </main>
    </div>
  </div>

  {#if paletteOpen}<Palette {commands} onclose={() => (paletteOpen = false)} />{/if}
  {#if writeOpen}
    <Sheet title="Admin write token" onclose={() => (writeOpen = false)}>
      <form id="write-form" onsubmit={saveWrite} class="write">
        <p class="muted">Snapshots and signals need the token in <code>admin.write_token_env</code>. It stays in this tab.</p>
        <label for="write-token" class="sr-only">Admin write token</label>
        <input id="write-token" type="password" bind:value={writeValue} placeholder="Admin write token" autocomplete="off" spellcheck="false" />
      </form>
      {#snippet actions()}
        <button class="btn" onclick={() => (writeOpen = false)}>Cancel</button>
        <button class="btn primary" type="submit" form="write-form" disabled={!writeValue.trim()}>Use it</button>
      {/snippet}
    </Sheet>
  {/if}
{/if}

<style>
  .splash {
    height: 100%;
    display: grid;
    place-items: center;
    color: var(--faint);
  }
  .signin-page {
    height: 100%;
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    gap: 6px;
    padding: var(--s5) var(--s5) 12vh;
    background: radial-gradient(900px 500px at 50% -10%, var(--accent-soft), transparent 70%), var(--bg);
  }
  .mark {
    width: 72px;
    height: 72px;
    border-radius: 18px;
    display: grid;
    place-items: center;
    color: #fff;
    background: linear-gradient(180deg, #5ea2ff, var(--accent));
    box-shadow: 0 10px 30px rgba(0, 122, 255, 0.35), inset 0 1px 0 rgba(255, 255, 255, 0.35);
    margin-bottom: var(--s3);
  }
  .signin-page h1 { margin: 0; font: 700 26px/1.2 var(--display); }
  .tagline { margin: 0 0 var(--s4); color: var(--muted); font-size: 15px; }
  .signin {
    width: min(380px, 100%);
    display: grid;
    grid-template-columns: 1fr auto;
    gap: var(--s2);
    padding: var(--s3);
  }
  .signin input { font: 13px var(--mono); padding: 0 10px; height: 34px; }
  .signin .error { grid-column: 1 / -1; margin: 0; font-size: 12px; }
  .fine { margin: var(--s4) 0 0; text-align: center; font-size: 12px; color: var(--muted); max-width: 44ch; }
  .error { display: flex; gap: 6px; align-items: center; color: var(--stalled); }
  .shell { display: grid; grid-template-columns: 232px minmax(0, 1fr); height: 100%; }
  .sidebar { display: flex; flex-direction: column; border-right: 0.5px solid var(--separator); min-height: 0; }
  .brand { display: flex; align-items: center; gap: 8px; padding: 16px 16px 10px; font: 600 15px/1 var(--display); color: var(--accent-ink); }
  .brand span { color: var(--text); }
  .places { list-style: none; margin: 0; padding: 0 8px; }
  .places button {
    width: 100%;
    display: flex;
    align-items: center;
    gap: 8px;
    height: 30px;
    padding: 0 10px;
    border: none;
    border-radius: var(--r-sm);
    background: none;
    text-align: left;
    color: var(--text);
  }
  .places button:hover { background: var(--fill); }
  .places button.on { background: var(--fill-2); font-weight: 600; }
  .places button :global(svg) { color: var(--accent-ink); }
  .places span:nth-child(2) { flex: 1; }
  .hint { font-size: 11px; color: var(--muted); }
  .light {
    margin: auto 8px 12px;
    display: flex;
    align-items: flex-start;
    gap: 8px;
    padding: 10px 12px;
    border: none;
    border-radius: var(--r);
    text-align: left;
    background: var(--healthy-soft);
    color: var(--healthy);
    font-weight: 500;
  }
  .light.attention { background: var(--attention-soft); color: var(--attention); }
  .light.stalled { background: var(--stalled-soft); color: var(--stalled); }
  .dot { width: 8px; height: 8px; margin-top: 5px; border-radius: 50%; background: currentColor; flex: none; animation: beat 2s ease-in-out infinite; }
  .light.stalled .dot { animation-duration: 0.9s; }
  .text { color: var(--text); font-size: 12px; }
  @keyframes beat { 50% { opacity: 0.35; } }
  .main { display: flex; flex-direction: column; min-width: 0; min-height: 0; }
  .toolbar {
    display: flex;
    align-items: center;
    gap: var(--s3);
    height: 52px;
    padding: 0 var(--s4) 0 var(--s5);
    border-bottom: 0.5px solid var(--separator);
  }
  .where { flex: 1; margin: 0; font: 600 14px/1 var(--display); }
  .find { color: var(--muted); min-width: 180px; }
  .find span { flex: 1; text-align: left; }
  kbd { font: 11px var(--font); color: var(--muted); border: 0.5px solid var(--separator); border-radius: 4px; padding: 0 4px; }
  .chip { display: inline-flex; align-items: center; gap: 6px; height: 28px; padding: 0 12px; border-radius: 999px; font-size: 12px; }
  .chip.open { color: var(--attention); background: var(--attention-soft); }
  .offline { display: flex; gap: 6px; align-items: center; margin: 0; padding: 8px var(--s5); color: var(--attention); background: var(--attention-soft); }
  .content { flex: 1; min-height: 0; overflow: hidden; }
  .content.stale { opacity: 0.6; }
  .write { display: grid; gap: var(--s3); }
  .write p { margin: 0; }
  .write input { font: 13px var(--mono); height: 34px; padding: 0 10px; }
  @media (max-width: 760px) {
    .shell { grid-template-columns: minmax(0, 1fr); grid-template-rows: auto minmax(0, 1fr); }
    .sidebar { border-right: none; border-bottom: 0.5px solid var(--separator); }
    .light { margin: 8px; }
  }
</style>
