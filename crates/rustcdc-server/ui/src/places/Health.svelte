<script lang="ts">
  // Is it healthy, and if not, whose problem is it? A sentence first, then the evidence.
  import Symbol from "../components/Symbol.svelte";
  import Pipeline from "../components/Pipeline.svelte";
  import Sparkline from "../components/Sparkline.svelte";
  import type { Status } from "../lib/api";
  import type { Reading } from "../lib/health";
  import { ago, count, duration, secondsSince } from "../lib/format";

  let {
    status,
    reading,
    rates,
    eventsPerSecond,
    source,
    destination,
  }: {
    status: Status | null;
    reading: Reading;
    rates: number[];
    eventsPerSecond: number | null;
    source: string;
    destination: string;
  } = $props();

  const lastBatch = $derived(secondsSince(status?.last_batch_at));
  const uptime = $derived(secondsSince(status?.started_at));
  const lag = $derived(status?.slo.source_lag_seconds ?? status?.slo.checkpoint_age_seconds ?? null);
  const icon = $derived(reading.tone === "stalled" ? "warning" : reading.tone === "healthy" ? "check" : "info");
  const word = $derived(reading.tone === "stalled" ? "Stalled" : reading.tone === "healthy" ? "Healthy" : "Attention");
</script>

<div class="health">
  <section class="verdict card {reading.tone}" aria-live="polite">
    <span class="state {reading.tone}"><Symbol name={icon} size={13} />{word}</span>
    <h1>{reading.title}</h1>
    {#if reading.detail}<p class="detail">{reading.detail}</p>{/if}
    {#if reading.next}<p class="next"><strong>What to do:</strong> {reading.next}</p>{/if}
    {#if reading.runbook}
      <a class="runbook" href={reading.runbook} target="_blank" rel="noreferrer"><Symbol name="link" size={13} />Runbook</a>
    {/if}
  </section>

  <section class="card strip">
    <h2 class="section-title">Pipeline</h2>
    <Pipeline tone={reading.tone} hop={reading.hop} {source} {destination} />
  </section>

  <section class="card pulse">
    <h2 class="section-title">Throughput</h2>
    <div class="pulse-row">
      <div>
        <div class="big num">{eventsPerSecond === null ? "—" : count(eventsPerSecond)}</div>
        <div class="muted">events per second</div>
      </div>
      <Sparkline values={rates} label="Events per second over the last five minutes" />
    </div>
  </section>

  <dl class="facts">
    <div class="card"><dt>Last batch</dt><dd class="num">{lastBatch === null ? "none yet" : ago(lastBatch)}</dd></div>
    <div class="card"><dt>Source lag</dt><dd class="num">{lag === null ? "—" : duration(lag)}</dd></div>
    <div class="card"><dt>Events captured</dt><dd class="num">{status ? count(status.events_processed) : "—"}</dd></div>
    <div class="card"><dt>Running for</dt><dd class="num">{uptime === null ? "—" : duration(uptime)}</dd></div>
  </dl>
</div>

<style>
  .health {
    height: 100%;
    overflow: auto;
    padding: var(--s5);
    display: grid;
    grid-template-columns: repeat(2, minmax(0, 1fr));
    gap: var(--s4);
    align-content: start;
  }
  .verdict {
    grid-column: 1 / -1;
    display: grid;
    gap: var(--s2);
    justify-items: start;
    border-left: 4px solid var(--healthy);
  }
  .verdict.attention { border-left-color: var(--attention); }
  .verdict.stalled { border-left-color: var(--stalled); }
  h1 {
    margin: 0;
    font: 700 22px/1.25 var(--display);
  }
  .detail, .next {
    margin: 0;
    max-width: 70ch;
    font-size: 14px;
  }
  .next { color: var(--text-2); }
  .runbook {
    display: inline-flex;
    gap: 5px;
    align-items: center;
    color: var(--accent-ink);
    text-decoration: none;
  }
  .runbook:hover { text-decoration: underline; }
  .strip { grid-column: 1 / -1; }
  .pulse { grid-column: 1 / -1; }
  .pulse-row {
    display: flex;
    align-items: flex-end;
    justify-content: space-between;
    gap: var(--s5);
    flex-wrap: wrap;
  }
  .big {
    font: 700 32px/1 var(--display);
  }
  .facts {
    grid-column: 1 / -1;
    margin: 0;
    display: grid;
    grid-template-columns: repeat(4, minmax(0, 1fr));
    gap: var(--s4);
  }
  dt {
    font-size: 12px;
    color: var(--muted);
  }
  dd {
    margin: 4px 0 0;
    font: 600 17px/1.2 var(--display);
  }
  @media (max-width: 900px) {
    .facts { grid-template-columns: repeat(2, minmax(0, 1fr)); }
  }
</style>
