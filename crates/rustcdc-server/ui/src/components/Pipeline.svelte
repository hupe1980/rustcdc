<script lang="ts">
  // source → rustcdc → destination: where the problem is, at a glance. The stalled hop is lit and
  // labelled; the others stay quiet. Words travel with the colour, never colour alone.
  import Symbol from "./Symbol.svelte";
  import type { Hop, Tone } from "../lib/health";

  let { tone, hop, source = "Source", destination = "Destination" }: { tone: Tone; hop?: Hop; source?: string; destination?: string } = $props();
  const hops: { id: Hop; label: string; icon: string }[] = $derived([
    { id: "source", label: source, icon: "database" },
    { id: "rustcdc", label: "rustcdc", icon: "engine" },
    { id: "destination", label: destination, icon: "destination" },
  ]);
  const state = (id: Hop) => (tone === "stalled" && hop === id ? "stalled" : tone === "stalled" ? "quiet" : tone);
</script>

<ol class="pipeline" aria-label="Pipeline">
  {#each hops as h, i (h.id)}
    {#if i > 0}<li class="link {tone === 'stalled' && (hop === h.id || hops[i - 1]?.id === hop) ? 'broken' : tone}" aria-hidden="true"><span></span></li>{/if}
    <li class="hop {state(h.id)}">
      <span class="glyph"><Symbol name={h.icon} size={20} /></span>
      <span class="name">{h.label}</span>
      {#if tone === "stalled" && hop === h.id}<span class="badge"><Symbol name="warning" size={12} />Here</span>{/if}
    </li>
  {/each}
</ol>

<style>
  .pipeline {
    list-style: none;
    margin: 0;
    padding: 0;
    display: flex;
    align-items: flex-start;
  }
  .hop {
    display: flex;
    flex-direction: column;
    align-items: center;
    gap: 6px;
    min-width: 96px;
  }
  .glyph {
    width: 44px;
    height: 44px;
    border-radius: 12px;
    display: grid;
    place-items: center;
    background: var(--fill);
    color: var(--muted);
    transition: background 200ms var(--ease), color 200ms var(--ease);
  }
  .healthy .glyph { background: var(--healthy-soft); color: var(--healthy); }
  .attention .glyph { background: var(--attention-soft); color: var(--attention); }
  .stalled .glyph { background: var(--stalled-soft); color: var(--stalled); box-shadow: 0 0 0 2px var(--stalled); }
  .name { font-weight: 500; }
  .quiet .name { color: var(--muted); }
  .badge {
    display: inline-flex;
    align-items: center;
    gap: 3px;
    font-size: 11px;
    font-weight: 600;
    color: var(--stalled);
  }
  .link {
    flex: 1;
    min-width: 32px;
    margin-top: 21px;
  }
  .link span {
    display: block;
    height: 2px;
    border-radius: 1px;
    background: var(--separator);
  }
  .link.healthy span {
    background: linear-gradient(90deg, var(--healthy-soft), var(--healthy), var(--healthy-soft));
    background-size: 200% 100%;
    animation: flow 2.4s linear infinite;
  }
  .link.broken span {
    background: repeating-linear-gradient(90deg, var(--stalled) 0 6px, transparent 6px 10px);
  }
  @keyframes flow {
    from { background-position: 200% 0; }
    to { background-position: 0 0; }
  }
</style>
