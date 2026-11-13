<script lang="ts">
  // Events per second over the last few minutes, sampled in the browser. History belongs to
  // Prometheus; this is only the pulse.
  let { values, label }: { values: number[]; label: string } = $props();
  const W = 240;
  const H = 40;
  const path = $derived.by(() => {
    if (values.length < 2) return "";
    const max = Math.max(1, ...values);
    const step = W / (values.length - 1);
    return values.map((v, i) => `${i ? "L" : "M"}${(i * step).toFixed(1)},${(H - 2 - (v / max) * (H - 4)).toFixed(1)}`).join(" ");
  });
</script>

<svg viewBox="0 0 {W} {H}" width={W} height={H} role="img" aria-label={label}>
  {#if path}
    <path d="{path} L{W},{H} L0,{H} Z" class="area" />
    <path d={path} class="line" />
  {:else}
    <line x1="0" y1={H - 2} x2={W} y2={H - 2} class="line idle" />
  {/if}
</svg>

<style>
  svg { display: block; max-width: 100%; }
  .line { fill: none; stroke: var(--accent); stroke-width: 1.6; stroke-linejoin: round; }
  .line.idle { stroke: var(--separator); }
  .area { fill: var(--accent-soft); }
</style>
