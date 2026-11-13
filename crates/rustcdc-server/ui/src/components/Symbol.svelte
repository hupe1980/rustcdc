<script lang="ts" module>
  // Monochrome glyphs in the spirit of SF Symbols (1.6 stroke on a 20-point grid), drawn for this
  // project — not Apple's. Decorative by default: the word next to a symbol carries the meaning.
  const paths: Record<string, string> = {
    pulse: "M2.5 10h3l2-5 4 10 2-5h4",
    snapshot: "M3.5 6.5h3l1.3-2h4.4l1.3 2h3v9h-13z M10 13.2a2.5 2.5 0 1 0 0-5 2.5 2.5 0 0 0 0 5z",
    activity: "M4 5h12 M4 10h12 M4 15h8",
    shape: "M3.5 4.5h13v11h-13z M3.5 8.2h13 M3.5 11.8h13 M8 8.2v7.3",
    database: "M4 5.5c0-1.4 2.7-2.5 6-2.5s6 1.1 6 2.5-2.7 2.5-6 2.5-6-1.1-6-2.5z M4 5.5v9c0 1.4 2.7 2.5 6 2.5s6-1.1 6-2.5v-9 M4 10c0 1.4 2.7 2.5 6 2.5s6-1.1 6-2.5",
    engine: "M10 12.5a2.5 2.5 0 1 0 0-5 2.5 2.5 0 0 0 0 5z M10 2.8v2 M10 15.2v2 M2.8 10h2 M15.2 10h2 M4.9 4.9l1.4 1.4 M13.7 13.7l1.4 1.4 M4.9 15.1l1.4-1.4 M13.7 6.3l1.4-1.4",
    destination: "M3 10h11 M10.5 5.5L15 10l-4.5 4.5 M17 4v12",
    play: "M6.5 4.5l9 5.5-9 5.5z",
    pause: "M7 4.5v11 M13 4.5v11",
    stop: "M5 5h10v10H5z",
    search: "M8.8 14.2a5.4 5.4 0 1 0 0-10.8 5.4 5.4 0 0 0 0 10.8z M12.8 12.8l3.7 3.7",
    copy: "M7 7h8.5v9.5H7z M4.5 13V3.5H13",
    check: "M4.5 10.5l3.5 3.5 7.5-8",
    xmark: "M5.5 5.5l9 9 M14.5 5.5l-9 9",
    warning: "M10 3.5l7 12.5H3z M10 8.5v3.5 M10 14.3h0",
    info: "M10 17a7 7 0 1 0 0-14 7 7 0 0 0 0 14z M10 9v4.5 M10 6.6h0",
    key: "M12.5 10.3a3.8 3.8 0 1 0-3.4-2.1L3.5 13.8V16.5h2.7v-1.7h1.7v-1.7h1.6l1.4-1.4c.5.4 1 .6 1.6.6z M13.6 6.4h0",
    signout: "M8 4.5H4.5v11H8 M11.5 6.5L15 10l-3.5 3.5 M15 10H7.5",
    link: "M8.5 11.5l3-3 M7 9.5l-1.8 1.8a2.5 2.5 0 0 0 3.5 3.5l1.8-1.8 M13 10.5l1.8-1.8a2.5 2.5 0 0 0-3.5-3.5L9.5 7",
    terminal: "M3.5 4.5h13v11h-13z M6.5 8.5l2 1.5-2 1.5 M10 12h3.5",
  };
  export type SymbolName = keyof typeof paths;
  export const symbolNames = Object.keys(paths);
</script>

<script lang="ts">
  let {
    name,
    size = 16,
    label = undefined,
    weight = 1.6,
  }: { name: string; size?: number; label?: string; weight?: number } = $props();
</script>

<svg
  width={size}
  height={size}
  viewBox="0 0 20 20"
  fill="none"
  stroke="currentColor"
  stroke-width={weight}
  stroke-linecap="round"
  stroke-linejoin="round"
  role={label ? "img" : undefined}
  aria-label={label}
  aria-hidden={label ? undefined : "true"}
>
  <path d={paths[name] ?? ""} />
</svg>

<style>
  svg {
    flex: none;
    display: block;
  }
</style>
