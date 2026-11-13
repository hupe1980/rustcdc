// Small formatting helpers, one place for each.

/** "just now", "12 s ago", "4 min ago", "3 h ago". */
export function ago(seconds: number): string {
  if (seconds < 2) return "just now";
  if (seconds < 60) return `${Math.round(seconds)} s ago`;
  return `${duration(seconds)} ago`;
}

/** "45 s", "4 min", "3 h 20 min", "2 days". */
export function duration(seconds: number): string {
  const s = Math.max(0, Math.round(seconds));
  if (s < 60) return `${s} s`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m} min`;
  const h = Math.floor(m / 60);
  if (h < 48) return m % 60 ? `${h} h ${m % 60} min` : `${h} h`;
  return `${Math.floor(h / 24)} days`;
}

/** 1240 → "1 240", 1.5 → "1.5". */
export function count(n: number): string {
  return n.toLocaleString(undefined, { maximumFractionDigits: n < 10 ? 1 : 0 });
}

export function secondsSince(iso: string | null | undefined, now = Date.now()): number | null {
  if (!iso) return null;
  const t = Date.parse(iso);
  return Number.isNaN(t) ? null : (now - t) / 1000;
}
