// The admin tokens live in this tab's session storage only: they survive a reload, are gone when
// the tab closes, and never travel in a URL or a cookie. A token the server rejects is forgotten.
export type Scope = "read" | "write";
const key = (scope: Scope) => `rustcdc.${scope}-token`;

export function token(scope: Scope): string | null {
  try {
    return sessionStorage.getItem(key(scope));
  } catch {
    return null;
  }
}

export function remember(scope: Scope, value: string): void {
  sessionStorage.setItem(key(scope), value.trim());
}

export function forget(scope?: Scope): void {
  try {
    for (const s of scope ? [scope] : (["read", "write"] as const)) sessionStorage.removeItem(key(s));
  } catch {
    /* storage unavailable: nothing to forget */
  }
}
