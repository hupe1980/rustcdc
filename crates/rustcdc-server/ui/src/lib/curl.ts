// Every action shows the request it makes, so the console teaches the API and nothing it does is
// unscriptable. The token is never in the text: the variable is.
export function curl(method: "GET" | "POST", path: string, body?: unknown, scope: "read" | "write" = "read"): string {
  const variable = scope === "write" ? "$RUSTCDC_ADMIN_WRITE_TOKEN" : "$RUSTCDC_ADMIN_READ_TOKEN";
  const url = `${location.origin}${path}`;
  const parts = [`curl -s${method === "POST" ? " -X POST" : ""} '${url}'`, `  -H "Authorization: Bearer ${variable}"`];
  if (body !== undefined) {
    parts.push(`  -H 'Content-Type: application/json'`, `  -d '${JSON.stringify(body)}'`);
  }
  return parts.join(" \\\n");
}
