<script lang="ts">
  // What a row looks like: paste the id a Kafka header or a CloudEvent `dataschema` carries.
  import Symbol from "../components/Symbol.svelte";
  import CurlLine from "../components/CurlLine.svelte";
  import { ApiError, get, type RowShape } from "../lib/api";
  import { curl } from "../lib/curl";

  let { id = "", ongo }: { id?: string; ongo: (id: string) => void } = $props();

  let typed = $state("");
  let shape = $state<RowShape | null>(null);
  let error = $state("");
  let loading = $state(false);

  // `urn:rustcdc:row-shape:<id>` and a bare id both work.
  const clean = (value: string) => value.trim().replace(/^urn:rustcdc:row-shape:/, "");

  $effect(() => {
    typed = id;
    shape = null;
    error = "";
    if (!id) return;
    loading = true;
    get<RowShape>(`/schemas/${encodeURIComponent(id)}`)
      .then((s) => (shape = s))
      .catch((e: ApiError) => (error = e.status === 404 ? "This server has not delivered a row with that shape since it started." : e.message))
      .finally(() => (loading = false));
  });

  function submit(e: SubmitEvent) {
    e.preventDefault();
    if (clean(typed)) ongo(clean(typed));
  }
</script>

<div class="shapes">
  <form class="find card" onsubmit={submit}>
    <label for="shape-id">Row shape id</label>
    <div class="row">
      <input id="shape-id" type="text" bind:value={typed} placeholder="From the __rustcdc.row_shape header or dataschema" spellcheck="false" />
      <button class="btn primary" type="submit" disabled={!clean(typed)}><Symbol name="search" size={13} />Look up</button>
    </div>
  </form>

  {#if loading}
    <span class="skeleton" style="width: 40%"></span>
  {:else if error}
    <p class="error" role="alert"><Symbol name="warning" size={14} />{error}</p>
  {:else if shape}
    <section class="card">
      <h2 class="title"><span class="mono">{shape.schema}.{shape.table}</span></h2>
      {#if shape.columns}
        <table>
          <caption class="sr-only">Columns of {shape.schema}.{shape.table}</caption>
          <thead><tr><th scope="col">Column</th><th scope="col">Declared type</th><th scope="col">Null</th><th scope="col">Key</th></tr></thead>
          <tbody>
            {#each shape.columns as c (c.name)}
              <tr>
                <td class="mono">{c.name}</td>
                <td class="mono">{c.data_type}</td>
                <td>{c.nullable ? "nullable" : "not null"}</td>
                <td>{#if shape.primary_key?.includes(c.name)}<span class="state healthy"><Symbol name="key" size={11} />key</span>{/if}</td>
              </tr>
            {/each}
          </tbody>
        </table>
      {:else}
        <p class="muted">A compact shape: the id names the table, the columns were not kept.</p>
      {/if}
      <p class="muted mono id">{shape.id}</p>
      <CurlLine command={curl("GET", `/schemas/${shape.id}`)} />
    </section>
  {:else}
    <p class="muted hint">Every row rustcdc delivers names its shape. Paste an id to see the columns, their declared types and the key.</p>
  {/if}
</div>

<style>
  .shapes {
    height: 100%;
    overflow: auto;
    padding: var(--s5);
    display: grid;
    gap: var(--s4);
    align-content: start;
    max-width: 960px;
  }
  .find label { display: block; font-weight: 600; margin-bottom: 6px; }
  .row { display: flex; gap: var(--s2); }
  .row input { flex: 1; font-family: var(--mono); }
  .title { margin: 0 0 var(--s3); font: 600 15px/1.3 var(--display); }
  table { width: 100%; border-collapse: collapse; margin-bottom: var(--s3); }
  th { text-align: left; font-size: 12px; color: var(--muted); font-weight: 500; padding: 6px 8px; }
  td { padding: 6px 8px; border-top: 0.5px solid var(--separator); }
  .id { word-break: break-all; }
  .error { display: flex; gap: 6px; align-items: center; color: var(--stalled); }
  .hint { max-width: 60ch; }
</style>
