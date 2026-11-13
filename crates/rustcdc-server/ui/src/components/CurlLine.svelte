<script lang="ts">
  // The request an action makes, ready to paste. The token is a variable, never the value.
  import Symbol from "./Symbol.svelte";
  import { copy } from "../lib/clipboard";
  let { command }: { command: string } = $props();
  let copied = $state(false);
  async function take() {
    copied = await copy(command);
    setTimeout(() => (copied = false), 1600);
  }
</script>

<div class="curl">
  <pre class="mono">{command}</pre>
  <button class="btn plain" onclick={take} aria-label="Copy the curl command">
    <Symbol name={copied ? "check" : "copy"} size={14} />{copied ? "Copied" : "Copy"}
  </button>
</div>

<style>
  .curl {
    display: flex;
    align-items: flex-start;
    gap: var(--s2);
    background: var(--surface-2);
    border-radius: var(--r);
    padding: var(--s2) var(--s2) var(--s2) var(--s3);
  }
  pre {
    flex: 1;
    margin: 0;
    white-space: pre-wrap;
    word-break: break-all;
    color: var(--text-2);
  }
</style>
