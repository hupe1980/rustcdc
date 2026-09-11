# Copilot instructions — rustcdc

## What this repository is

A Cargo workspace shipping change data capture two ways from one version:

| Crate | What |
|---|---|
| `crates/rustcdc` | The library. Published to crates.io |
| `crates/rustcdc-server` | The server binary and container image. `publish = false` |
| `crates/crash-workers` | Binaries the process-crash suites spawn; a test cannot `SIGKILL` itself |
| `crates/xtask` | `cargo xtask <task>` — the repository's gates |

Everything that decides correctness is in the library. The server adds configuration,
sinks, state backends and an operational surface on top.

Rust 1.94.1, edition 2024. Every command runs from the repository root.

## Priorities

Correctness and replay safety over convenience. Data loss, silent corruption and
uncontrolled duplication are release-critical.

- Preserve the commit-barrier ordering: the durable checkpoint never advances past an event
  the sink has not acknowledged.
- Keep delivery semantics explicit. Do not imply exactly-once where the mechanism does not
  provide it.
- Prefer deterministic behaviour and explicit policy knobs over hidden heuristics.
- **State limits next to guarantees.** A guarantee that silently does not hold is worse
  than one that is absent.

## Validation

```bash
cargo fmt --all
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test -p rustcdc --lib --all-features
cargo test -p rustcdc-server --lib --all-features
cargo xtask policy-gate            # the gate a pull request must pass
```

Also run when relevant:

- Touched the library's public API, or any published `.md`: `cargo test -p rustcdc --doc --all-features`
- Touched the library's file layout, `build.rs`, or anything it reads: `cargo package -p rustcdc --locked`
- Touched benchmarks or release evidence: `cargo xtask benchmark-gate`, `cargo xtask evidence`
- Touched latency evidence: `cargo xtask latency-gate`
- Touched dependencies: `cargo deny check`

## Standing rules

1. **Test the path the user crosses.** A unit test of a helper is not coverage of a feature.
2. **A guard that cannot fire is worse than no guard** — it reads as coverage. Verify every
   new guard against a *planted* violation before trusting it.
3. **Two implementations of one semantic must be reduced to one**, or asserted to agree.
4. **Prose is not data.** Anything compared, routed or keyed on must be a stable value, not
   a human-readable string.
5. **Name the test that pins a number.** If you cannot, write "not measured".
6. **Reproduce before repairing**, then plant the defect against the new test. A regression
   test that passes against the old code is not a regression test.

## Writing style

- Comments explain **why**, not what. Delete a comment that restates the code.
- No essays. If a comment needs three paragraphs, the code probably needs changing.
- Published docs under `site/content/docs/` are **not a changelog**. Describe current
  behaviour; do not narrate what a previous release did wrong.
- Every Rust block in `README.md` and `site/content/docs/` is compiled by
  `cargo test --doc`. Mark a block that genuinely cannot run `ignore` with a one-line reason.

## Do not

- Add a Git dependency — it makes the crate unpublishable.
- Add `unsafe` outside the two allowlisted sites (`crates/rustcdc-server/tests/architecture.rs`
  holds the list).
- Relax a safety check silently. Change the policy and its CI guard together.
- Introduce deprecated APIs or usage patterns.
