# Copilot Instructions for rustcdc

## Project intent
- This repository is a correctness-first Rust CDC library.
- Prioritize correctness and replay safety over convenience.
- Treat data loss, silent corruption, and uncontrolled duplication risks as release-critical concerns.

## Engineering priorities
- Preserve runtime commit/checkpoint ordering invariants.
- Keep at-least-once semantics explicit; do not imply exactly-once guarantees.
- Prefer deterministic behavior and explicit policy knobs over hidden heuristics.
- Avoid introducing deprecated APIs or deprecated usage patterns.

## Required validation for meaningful changes
Run these locally when touching related areas:
- General quality:
  - `cargo check --all-targets --all-features`
  - `cargo clippy --all-targets --all-features -- -D warnings`
  - `bash scripts/ci-policy-gate.sh`
- Policy/contract gates:
  - `bash scripts/ci-policy-gate.sh`
- If touching benchmark/release evidence logic:
  - `bash scripts/ci-benchmark-gate.sh`
  - `bash scripts/run_full_integration_matrix_evidence.sh`
- If touching latency evidence logic:
  - `bash scripts/ci-latency-gate.sh`

## CI policy invariants
- Keep `scripts/ci-benchmark-gate.sh` and `scripts/run_full_integration_matrix_evidence.sh` as the release evidence execution path in `ci.yml`.
- Keep `BENCHMARK_ENFORCE_RELEASE_POLICY: "1"` in release workflows.
- Keep policy-gate, reliability-core, and latency-core lanes present in default CI.
- Keep core connector integration matrix jobs present on push/pull_request unless intentionally re-governed.

## Code change guidance
- Prefer small, auditable changes with direct evidence.
- Add or update tests when changing behavior.
- Do not relax safety checks silently; if policies are changed, update docs and CI guards together.
- Keep scripts portable across macOS/Linux shell environments.

## Documentation guidance
- Keep docs aligned with implementation and CI behavior.
- For audits and findings, prefer current-state reporting over historical timelines unless explicitly requested.
- Include clear release conditions and evidence anchors when documenting risk decisions.
