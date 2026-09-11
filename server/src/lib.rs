// `unsafe` is denied crate-wide by `[workspace.lints]`, which this package opts into.
//
// It is `deny` rather than `forbid` for exactly one reason: `test_env::write_env` calls
// `std::env::set_var`, which Rust 2024 makes `unsafe`, and several tests need a variable in
// the process environment because `SecretString`'s only deferred form is `{ env = "VAR" }`
// and krafka resolves the MSK IAM chain from `AWS_*`. That module cannot be `#[cfg(test)]`
// — `tests/config_roundtrip.rs` is a separate crate and would have to duplicate the
// `unsafe` — so it is compiled into the library and denied-with-one-exemption instead.
//
// `src/main.rs` carries `forbid`, which cannot be overridden at all: the **binary** this
// project ships contains no `unsafe`, and that is the claim the README makes.
// `tests/architecture.rs::unsafe_code_appears_only_where_the_allowlist_says_it_may` holds
// the exemption list to exactly one entry, in both directions.

//! # Connector features
//!
//! Source connectors are opt-in cargo features (`postgres`, `mysql`, `sqlserver`), so a
//! deployment links only the drivers it uses — the point being that a PostgreSQL-only
//! build stops shipping `tiberius`, and with it a second TLS stack carrying four
//! suppressed RUSTSEC advisories.
//!
//! **At least one is required.** A binary with no source connector cannot capture
//! anything: every config would be rejected by `reject_uncompiled_source_driver`, and the
//! failure would arrive at startup in production rather than at build time. Refusing to
//! compile is the earlier and louder of the two.
// The default trait-solver depth of 128 is not enough to prove that the admin Kafka
// signal-ingress worker's `tokio::spawn` future is `Send`. The obligation unwinds through
// krafka 0.22's `Consumer::poll` — task-local lock tracking wrapping a `poll_fn` over a
// `JoinAll` of per-broker fetches — and overflows before it lands.
//
// Nightly's `recursion_depth_exceeding_limit` lint reports that overflow, and CI's
// `RUSTFLAGS: -D warnings` makes it fatal in the fuzz job, the only job on nightly. 256 is
// the compiler's own suggestion, and it *proves* the bound rather than muting the report:
// `#[allow]` would leave the future unproven and the failure waiting for the day the lint
// stops being future-compat (rust-lang/rust#159228).
#![recursion_limit = "256"]

#[cfg(not(any(feature = "postgres", feature = "mysql", feature = "sqlserver")))]
compile_error!(
    "rustcdc-server needs at least one source connector, and this build has none. \
     Enable one of `postgres` (the default), `mysql` (MySQL + MariaDB) or `sqlserver` — \
     e.g. `cargo build --no-default-features --features mysql`, or `--all-features` for \
     every connector. A binary with no connector would reject every configuration at \
     startup."
);

pub mod admin;
pub mod cli;
pub mod codec;
pub mod commands;
pub mod config;
pub mod dlq;
pub mod error;
/// Secret redaction for the `/status` config snapshot.
///
/// Public so the randomised property suite in `tests/fuzz_properties.rs` and the
/// `fuzz/` targets can reach it. The crate is `publish = false`, so this widens no
/// external API — and the alternative, a test-only re-export, hides the one function
/// whose correctness a reader most needs to be able to find.
pub mod redaction;

pub mod pipeline;
/// The capture pipeline's engine, extracted from `commands/`.
pub mod runtime;
pub mod sink;
/// Byte-bounded truncation that cannot panic on a character boundary.
pub(crate) mod text;

pub mod state;
pub mod telemetry;
/// Process-environment mutation for tests, and the crate's only `unsafe`.
///
/// `pub` because `tests/config_roundtrip.rs` is a separate crate and cannot reach a
/// `#[cfg(test)]` item; duplicating the `unsafe` there is exactly what this prevents. The
/// crate is `publish = false`, so this widens no external API.
pub mod test_env;
pub mod token_manifest_policy;
