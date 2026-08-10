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
pub mod sink;
/// Byte-bounded truncation that cannot panic on a character boundary.
pub(crate) mod text;

pub mod state;
pub mod telemetry;
pub mod token_manifest_policy;
