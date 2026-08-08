//! A minimal PostgreSQL logical replication client.
//!
//! Enough of the v3 protocol to run `START_REPLICATION ... LOGICAL` and nothing more:
//! startup, TLS upgrade, password authentication, the `CopyBoth` data loop, and Standby
//! Status Update feedback. Ordinary SQL stays with `tokio-postgres`.
//!
//! # Why this is hand-written
//!
//! `tokio-postgres` exposes no `CopyBoth` or replication-mode API, so the streaming
//! protocol is unreachable through it. The alternative was
//! `pg_logical_slot_peek_binary_changes` over an ordinary connection, which is
//! non-consuming: PostgreSQL begins decoding at the slot's `restart_lsn` and only *emits*
//! past `confirmed_flush_lsn`, so any long-running transaction on the source pins
//! `restart_lsn` and every poll re-reads the WAL gap between the two. The streaming
//! protocol pays that once per connection.
//!
//! A published crate does implement this, but it declares `rustls` without
//! `default-features = false`, which force-enables rustls's `aws-lc-rs` provider across the
//! whole dependency graph — a second crypto backend beside the `ring` one this crate
//! standardises on, and not something a dependent can opt out of, because Cargo unifies
//! features. The wire protocol here is well-specified and stable; a second TLS stack is not
//! worth avoiding it.
//!
//! # Scope
//!
//! Deliberately absent: query execution, prepared statements, connection pooling, physical
//! replication, and SCRAM channel binding (see [`auth`]). This is a transport, not a client.

mod auth;
mod framing;
mod stream;
#[cfg(test)]
mod tests;

pub(in crate::source::postgres) use stream::{ReplicationParams, ReplicationStream, WalMessage};

/// PostgreSQL's epoch for protocol timestamps: 2000-01-01 00:00:00 UTC, as seconds since
/// the Unix epoch.
///
/// Every timestamp on the replication wire is microseconds from this point, not from 1970.
/// Reading one as a Unix timestamp puts it 30 years in the past, which is exactly wrong
/// enough to look like a plausible lag figure.
pub(super) const POSTGRES_EPOCH_UNIX_SECONDS: i64 = 946_684_800;

/// Current time as a PostgreSQL protocol timestamp.
fn now_pg_timestamp() -> i64 {
    let unix_micros = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_micros() as i64)
        .unwrap_or_default();
    unix_micros - POSTGRES_EPOCH_UNIX_SECONDS * 1_000_000
}

#[cfg(test)]
mod timestamp_tests {
    use super::*;

    #[test]
    fn a_generated_timestamp_round_trips_back_to_roughly_now() {
        let unix_now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock is after the unix epoch")
            .as_millis() as i64;
        let round_tripped =
            POSTGRES_EPOCH_UNIX_SECONDS * 1_000 + now_pg_timestamp() / 1_000;
        assert!(
            (round_tripped - unix_now_ms).abs() < 5_000,
            "a timestamp written for the server and read back must agree with the clock: \
             {round_tripped} vs {unix_now_ms}"
        );
    }
}
