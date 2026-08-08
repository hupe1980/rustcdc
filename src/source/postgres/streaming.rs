//! [`PgOutputMessageProvider`] over the streaming replication protocol.
//!
//! This is the default WAL transport. It supplies raw pgoutput bytes exactly as the
//! SQL-peek transport does, so everything downstream — the decoder, event construction,
//! table filtering, the snapshot handoff, checkpointing — is shared and untouched.
//!
//! Because rustcdc owns the wire client ([`super::wire`]), the pgoutput payload arrives
//! byte-for-byte as the server produced it, including the `Begin` and `Commit` messages.
//! Nothing is intercepted, re-encoded, or reconstructed, so the decoder cannot disagree
//! between the two transports.

use std::time::Duration;

use async_trait::async_trait;

use crate::core::Result;

use super::decoder::{PgOutputMessageProvider, PgOutputXLogData, PollOutcome};
use super::wire::{ReplicationParams, ReplicationStream, WalMessage};

/// Reads the WAL stream over `START_REPLICATION ... LOGICAL`.
pub(super) struct StreamingPgOutputProvider {
    stream: ReplicationStream,
    slot_name: String,
    /// Most recent server-side end-of-WAL, learned from keepalives and data messages.
    ///
    /// This is what makes lag reporting free: the server volunteers its write position on
    /// every keepalive, so there is no `pg_current_wal_lsn()` query to run.
    server_wal_end: u64,
}

impl StreamingPgOutputProvider {
    /// Open a replication stream, resuming from `start_lsn`.
    ///
    /// A `start_lsn` of zero asks the server to resume from the slot's own
    /// `confirmed_flush_lsn`, which is correct for a stream with no checkpoint: the slot,
    /// not the connector, is the authority on where an unresumed stream begins.
    pub(super) async fn connect(
        config: &super::PostgresSourceConfig,
        start_lsn: u64,
    ) -> Result<Self> {
        // `resolve`, not `expose_secret`: a deferred secret (provider or callback) must be
        // fetched rather than rejected, because that is how AWS IAM database auth supplies a
        // short-lived token. This runs on every reconnect, so each new replication
        // connection gets a freshly minted one.
        let password = config.password.resolve()?;

        let stream = ReplicationStream::connect(ReplicationParams {
            host: &config.host,
            port: config.port,
            user: &config.user,
            password: &password,
            database: &config.database,
            slot_name: &config.replication_slot_name,
            publication_name: &config.publication_name,
            transport: &config.transport,
            start_lsn,
            connect_timeout: Duration::from_secs(config.conn_timeout_secs),
        })
        .await?;

        Ok(Self {
            stream,
            slot_name: config.replication_slot_name.clone(),
            server_wal_end: start_lsn,
        })
    }

    fn observe_wal_end(&mut self, wal_end: u64) {
        // `wal_end` is zero on a mid-transaction record, and must never drag the high-water
        // mark backwards — that would make lag reporting oscillate and let idle-advance
        // confirm a position behind one already confirmed.
        self.server_wal_end = self.server_wal_end.max(wal_end);
    }
}

#[async_trait]
impl PgOutputMessageProvider for StreamingPgOutputProvider {
    async fn poll_xlog_data(
        &mut self,
        max_messages: usize,
        poll_timeout: Duration,
    ) -> Result<PollOutcome> {
        let deadline = tokio::time::Instant::now() + poll_timeout;
        let mut messages = Vec::new();

        while messages.len() < max_messages.max(1) {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                break;
            }

            match self.stream.recv(remaining).await? {
                // Budget spent. Unlike the SQL-peek transport this is never backlog
                // pressure and carries no risk of being read as an idle slot: the stream is
                // intact, and whatever already arrived is returned below.
                None => break,
                Some(WalMessage::XLogData {
                    wal_start,
                    wal_end,
                    data,
                }) => {
                    self.observe_wal_end(wal_end);
                    messages.push(PgOutputXLogData {
                        lsn: wal_start,
                        data,
                    });
                }
                Some(WalMessage::Keepalive { wal_end }) => {
                    self.observe_wal_end(wal_end);
                    // A keepalive is not data. Returning here on an empty batch would make
                    // a busy-but-quiet server look like a completed poll on every heartbeat;
                    // the loop simply continues until the budget runs out.
                }
            }
        }

        Ok(PollOutcome::Data(messages))
    }

    async fn confirm_lsn(&mut self, lsn: u64) -> Result<()> {
        // Reported to the server immediately when it actually advances, rather than left
        // for the next status interval. Two reasons, both operational:
        //
        // * The slot's `confirmed_flush_lsn` is what releases WAL. Deferring the report by
        //   up to a status interval defers WAL release by the same amount, which is the one
        //   thing a replication slot must not sit on.
        // * A process that exits right after a commit would otherwise never report it, and
        //   the next start would re-read that WAL. Correct under at-least-once, but
        //   avoidable, and it removes any need for a shutdown hook on the hot path.
        //
        // A Standby Status Update is a 34-byte write on an open socket, sent once per
        // committed batch rather than per event. The SQL-peek transport spent a full
        // `pg_replication_slot_advance` query here.
        let advanced = lsn > self.stream.applied_lsn();
        self.stream.set_applied_lsn(lsn);
        if advanced {
            self.stream.send_status_update(false).await?;
        }
        Ok(())
    }

    async fn idle_advance(&mut self) -> Result<u64> {
        // Called only when nothing has been delivered for the idle interval, so there is no
        // unacknowledged work and the server's own write position is safe to confirm.
        // Without this a slot on a quiet database pins WAL forever.
        let applied = self.stream.applied_lsn();
        let lag = self.server_wal_end.saturating_sub(applied);

        if self.server_wal_end > applied {
            self.stream.set_applied_lsn(self.server_wal_end);
            // Sent immediately rather than left for the next interval: the point of an idle
            // advance is to release WAL now, and on a quiet stream there may be no further
            // traffic to piggyback on for another status period.
            self.stream.send_status_update(false).await?;
            tracing::debug!(
                target: "rustcdc::source::postgres",
                slot = %self.slot_name,
                lsn = self.server_wal_end,
                lag_bytes = lag,
                "postgres replication slot advanced during idle period",
            );
        }

        Ok(lag)
    }
}
