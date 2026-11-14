//! [`PgOutputMessageProvider`] over the streaming replication protocol.
//!
//! This is the default WAL transport. It supplies raw pgoutput bytes exactly as the
//! SQL-peek transport does, so everything downstream — the decoder, event construction,
//! table filtering, the snapshot handoff, checkpointing — is shared and untouched.
//!
//! Because rustcdc owns the wire client ([`super::wire`]), the pgoutput payload arrives
//! byte-for-byte as the server produced it. The one exception is a transaction the server
//! streams under v2: [`StreamAssembler`] holds it and hands it on at its commit as the v1
//! bytes it is made of, so the decoder still cannot disagree between the two transports.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio_postgres::Client;

use crate::core::Result;

use super::decoder::{PgOutputMessageProvider, PgOutputXLogData, PollOutcome};
use super::stream_assembly::{
    Assembled, OpenStreamSummary, StreamAssembler, StreamBufferTotals, probe_spill_directory,
};
use super::wire::{OpenError, PgOutputProtocol, ReplicationParams, ReplicationStream, WalMessage};

/// A committed streamed transaction being handed on, a poll's worth at a time.
type Replay = Box<dyn Iterator<Item = Result<PgOutputXLogData>> + Send + Sync>;

enum Ready {
    Message(PgOutputXLogData),
    Replay(Replay),
}

/// Reads the WAL stream over `START_REPLICATION ... LOGICAL`.
pub(super) struct StreamingPgOutputProvider {
    stream: ReplicationStream,
    slot_name: String,
    /// Most recent server-side end-of-WAL, learned from keepalives and data messages.
    ///
    /// This is what makes lag reporting free: the server volunteers its write position on
    /// every keepalive, so there is no `pg_current_wal_lsn()` query to run.
    server_wal_end: u64,
    /// The connection's ordinary SQL client, shared rather than opened anew. The replication
    /// socket cannot run a catalogue query, and the slot's `wal_status` is only in one.
    state_client: Arc<Client>,
    /// Present when v2 was negotiated.
    assembler: Option<StreamAssembler>,
    /// What the assembler released, not yet handed on.
    ready: VecDeque<Ready>,
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
        state_client: Arc<Client>,
    ) -> std::result::Result<Self, OpenError> {
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
            capture_logical_messages: config.capture_logical_messages,
            stream_in_progress: config.stream_in_progress_transactions,
        })
        .await?;

        let protocol = stream.protocol();
        let assembler = if protocol == PgOutputProtocol::StreamingV2 {
            let directory = config.stream_spill_directory();
            probe_spill_directory(&directory)?;
            Some(StreamAssembler::new(
                config.stream_buffer_memory_bytes,
                directory,
            ))
        } else {
            None
        };
        match protocol {
            PgOutputProtocol::StreamingV2 => tracing::info!(
                target: "rustcdc::source::postgres",
                slot = %config.replication_slot_name,
                "pgoutput v2: large transactions stream while they run; rows are released at commit",
            ),
            PgOutputProtocol::V1Disabled => tracing::info!(
                target: "rustcdc::source::postgres",
                slot = %config.replication_slot_name,
                "pgoutput v1: stream_in_progress_transactions = false",
            ),
            PgOutputProtocol::V1Server(major) => tracing::info!(
                target: "rustcdc::source::postgres",
                slot = %config.replication_slot_name,
                server_major = ?major,
                "pgoutput v1: streaming in-progress transactions needs PostgreSQL 14 or later",
            ),
        }

        Ok(Self {
            stream,
            slot_name: config.replication_slot_name.clone(),
            server_wal_end: start_lsn,
            state_client,
            assembler,
            ready: VecDeque::new(),
        })
    }

    fn observe_wal_end(&mut self, wal_end: u64) {
        // `wal_end` is zero on a mid-transaction record, and must never drag the high-water
        // mark backwards — that would make lag reporting oscillate and let idle-advance
        // confirm a position behind one already confirmed.
        self.server_wal_end = self.server_wal_end.max(wal_end);
    }

    /// Up to `max` released messages, expanding a committed stream lazily.
    fn take_ready(&mut self, max: usize) -> Result<Vec<PgOutputXLogData>> {
        let mut out = Vec::new();
        while out.len() < max {
            match self.ready.front_mut() {
                None => break,
                Some(Ready::Message(_)) => {
                    let Some(Ready::Message(message)) = self.ready.pop_front() else {
                        unreachable!("matched above")
                    };
                    out.push(message);
                }
                Some(Ready::Replay(replay)) => match replay.next() {
                    Some(message) => out.push(message?),
                    None => {
                        self.ready.pop_front();
                    }
                },
            }
        }
        Ok(out)
    }

    /// Read what the socket has within the budget: block for the first record, then take
    /// only what is already buffered.
    async fn read_wal(
        &mut self,
        max_messages: usize,
        deadline: tokio::time::Instant,
    ) -> Result<Vec<PgOutputXLogData>> {
        let mut messages = Vec::new();
        while messages.len() < max_messages.max(1) {
            // Waiting the full budget once data has arrived would make every record wait for
            // the last one, turning a push transport back into a polling one.
            let remaining = if messages.is_empty() {
                deadline.saturating_duration_since(tokio::time::Instant::now())
            } else {
                Duration::ZERO
            };
            if remaining.is_zero() && messages.is_empty() {
                break;
            }

            match self.stream.recv(remaining).await? {
                // Nothing more without blocking. On an empty batch the budget is simply
                // spent — unlike a peek timeout this is never backlog pressure, so it cannot
                // be mistaken for an idle slot.
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
                    // A keepalive is not data: returning on one would make every heartbeat
                    // look like a completed poll. Keep waiting out the budget.
                }
            }
        }
        Ok(messages)
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
        let max = max_messages.max(1);
        loop {
            let ready = self.take_ready(max)?;
            if !ready.is_empty() {
                return Ok(PollOutcome::Data(ready));
            }
            let read = self.read_wal(max, deadline).await?;
            let Some(assembler) = self.assembler.as_mut() else {
                return Ok(PollOutcome::Data(read));
            };
            let budget_spent = read.is_empty();
            for item in assembler.feed(read)? {
                self.ready.push_back(match item {
                    Assembled::Message(message) => Ready::Message(message),
                    Assembled::Committed(stream) => {
                        let begin = stream.begin();
                        let commit = stream.commit();
                        Ready::Replay(Box::new(
                            std::iter::once(Ok(begin))
                                .chain(stream.messages()?)
                                .chain(std::iter::once(Ok(commit))),
                        ))
                    }
                });
            }
            // A poll that read only stream blocks has nothing to hand on yet; keep reading
            // until the budget is spent rather than report an empty poll, which the caller
            // would wait out as an idle one.
            if budget_spent || tokio::time::Instant::now() >= deadline {
                return Ok(PollOutcome::Data(self.take_ready(max)?));
            }
        }
    }

    fn waits_for_data(&self) -> bool {
        // Blocks on the socket, so the caller must pass its real budget. See the trait docs.
        true
    }

    async fn confirm_lsn(&mut self, lsn: u64) -> Result<()> {
        // Reported immediately on advance rather than deferred to the status interval:
        // `confirmed_flush_lsn` is what releases WAL, and a process exiting right after a
        // commit would otherwise never report it. The cost is a 34-byte write per committed
        // batch — the peek transport spent a whole `pg_replication_slot_advance` query here.
        let advanced = lsn > self.stream.applied_lsn();
        self.stream.set_applied_lsn(lsn);
        if advanced {
            self.stream.send_status_update(false).await?;
        }
        Ok(())
    }

    async fn measure_slot_lag(&mut self) -> Result<Option<u64>> {
        // Free: the server's write position arrives on every keepalive and XLogData
        // header, so lag is the gap to what we have confirmed. No query, no slot mutation,
        // and therefore safe to sample while the pipeline is behind.
        Ok(Some(
            self.server_wal_end
                .saturating_sub(self.stream.applied_lsn()),
        ))
    }

    async fn sample_slot_state(&mut self) -> Result<Option<super::parser::SlotState>> {
        super::query::query_slot_state(&self.state_client, &self.slot_name).await
    }

    fn streamed_transactions(&self) -> Option<crate::core::StreamedTransactions> {
        let assembler = self.assembler.as_ref()?;
        let StreamBufferTotals {
            open,
            memory_bytes,
            spilled_bytes,
        } = assembler.totals();
        let largest = assembler.open_transaction().map(
            |OpenStreamSummary {
                 xid,
                 changes,
                 bytes,
                 spilled,
                 started_at,
             }| {
                let started_at_ms = started_at
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_or(0, |since| since.as_millis() as u64);
                crate::core::OpenTransaction::new(
                    u64::from(xid),
                    changes,
                    bytes,
                    spilled,
                    started_at_ms,
                )
            },
        );
        Some(crate::core::StreamedTransactions::new(
            open,
            memory_bytes,
            spilled_bytes,
            largest,
        ))
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
