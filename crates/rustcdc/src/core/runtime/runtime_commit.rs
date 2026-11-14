use super::*;
use crate::checkpoint::Checkpoint;

impl CdcRuntime {
    /// Commit an acknowledged batch, advancing the checkpoint barrier.
    ///
    /// Accepts either an [`AckMode`] (returned by [`EventBatch::ack_mode()`]) or a
    /// raw [`AckToken`] directly.
    ///
    /// With the default [`RuntimeOptions::checkpoint_interval`] of zero, the position is
    /// durable and confirmed to the source when this returns. With an interval, it is
    /// accepted and written within the interval, or by [`stop`](Self::stop); a crash in
    /// between replays those events, and loses none.
    ///
    /// When `mode` is [`AckMode::NotRequired`] (empty batch or disabled source) this
    /// is a safe, low-cost no-op. Callers do **not** need to guard the call:
    ///
    /// ```no_run
    /// # use rustcdc::CdcRuntime;
    /// # async fn example(runtime: &mut CdcRuntime) -> rustcdc::Result<()> {
    /// let batch = runtime.poll_event_batch().await?;
    /// // Always correct — no-op for empty batches, commits for non-empty ones.
    /// runtime.commit_ack(batch.ack_mode()).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn commit_ack(&mut self, mode: impl Into<AckMode>) -> Result<()> {
        let token = match mode.into() {
            AckMode::Required(token) => token,
            AckMode::NotRequired => return Ok(()),
        };

        let pending = self.pending_delivery.as_ref().ok_or_else(|| {
            let error = Error::CheckpointError(
                "no in-flight batch is available for acknowledgement".into(),
            );
            self.record_runtime_error("runtime.commit.missing_pending", &error);
            error
        })?;

        if pending.delivery_id != token.delivery_id {
            let error = Error::CheckpointError(
                "ack token does not match the current in-flight delivery".into(),
            );
            self.record_runtime_error("runtime.commit.token_mismatch", &error);
            return Err(error);
        }

        // A token is spent by the commit that accepts it. Without this, committing the
        // same token twice matched the delivery id both times and advanced the
        // checkpoint over the *next* N events — events the caller had never been given.
        if pending.ack_epoch != token.epoch {
            let error = Error::CheckpointError(format!(
                "this ack token has already been committed (token epoch {}, delivery is at \
                 {}). Each token may be committed once; poll again for a fresh one covering \
                 whatever remains uncommitted.",
                token.epoch, pending.ack_epoch
            ));
            self.record_runtime_error("runtime.commit.token_replay", &error);
            return Err(error);
        }

        self.commit_prefix(token.event_count).await
    }

    async fn commit_prefix(&mut self, count: usize) -> Result<()> {
        if count == 0 {
            return Ok(());
        }

        let pending_remaining = self
            .pending_delivery
            .as_ref()
            .map_or(0, |pending| pending.events.len() - pending.committed_prefix);
        if count > self.delivered_not_committed || count > pending_remaining {
            return Err(Error::CheckpointError(
                "cannot commit more events than were delivered".into(),
            ));
        }

        let snapshot_target = self.snapshot_checkpoint_target(count)?;

        let started = tokio::time::Instant::now();
        let confirmation_lsn = self.committed_confirmation_lsn(count)?;
        self.observability()
            .tracer
            .trace_checkpoint_barrier("accepting");
        self.observability()
            .tracer
            .trace_checkpoint_barrier("flushing");

        let durable_now = self.checkpoint_due(count as u64);
        let mut held = self.deferred.save.take();
        let mut cadence = Cadence {
            store: &mut *self.config.checkpoint,
            held: &mut held,
            defer: !durable_now,
        };
        // The snapshot's own progress goes first, so a mixed prefix ends at the stream
        // position the barrier writes after it.
        let snapshot_written = match (snapshot_target, self.snapshot.as_ref()) {
            (Some(target), Some(snapshot)) => snapshot.checkpoint(&mut cadence, target).await,
            _ => Ok(()),
        };
        // Acceptance and the write are one operation (see `accept_and_commit`), which
        // restores the pre-call state on failure, so retrying the identical `commit_ack` is
        // correct.
        let accepted = match snapshot_written {
            Ok(()) => self
                .commit_barrier
                .accept_and_commit(count as u64, &mut cadence)
                .await
                .map_err(|error| ("runtime.commit.checkpoint", error)),
            Err(error) => Err(("runtime.commit.snapshot_checkpoint", error)),
        };
        self.deferred.save = held;
        if let Err((context, error)) = accepted {
            self.record_runtime_error(context, &error);
            return Err(error);
        }

        // A later position supersedes an earlier one the source has not been told yet.
        let confirmation_lsn = confirmation_lsn.or(self.deferred.confirmation_lsn.take());
        self.deferred.events = self.deferred.events.saturating_add(count as u64);
        self.deferred.confirmation_lsn = confirmation_lsn;
        // When this prefix carried no position of its own, an earlier deferred one is still
        // the latest and is owed now. The events are accepted either way: a failure leaves
        // it deferred for the next poll to retry, as it would be under a longer interval.
        let post_commit_failures = if durable_now {
            match self.flush_deferred().await {
                Ok(failures) => failures,
                Err(error) => {
                    tracing::warn!(
                        target: "rustcdc::core::runtime",
                        error = %error.report(),
                        "checkpoint write failed after the commit was accepted; \
                         retrying on the next poll",
                    );
                    Vec::new()
                }
            }
        } else {
            Vec::new()
        };

        // Build trace ids only when a tracer will actually consume them.
        //
        // Otherwise the default configuration would pay for copies of the batch handed to
        // a `trace_event_end` whose default implementation has an empty body.
        let tracing_enabled = self.observability().tracer.is_enabled();
        let committed_trace_ids: Vec<String> = if tracing_enabled {
            self.pending_delivery
                .as_ref()
                .map(|pending| {
                    let start = pending.committed_prefix;
                    pending.events[start..start + count]
                        .iter()
                        .map(Self::event_trace_id)
                        .collect()
                })
                .unwrap_or_default()
        } else {
            Vec::new()
        };

        self.delivered_not_committed -= count;
        self.total_events_committed = self.total_events_committed.saturating_add(count as u64);
        let now_ms = now_millis();
        self.last_commit_at_ms = Some(now_ms);
        self.delivery_failure = None;
        if let Some(pending) = self.pending_delivery.as_mut() {
            pending.committed_prefix += count;
            pending.ack_epoch = pending.ack_epoch.saturating_add(1);
            if pending.committed_prefix >= pending.events.len() {
                self.pending_delivery = None;
            }
        }
        let latency_ms = started.elapsed().as_millis() as u64;
        self.observability()
            .metrics
            .record_checkpoint_committed(count as u64, latency_ms);
        for trace_id in &committed_trace_ids {
            self.observability()
                .tracer
                .trace_event_end(trace_id, "committed");
        }
        self.observability()
            .tracer
            .trace_checkpoint_barrier("committed");
        self.record_replication_lag_metric();

        self.apply_post_commit_policy(post_commit_failures, count)
    }

    /// Whether this commit writes the checkpoint now rather than holding it.
    fn checkpoint_due(&self, accepting: u64) -> bool {
        let options = &self.config.options;
        options.checkpoint_interval.is_zero()
            || self.deferred.last_durable_at.elapsed() >= options.checkpoint_interval
            || self.deferred.events.saturating_add(accepting) >= options.checkpoint_max_events
    }

    /// Write what earlier commits deferred, and tell the source, if the interval has run
    /// out. Called between polls, so a pipeline that goes quiet still becomes durable.
    pub(super) async fn flush_checkpoint_if_due(&mut self) -> Result<()> {
        if self.deferred.is_empty() || !self.checkpoint_due(0) {
            return Ok(());
        }
        self.flush_checkpoint().await
    }

    /// Write what earlier commits deferred, and tell the source, now.
    ///
    /// On a write failure the deferred position is kept, so the next call retries it.
    pub(super) async fn flush_checkpoint(&mut self) -> Result<()> {
        if self.deferred.is_empty() {
            return Ok(());
        }
        let failures = self.flush_deferred().await?;
        self.apply_post_commit_policy(failures, 0)
    }

    /// Write the deferred position, then confirm it to the source. Returns the
    /// confirmation failures for the caller's policy; a write failure keeps the position
    /// deferred and is the error.
    async fn flush_deferred(&mut self) -> Result<Vec<(&'static str, Error)>> {
        if let Some((offset, committed)) = self.deferred.save.take()
            && let Err(error) = self
                .config
                .checkpoint
                .save(offset.as_ref(), committed)
                .await
        {
            self.record_runtime_error("runtime.commit.checkpoint", &error);
            self.deferred.save = Some((offset, committed));
            return Err(error);
        }
        self.deferred.events = 0;
        self.deferred.last_durable_at = tokio::time::Instant::now();
        self.last_checkpoint_saved_at_ms = Some(now_millis());
        let lsn = self.deferred.confirmation_lsn.take();
        Ok(self.confirm_durable_position(lsn).await)
    }

    /// Tell the source a position that is now durable here. Never called for one that is
    /// not: the source may discard what lies before it.
    async fn confirm_durable_position(&mut self, lsn: Option<u64>) -> Vec<(&'static str, Error)> {
        let mut failures = Vec::new();
        if let Some(lsn) = lsn
            && let Some(stream) = self.stream.as_mut()
        {
            match stream.confirm_lsn(lsn).await {
                Ok(()) => {
                    // Confirmed: any earlier unconfirmed position is now superseded.
                    self.pending_confirmation_lsn = None;
                    self.unconfirmed_stall_polls = 0;
                }
                Err(error) => {
                    // Retain the LSN so the next poll can retry *before* the
                    // idempotency guard suppresses the replayed events. Without
                    // this the runtime stalls silently and forever.
                    self.pending_confirmation_lsn = Some(lsn);
                    self.record_runtime_error("runtime.commit.confirm_lsn", &error);
                    failures.push(("stream confirm_lsn", error));
                }
            }
        } else if let Some(lsn) = lsn {
            // No stream to tell (between a disconnect and the reconnect): the next poll
            // confirms it before anything is replayed.
            self.pending_confirmation_lsn = Some(lsn);
        }
        failures
    }

    fn apply_post_commit_policy(
        &self,
        post_commit_failures: Vec<(&'static str, Error)>,
        count: usize,
    ) -> Result<()> {
        if !post_commit_failures.is_empty() {
            let summary = post_commit_failures
                .into_iter()
                .map(|(phase, error)| format!("{phase}: {error}"))
                .collect::<Vec<_>>()
                .join("; ");

            match self.config.options.post_commit_source_confirm_policy {
                PostCommitSourceConfirmPolicy::Continue => {
                    tracing::warn!(
                        target: "rustcdc::core::runtime",
                        committed_count = count,
                        "runtime ack remained successful after durable checkpoint commit despite post-commit failures: {summary}",
                    );
                }
                PostCommitSourceConfirmPolicy::FailFast => {
                    let error = Error::PostCommitConfirmFailed {
                        checkpoint_safe: true,
                        detail: summary.clone(),
                    };
                    self.record_runtime_error(
                        "runtime.commit.post_commit_confirm_fail_fast",
                        &error,
                    );
                    tracing::error!(
                        target: "rustcdc::core::runtime",
                        committed_count = count,
                        "runtime ack failed by policy after durable checkpoint commit due to post-commit failures: {summary}",
                    );
                    return Err(error);
                }
            }
        }

        Ok(())
    }

    fn pending_prefix_contains_snapshot_events(&self, count: usize) -> bool {
        let Some(pending) = self.pending_delivery.as_ref() else {
            return false;
        };

        let start = pending.committed_prefix;
        pending.events[start..start + count]
            .iter()
            .any(|event| event.snapshot.is_some())
    }

    /// The committed count a bulk snapshot's own checkpoint is written at, if this prefix
    /// owes one.
    fn snapshot_checkpoint_target(&self, count: usize) -> Result<Option<u64>> {
        if !self.pending_prefix_contains_snapshot_events(count) {
            return Ok(None);
        }

        // Two kinds of snapshot reach this point, and only one needs a write here.
        //
        // A **bulk** snapshot persists its progress through connector-native state, so its
        // handle must checkpoint explicitly — and a missing handle with snapshot rows pending
        // is a genuine state error worth refusing.
        //
        // An **incremental** snapshot has no handle at all: `start()` leaves `self.snapshot`
        // as `None` because the driver *is* the stream. Its chunk cursors ride inside the
        // stream's own offset (see `buffer_and_deliver`, which uses `add_boxed_event` for
        // snapshot rows), so the commit barrier already writes them in the same atomic record
        // as the stream position. Demanding a handle for both meant the **first commit of any
        // incremental-snapshot row failed**, which made `with_incremental_snapshot`
        // unusable through `CdcRuntime` — the feature worked only when driving the
        // `StreamHandle` directly, which is what its tests did.
        if self.snapshot.is_none() {
            if self.config.incremental_snapshot.is_some() {
                return Ok(None);
            }
            let error = Error::StateError(
                "snapshot events are pending commit but snapshot handle is unavailable".into(),
            );
            self.record_runtime_error("runtime.commit.snapshot_checkpoint_missing", &error);
            return Err(error);
        }

        Ok(Some(
            self.commit_barrier
                .committed_event_count()
                .saturating_add(count as u64),
        ))
    }

    fn committed_confirmation_lsn(&self, count: usize) -> Result<Option<u64>> {
        let pending = match self.pending_delivery.as_ref() {
            Some(pending) => pending,
            None => return Ok(None),
        };

        let remaining = pending.events.len() - pending.committed_prefix;
        if count == 0 || count > remaining {
            return Ok(None);
        }

        let last_committed = &pending.events[pending.committed_prefix + count - 1];

        #[cfg(feature = "postgres")]
        {
            // Snapshot offsets use cursor syntax (e.g. "public.table:(1,46)") and should not
            // drive replication-slot confirmation.
            if last_committed.snapshot.is_some() {
                return Ok(None);
            }

            let is_postgres_event = last_committed
                .source
                .source_name
                .eq_ignore_ascii_case("postgres");

            if !is_postgres_event
                && !matches!(&self.config.source, RuntimeSourceConfig::Postgres(_))
            {
                return Ok(None);
            }

            if !last_committed.source.offset.contains('/') {
                return Ok(None);
            }

            // Confirm the transaction's end position, not the change's own LSN. Leaving the
            // slot's `confirmed_flush_lsn` inside a transaction makes the server re-send
            // that whole transaction — see `StreamHandle::resume_offset_for`.
            Ok(Some(parse_postgres_lsn(
                &self.resume_offset_for(last_committed),
            )?))
        }

        #[cfg(not(feature = "postgres"))]
        {
            let _ = last_committed;
            Ok(None)
        }
    }
}

/// Routes the commit path's checkpoint writes: to the store when the cadence is due, into
/// memory when it is not.
struct Cadence<'a> {
    store: &'a mut dyn Checkpoint,
    held: &'a mut Option<(Box<dyn Offset>, u64)>,
    defer: bool,
}

#[async_trait::async_trait]
impl Checkpoint for Cadence<'_> {
    async fn save(&mut self, offset: &dyn Offset, committed_event_count: u64) -> Result<()> {
        if self.defer {
            *self.held = Some((offset.clone_box(), committed_event_count));
            return Ok(());
        }
        self.store.save(offset, committed_event_count).await?;
        // A later position, now durable, supersedes whatever was held.
        *self.held = None;
        Ok(())
    }

    async fn load(&self) -> Result<Option<Box<dyn Offset>>> {
        self.store.load().await
    }

    async fn get_committed_count(&self) -> Result<u64> {
        self.store.get_committed_count().await
    }
}
