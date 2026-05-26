use super::*;

impl<C, H> CdcRuntime<C, H>
where
    C: crate::checkpoint::Checkpoint + Send + Sync + 'static,
    H: SchemaHistory + Send + Sync + 'static,
{
    /// Commit an acknowledged batch prefix represented by an opaque token.
    pub async fn commit_ack(&mut self, token: AckToken) -> Result<()> {
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

        self.persist_snapshot_checkpoint_before_commit(count)
            .await?;

        let started = tokio::time::Instant::now();
        let confirmation_lsn = self.committed_confirmation_lsn(count)?;
        self.observability()
            .tracer
            .trace_checkpoint_barrier("accepting");
        self.commit_barrier
            .notify_consumer_accepted(count as u64)
            .inspect_err(|error| self.record_runtime_error("runtime.commit.accept", error))?;
        self.observability()
            .tracer
            .trace_checkpoint_barrier("flushing");
        self.commit_barrier
            .commit(&mut self.config.checkpoint)
            .await
            .inspect_err(|error| self.record_runtime_error("runtime.commit.checkpoint", error))?;

        // Durability is guaranteed once the commit barrier flush succeeds. Post-commit
        // source confirmation failures are handled by policy.
        let mut post_commit_failures = Vec::new();

        if let Some(lsn) = confirmation_lsn {
            if let Some(stream) = self.stream.as_mut() {
                if let Err(error) = stream.confirm_lsn(lsn).await {
                    self.record_runtime_error("runtime.commit.confirm_lsn", &error);
                    post_commit_failures.push(("stream confirm_lsn", error));
                }
            }
        }

        let committed_events = self
            .pending_delivery
            .as_ref()
            .map(|pending| {
                let start = pending.committed_prefix;
                pending.events[start..start + count].to_vec()
            })
            .unwrap_or_default();

        self.delivered_not_committed -= count;
        self.total_events_committed = self.total_events_committed.saturating_add(count as u64);
        let now_ms = now_millis();
        self.last_commit_at_ms = Some(now_ms);
        self.last_checkpoint_saved_at_ms = Some(now_ms);
        if let Some(pending) = self.pending_delivery.as_mut() {
            pending.committed_prefix += count;
            if pending.committed_prefix >= pending.events.len() {
                self.pending_delivery = None;
            }
        }
        let latency_ms = started.elapsed().as_millis() as u64;
        self.observability()
            .metrics
            .record_checkpoint_committed(count as u64, latency_ms);
        for event in &committed_events {
            self.observability()
                .tracer
                .trace_event_end(&Self::event_trace_id(event), "committed");
        }
        self.observability()
            .tracer
            .trace_checkpoint_barrier("committed");
        self.record_replication_lag_metric();

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
                    let error = Error::SourceError(format!(
                        "post-commit source confirmation failed after durable checkpoint commit: {summary}"
                    ));
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

    async fn persist_snapshot_checkpoint_before_commit(&mut self, count: usize) -> Result<()> {
        if !self.pending_prefix_contains_snapshot_events(count) {
            return Ok(());
        }

        let Some(snapshot) = self.snapshot.as_ref() else {
            let error = Error::StateError(
                "snapshot events are pending commit but snapshot handle is unavailable".into(),
            );
            self.record_runtime_error("runtime.commit.snapshot_checkpoint_missing", &error);
            return Err(error);
        };

        let target_committed_count = self
            .commit_barrier
            .committed_event_count()
            .saturating_add(count as u64);

        snapshot
            .checkpoint(&mut self.config.checkpoint, target_committed_count)
            .await
            .inspect_err(|error| {
                self.record_runtime_error("runtime.commit.snapshot_checkpoint", error)
            })
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

            Ok(Some(parse_postgres_lsn(&last_committed.source.offset)?))
        }

        #[cfg(not(feature = "postgres"))]
        {
            let _ = last_committed;
            Ok(None)
        }
    }
}
