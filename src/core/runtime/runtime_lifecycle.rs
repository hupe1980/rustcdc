use super::*;

impl CdcRuntime {
    /// Start the runtime and initialize source handles.
    pub async fn start(&mut self) -> Result<()> {
        match self.state {
            RuntimeState::Idle | RuntimeState::Stopped => {}
            RuntimeState::Running => {
                let error = Error::StateError("runtime already started".into());
                self.record_runtime_error("runtime.start.state", &error);
                return Err(error);
            }
            RuntimeState::Stopping => {
                let error = Error::StateError("runtime is currently stopping".into());
                self.record_runtime_error("runtime.start.state", &error);
                return Err(error);
            }
        }

        if self.config.options.schema_history_retention.is_none() {
            // Warn rather than refuse to start. Schema history now grows only in
            // response to actual DDL on captured tables, which is rare in most
            // deployments — refusing to start over an unbounded-growth risk that may
            // never materialize is disproportionate, and it previously forced every
            // operator to configure retention for a subsystem nothing populated.
            tracing::warn!(
                target: "rustcdc::core::runtime",
                "no schema_history_retention policy configured; schema history will grow \
                 without bound in DDL-heavy deployments. Configure \
                 RuntimeOptions::with_schema_history_retention() to bound it.",
            );
        }

        let committed_event_count = self
            .config
            .checkpoint
            .get_committed_count()
            .await
            .inspect_err(|error| {
                self.record_runtime_error("runtime.start.committed_count", error)
            })?;
        self.commit_barrier
            .hydrate_committed_event_count(committed_event_count)
            .inspect_err(|error| {
                self.record_runtime_error("runtime.start.barrier_hydrate", error)
            })?;

        if matches!(self.source, RuntimeSource::Disabled) {
            self.state = RuntimeState::Running;
            self.observability().tracer.trace_checkpoint_barrier("open");
            self.reset_run_counters();
            return Ok(());
        }

        if self.config.incremental_snapshot.is_some() && !self.config.snapshot_tables.is_empty() {
            return Err(Error::ConfigError(
                "cannot configure both snapshot_tables and incremental_snapshot; choose one startup mode"
                    .into(),
            ));
        }

        let mut checkpoint_offset = self.config.checkpoint.load().await?;
        if let Some(offset) = checkpoint_offset.as_ref() {
            if self.is_snapshot_checkpoint(offset.as_ref()) {
                if self.config.incremental_snapshot.is_some() {
                    return Err(Error::ConfigError(
                        "cannot resume incremental snapshot startup from a snapshot checkpoint"
                            .into(),
                    ));
                }
                if !self.source_capabilities().snapshot_checkpoint_resume {
                    tracing::warn!(
                        target: "rustcdc::runtime",
                        source = self.config.source.source_type().unwrap_or("unknown"),
                        "snapshot checkpoint resume is unsupported by connector; restarting snapshot from scratch"
                    );
                    checkpoint_offset = None;
                }

                if checkpoint_offset.is_some() && self.config.snapshot_tables.is_empty() {
                    return Err(Error::ConfigError(
                        "snapshot_tables must not be empty when resuming from a snapshot checkpoint"
                            .into(),
                    ));
                }
            }
        }

        self.source
            .connect()
            .await
            .inspect_err(|error| self.record_runtime_error("runtime.start.connect", error))?;

        if let Some(incremental) = self.config.incremental_snapshot.clone() {
            self.snapshot = None;
            self.stream = Some(
                self.source
                    .start_incremental_snapshot(incremental, checkpoint_offset.as_deref())
                    .await?,
            );
            self.handoff_complete = true;

            self.state = RuntimeState::Running;
            self.observability().tracer.trace_checkpoint_barrier("open");
            self.reset_run_counters();
            return Ok(());
        }

        if let Some(offset) = checkpoint_offset.as_ref() {
            if self.is_snapshot_checkpoint(offset.as_ref()) {
                self.snapshot = Some(
                    self.source
                        .start_snapshot_from_checkpoint(
                            &self.config.snapshot_tables,
                            offset.as_ref(),
                        )
                        .await?,
                );
                let stream_resume_from = self
                    .stream_resume_offset_for_snapshot_checkpoint(offset.as_ref())?
                    .ok_or_else(|| {
                        Error::StateError(
                            "cannot resume streaming after snapshot checkpoint: no \
                             'snapshot_watermark' LSN was found in the checkpoint payload. \
                             Streaming without a known start position risks a data-loss window. \
                             Re-run the snapshot from scratch to obtain a fresh watermark."
                                .into(),
                        )
                    })?;
                self.stream = Some(
                    self.source
                        .start_stream(Some(stream_resume_from.as_ref()))
                        .await?,
                );
                self.handoff_complete = false;
            } else {
                self.stream = Some(self.source.start_stream(Some(offset.as_ref())).await?);
                self.snapshot = None;
                self.handoff_complete = true;
            }
        } else if self.config.snapshot_tables.is_empty() {
            self.snapshot = None;
            self.stream = Some(self.source.start_stream(None).await?);
            self.handoff_complete = true;
        } else {
            self.snapshot = Some(
                self.source
                    .start_snapshot(&self.config.snapshot_tables)
                    .await?,
            );
            self.stream = Some(self.source.start_stream(None).await?);
            self.handoff_complete = false;
        }

        self.state = RuntimeState::Running;
        self.observability().tracer.trace_checkpoint_barrier("open");
        self.reset_run_counters();
        Ok(())
    }

    /// Zero the per-run counters and timestamps `start()` reports against.
    ///
    /// One function rather than three copies: the copies had drifted, and the drift was
    /// silent. `total_events_skipped` was reset by none of them, so a restarted runtime
    /// reported skips from a previous run, and the `Disabled` source path set no
    /// `started_at_ms` at all — leaving `uptime_ms` at zero forever on the one
    /// configuration used by every embedder testing against a custom source.
    fn reset_run_counters(&mut self) {
        self.started_at_ms = Some(now_millis());
        self.last_poll_at_ms = None;
        self.last_source_event_ts_ms = None;
        self.last_commit_at_ms = None;
        self.total_events_polled = 0;
        self.total_events_committed = 0;
        self.total_events_deduplicated = 0;
        self.total_events_skipped = 0;
    }

    fn is_snapshot_checkpoint(&self, offset: &dyn Offset) -> bool {
        let Some(source_type) = self.config.source.source_type() else {
            return false;
        };
        let expected_snapshot_source = format!("{source_type}_snapshot");
        offset.source_type() == expected_snapshot_source
    }

    #[allow(unused_variables)]
    fn stream_resume_offset_for_snapshot_checkpoint(
        &self,
        snapshot_checkpoint: &dyn Offset,
    ) -> Result<Option<Box<dyn Offset>>> {
        #[cfg(feature = "postgres")]
        if matches!(&self.config.source, RuntimeSourceConfig::Postgres(_)) {
            return Ok(Some(Box::new(
                self.postgres_stream_offset_from_snapshot_checkpoint(snapshot_checkpoint)?,
            )));
        }

        #[cfg(feature = "mysql")]
        let mysql_family = {
            let mysql_family = matches!(&self.config.source, RuntimeSourceConfig::Mysql(_));
            #[cfg(feature = "mariadb")]
            let mysql_family =
                mysql_family || matches!(&self.config.source, RuntimeSourceConfig::MariaDb(_));
            mysql_family
        };

        #[cfg(feature = "mysql")]
        if mysql_family {
            // The flavor determines the checkpoint file name, so it must travel with
            // the offset — see `MysqlOffset::source_flavor`.
            let flavor = {
                #[cfg(feature = "mariadb")]
                {
                    if matches!(&self.config.source, RuntimeSourceConfig::MariaDb(_)) {
                        "mariadb"
                    } else {
                        "mysql"
                    }
                }
                #[cfg(not(feature = "mariadb"))]
                {
                    "mysql"
                }
            };
            let offset =
                Self::mysql_stream_offset_from_snapshot_checkpoint(snapshot_checkpoint, flavor)?;
            #[cfg(feature = "mariadb")]
            if matches!(&self.config.source, RuntimeSourceConfig::MariaDb(_)) {
                return Ok(Some(Box::new(GenericOffset::new(
                    "mariadb",
                    offset.encode()?,
                ))));
            }
            return Ok(Some(Box::new(offset)));
        }

        #[cfg(feature = "sqlserver")]
        if matches!(&self.config.source, RuntimeSourceConfig::SqlServer(_)) {
            return Ok(Some(Box::new(
                Self::sqlserver_stream_offset_from_snapshot_checkpoint(snapshot_checkpoint)?,
            )));
        }

        Ok(None)
    }

    #[cfg(feature = "postgres")]
    fn postgres_stream_offset_from_snapshot_checkpoint(
        &self,
        snapshot_checkpoint: &dyn Offset,
    ) -> Result<PostgresOffset> {
        let payload = snapshot_checkpoint.encode()?;
        let value: serde_json::Value = serde_json::from_slice(&payload)
            .map_err(|error| Error::CheckpointError(error.to_string()))?;

        let lsn = value
            .get("snapshot_watermark")
            .and_then(|entry| entry.as_u64())
            .ok_or_else(|| {
                Error::CheckpointError(
                    "postgres snapshot checkpoint is missing 'snapshot_watermark'".into(),
                )
            })?;

        let slot_name = match &self.config.source {
            RuntimeSourceConfig::Postgres(cfg) => cfg.replication_slot_name.clone(),
            _ => {
                return Err(Error::StateError(
                    "postgres stream resume conversion called for non-postgres runtime source"
                        .into(),
                ));
            }
        };

        Ok(PostgresOffset::new(lsn, slot_name))
    }

    #[cfg(feature = "mysql")]
    fn mysql_stream_offset_from_snapshot_checkpoint(
        snapshot_checkpoint: &dyn Offset,
        source_flavor: &str,
    ) -> Result<MysqlOffset> {
        let payload = snapshot_checkpoint.encode()?;
        let value: serde_json::Value = serde_json::from_slice(&payload)
            .map_err(|error| Error::CheckpointError(error.to_string()))?;

        let binlog_file = value
            .get("binlog_file")
            .and_then(|entry| entry.as_str())
            .ok_or_else(|| {
                Error::CheckpointError("mysql snapshot checkpoint is missing 'binlog_file'".into())
            })?
            .to_string();
        let binlog_pos = value
            .get("binlog_pos")
            .and_then(|entry| entry.as_u64())
            .ok_or_else(|| {
                Error::CheckpointError("mysql snapshot checkpoint is missing 'binlog_pos'".into())
            })?;
        let binlog_pos = u32::try_from(binlog_pos).map_err(|_| {
            Error::CheckpointError("mysql snapshot checkpoint binlog_pos exceeds u32".into())
        })?;
        let gtid = value
            .get("gtid")
            .and_then(|entry| entry.as_str())
            .unwrap_or_default()
            .to_string();

        Ok(MysqlOffset::new(
            source_flavor,
            binlog_file,
            binlog_pos,
            gtid,
        ))
    }

    #[cfg(feature = "sqlserver")]
    fn sqlserver_stream_offset_from_snapshot_checkpoint(
        snapshot_checkpoint: &dyn Offset,
    ) -> Result<GenericOffset> {
        let payload = snapshot_checkpoint.encode()?;
        let value: serde_json::Value = serde_json::from_slice(&payload)
            .map_err(|error| Error::CheckpointError(error.to_string()))?;

        let lsn_start = value
            .get("lsn_start")
            .and_then(|entry| entry.as_array())
            .ok_or_else(|| {
                Error::CheckpointError(
                    "sqlserver snapshot checkpoint is missing 'lsn_start'".into(),
                )
            })?;

        if lsn_start.len() != 10 {
            return Err(Error::CheckpointError(
                "sqlserver snapshot checkpoint lsn_start must contain exactly 10 bytes".into(),
            ));
        }

        let mut bytes = Vec::with_capacity(10);
        for value in lsn_start {
            let byte = value.as_u64().ok_or_else(|| {
                Error::CheckpointError(
                    "sqlserver snapshot checkpoint lsn_start contains non-byte value".into(),
                )
            })?;
            let byte = u8::try_from(byte).map_err(|_| {
                Error::CheckpointError(
                    "sqlserver snapshot checkpoint lsn_start contains out-of-range byte".into(),
                )
            })?;
            bytes.push(byte);
        }

        Ok(GenericOffset::new(
            "sqlserver",
            serde_json::to_vec(&format!(
                "0x{}",
                bytes
                    .iter()
                    .map(|byte| format!("{byte:02X}"))
                    .collect::<String>()
            ))
            .map_err(|error| Error::SerializationError(error.to_string()))?,
        ))
    }

    /// Drive the whole pipeline into the registered sink until `shutdown` is cancelled.
    ///
    /// This is the loop every embedder writes, written once:
    ///
    /// ```text
    /// poll a batch → send each event → flush the sink → acknowledge
    /// ```
    ///
    /// The ordering is the part worth having in the library. Flushing **before**
    /// acknowledging is what makes at-least-once hold: acknowledge first and a crash in
    /// the gap advances the durable checkpoint past events the sink never accepted, and
    /// nothing replays them. It is one line to get wrong and it fails silently, months
    /// later, as rows that are simply missing downstream.
    ///
    /// Register the sink with [`CdcRuntime::register_sink`] and
    /// [`start`](CdcRuntime::start) the runtime first.
    ///
    /// # Returns
    ///
    /// The number of events delivered and acknowledged, once `shutdown` is cancelled.
    /// The runtime is still `Running` and the sink still registered — call
    /// [`stop`](CdcRuntime::stop) to shut down and close the sink.
    ///
    /// Cancellation is observed **between** polls, never in the middle of one, because
    /// [`poll_event_batch`](CdcRuntime::poll_event_batch) is not cancel-safe. Shutdown
    /// therefore takes up to `max_poll_wait_ms` — the budget the poll already returns
    /// within.
    ///
    /// # Errors
    ///
    /// Returns on the first error from the source, a transform, the sink, or the
    /// checkpoint. A batch that failed mid-delivery is **not** acknowledged, so it is
    /// redelivered by the next poll — retrying is calling this again. Because that
    /// batch is left in flight, [`stop`](CdcRuntime::stop) refuses until it is
    /// acknowledged; [`force_stop`](CdcRuntime::force_stop) hands it back instead.
    ///
    /// # When to write the loop yourself
    ///
    /// When the write has to be coordinated with something the runtime cannot see — your
    /// own database transaction, a two-phase commit, per-branch error handling across a
    /// fan-out. [`poll_event_batch`](CdcRuntime::poll_event_batch) and
    /// [`commit_ack`](CdcRuntime::commit_ack) remain the supported way to do that.
    ///
    /// # Flush frequency
    ///
    /// Every batch is flushed, including for a sink that would rather batch across
    /// several. That is not tunable here, and deliberately so: the acknowledgement
    /// cannot outrun the flush without giving up the guarantee, so a rarer flush means a
    /// rarer acknowledgement and a growing redelivery window. Batch inside the sink
    /// (raise [`RuntimeOptions::max_buffer_size`] to hand it more per call) rather than
    /// by acknowledging late.
    pub async fn run_to_completion(
        &mut self,
        shutdown: tokio_util::sync::CancellationToken,
    ) -> Result<u64> {
        // Moved out for the duration of the run so the loop can hold `&mut sink` and
        // `&mut self` at once, and put back afterwards so `stop()` still closes it.
        let slot = self.registered_sink.take().ok_or_else(|| {
            Error::ConfigError(
                "run_to_completion needs a sink to deliver to; call \
                 CdcRuntime::register_sink(...) before start(), or drive \
                 poll_event_batch/commit_ack yourself"
                    .into(),
            )
        })?;
        let mut sink = slot
            .into_inner()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        let result = self.run_sink_loop(&mut sink, shutdown).await;
        self.registered_sink = Some(std::sync::Mutex::new(sink));
        result
    }

    async fn run_sink_loop(
        &mut self,
        sink: &mut BoxedSink,
        shutdown: tokio_util::sync::CancellationToken,
    ) -> Result<u64> {
        use crate::sink::SinkAdapter as _;

        let sink_name = sink.name().to_string();
        let mut delivered = 0u64;

        loop {
            // Checked between polls, never raced against one. `poll_event_batch` is not
            // cancel-safe: dropping it after the source has handed over a batch but before
            // the runtime has staged it discards events that have left the source's buffer
            // and never reached the commit barrier. Nothing acknowledges them, so a restart
            // replays them — but a runtime that keeps polling skips them permanently.
            //
            // Shutdown latency is therefore bounded by `max_poll_wait_ms`, which is the
            // budget the poll already promises to return within.
            if shutdown.is_cancelled() {
                break;
            }

            let batch = self.poll_event_batch().await?;

            if batch.is_empty() {
                // `poll_event_batch` already waited out `max_poll_wait_ms`, so this is
                // not a spin — but a source that returns empty synchronously would make
                // it one, and `event_batches()` has the same guard for the same reason.
                tokio::task::yield_now().await;
                continue;
            }

            let count = batch.len();
            for event in batch.events() {
                sink.send(event).await.inspect_err(|error| {
                    self.record_runtime_error("runtime.run.sink_send", error);
                    tracing::error!(
                        target: "rustcdc::core::runtime",
                        sink = %sink_name,
                        table = %event.table,
                        offset = %event.source.offset,
                        error = %error.report(),
                        "sink rejected an event; the batch is not acknowledged and will be \
                         redelivered",
                    );
                })?;
            }

            // Durable at the sink *before* the checkpoint moves. Reversing these two is
            // the silent data-loss bug this method exists to stop embedders writing.
            sink.flush().await.inspect_err(|error| {
                self.record_runtime_error("runtime.run.sink_flush", error);
                tracing::error!(
                    target: "rustcdc::core::runtime",
                    sink = %sink_name,
                    events = count,
                    error = %error.report(),
                    "sink flush failed; the batch is not acknowledged and will be redelivered",
                );
            })?;

            self.commit_ack(batch.ack_mode()).await?;
            delivered = delivered.saturating_add(count as u64);
        }

        Ok(delivered)
    }

    /// Stop the runtime.
    ///
    /// This is safe-by-default and will fail if there are uncommitted in-memory
    /// events. Callers must acknowledge deliveries first, or use `force_stop()`
    /// to explicitly drain pending events.
    pub async fn stop(&mut self) -> Result<Vec<Event>> {
        match self.state {
            RuntimeState::Idle | RuntimeState::Stopped => {
                self.state = RuntimeState::Stopped;
                return Ok(Vec::new());
            }
            RuntimeState::Stopping => {
                let error = Error::StateError("runtime already stopping".into());
                self.record_runtime_error("runtime.stop.state", &error);
                return Err(error);
            }
            RuntimeState::Running => {}
        }

        let pending_events = self
            .commit_barrier
            .pending_count()
            .saturating_add(self.injected_events.len())
            .saturating_add(self.pending_source_events.len());
        if pending_events > 0 {
            let error = Error::StateError(format!(
                "runtime has {pending_events} uncommitted events; commit acknowledgements before stop or call force_stop()"
            ));
            self.record_runtime_error("runtime.stop.uncommitted", &error);
            return Err(error);
        }

        self.state = RuntimeState::Stopping;
        self.delivered_not_committed = 0;
        self.pending_delivery = None;
        self.source.close().await;

        self.snapshot = None;
        self.stream = None;
        self.started_at_ms = None;
        self.last_source_event_ts_ms = None;
        self.observability()
            .tracer
            .trace_checkpoint_barrier("stopped");
        self.state = RuntimeState::Stopped;

        if let Some(mutex) = self.registered_sink.take() {
            let mut sink = mutex
                .into_inner()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let result = if let Some(timeout_ms) = self.config.options.sink_close_timeout_ms {
                sink.close_with_timeout(timeout_ms).await
            } else {
                sink.close().await
            };
            if let Err(ref e) = result {
                self.observability()
                    .metrics
                    .record_error(e, "runtime.stop.sink_close");
            }
            result?;
        }

        Ok(Vec::new())
    }

    /// Force stop the runtime and drain all pending in-memory events.
    ///
    /// This is intended for emergency shutdown paths where replay/duplication
    /// handling is delegated to the embedder.
    pub async fn force_stop(&mut self) -> Result<Vec<Event>> {
        match self.state {
            RuntimeState::Idle | RuntimeState::Stopped => {
                self.state = RuntimeState::Stopped;
                return Ok(Vec::new());
            }
            RuntimeState::Stopping => {
                let error = Error::StateError("runtime already stopping".into());
                self.record_runtime_error("runtime.force_stop.state", &error);
                return Err(error);
            }
            RuntimeState::Running => {}
        }

        self.state = RuntimeState::Stopping;
        // Drained in **delivery order**, which is the order `poll_event_batch` would have
        // produced them in: the in-flight delivery first, then what is buffered behind
        // it, then what has been read from the source but not cut into a batch, and the
        // injected queue last — `poll_event_batch` only reaches it once the source queues
        // are empty. Returning them in any other order hands the embedder a stream it
        // cannot apply, which for CDC means the older value of a row landing last.
        let mut drained: Vec<Event> = Vec::new();
        if let Some(pending) = self.pending_delivery.take() {
            // Only re-drain events that were not yet committed.
            drained.extend(pending.events[pending.committed_prefix..].iter().cloned());
        }
        drained.extend(self.buffered_events.drain(..));
        drained.extend(self.pending_source_events.drain(..));
        drained.extend(std::mem::take(&mut self.injected_events));
        self.commit_barrier.clear_pending();
        let drained_event_count = drained.len();
        for event in &drained {
            self.observability()
                .tracer
                .trace_event_end(&Self::event_trace_id(event), "force_stopped");
        }
        tracing::warn!(
            target: "rustcdc::core::runtime",
            shutdown_mode = "forced",
            drained_events = drained_event_count,
            "force_stop called; uncommitted events discarded — embedder must handle replay/deduplication"
        );
        self.delivered_not_committed = 0;
        self.source.close().await;

        self.snapshot = None;
        self.stream = None;
        self.started_at_ms = None;
        self.last_source_event_ts_ms = None;
        self.observability()
            .tracer
            .trace_checkpoint_barrier("stopped");
        self.state = RuntimeState::Stopped;

        if let Some(mutex) = self.registered_sink.take() {
            let mut sink = mutex
                .into_inner()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let result = if let Some(timeout_ms) = self.config.options.sink_close_timeout_ms {
                sink.close_with_timeout(timeout_ms).await
            } else {
                sink.close().await
            };
            if let Err(ref e) = result {
                self.observability()
                    .metrics
                    .record_error(e, "runtime.force_stop.sink_close");
            }
            result?;
        }

        Ok(drained)
    }

    /// Drain buffered in-flight events, acknowledge them, then stop the runtime cleanly.
    ///
    /// This is a convenience shortcut for the common shutdown pattern:
    ///
    /// ```ignore
    /// while let Ok(batch) = runtime.poll_event_batch().await {
    ///     if batch.is_empty() { break; }
    ///     runtime.commit_ack(batch.ack_mode()).await?;
    /// }
    /// runtime.stop().await?;
    /// ```
    ///
    /// # Termination semantics
    ///
    /// `drain_and_stop` terminates on the **first empty batch** returned by
    /// `poll_event_batch()`. For a finite source (e.g. a snapshot or a mock),
    /// this drains all buffered events. For a **continuous live-replication
    /// stream**, the source will appear empty as soon as the internal buffer
    /// drains — which may happen quickly even while the upstream database is
    /// still producing writes. In that case `drain_and_stop` stops after the
    /// current buffer is consumed, not after the stream is logically
    /// exhausted.
    ///
    /// For continuous streams, prefer calling [`CdcRuntime::stop()`] after your consumer
    /// loop signals shutdown, or use [`CdcRuntime::force_stop()`] if you need an
    /// unconditional immediate halt.
    ///
    /// Returns the drained events, in delivery order, after committing them.
    ///
    /// # You must consume the returned events
    ///
    /// `drain_and_stop` acknowledges each batch it polls, which advances the durable
    /// checkpoint (and, for connectors that support it, the source-side confirmation
    /// such as the PostgreSQL replication slot) **past** those events. They will not
    /// be replayed on restart. Returning them is therefore the only way they reach a
    /// consumer — dropping the returned `Vec` is unrecoverable data loss.
    ///
    /// If you do not intend to process the drained events, call
    /// [`CdcRuntime::stop()`] instead, which refuses to stop while events are
    /// uncommitted, or [`CdcRuntime::force_stop()`], which discards them explicitly.
    #[must_use = "drain_and_stop commits the returned events; dropping them loses data permanently"]
    pub async fn drain_and_stop(&mut self) -> Result<Vec<Event>> {
        let mut drained: Vec<Event> = Vec::new();
        loop {
            let batch = self.poll_event_batch().await?;
            if batch.is_empty() {
                break;
            }
            let ack = batch.ack_mode();
            drained.extend(batch.into_events());
            self.commit_ack(ack).await?;
        }
        self.stop().await?;
        Ok(drained)
    }
}
