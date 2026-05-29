use super::*;

impl<C, H> CdcRuntime<C, H>
where
    C: crate::checkpoint::Checkpoint + Send + Sync + 'static,
    H: SchemaHistory + Send + Sync + 'static,
{
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
            tracing::error!(
                target: "rustcdc::core::runtime",
                "no schema_history_retention policy configured; schema history will grow \
                 unboundedly. Configure RuntimeOptions::with_schema_history_retention() to \
                 avoid resource exhaustion in DDL-heavy deployments."
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
            self.started_at_ms = Some(now_millis());
            self.last_poll_at_ms = None;
            self.last_source_event_ts_ms = None;
            self.last_commit_at_ms = None;
            self.total_events_polled = 0;
            self.total_events_committed = 0;
            self.total_events_deduplicated = 0;
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
                let stream_resume_from =
                    self.stream_resume_offset_for_snapshot_checkpoint(offset.as_ref())?;
                self.stream = Some(
                    self.source
                        .start_stream(stream_resume_from.as_deref())
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
        self.started_at_ms = Some(now_millis());
        self.last_poll_at_ms = None;
        self.last_source_event_ts_ms = None;
        self.last_commit_at_ms = None;
        self.total_events_polled = 0;
        self.total_events_committed = 0;
        self.total_events_deduplicated = 0;
        Ok(())
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
        if matches!(&self.config.source, RuntimeSourceConfig::Mysql(_)) {
            return Ok(Some(Box::new(
                Self::mysql_stream_offset_from_snapshot_checkpoint(snapshot_checkpoint)?,
            )));
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

        Ok(PostgresOffset { lsn, slot_name })
    }

    #[cfg(feature = "mysql")]
    fn mysql_stream_offset_from_snapshot_checkpoint(
        snapshot_checkpoint: &dyn Offset,
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

        Ok(MysqlOffset {
            gtid,
            binlog_file,
            binlog_pos,
        })
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
        let mut drained = std::mem::take(&mut self.injected_events)
            .into_iter()
            .collect::<Vec<_>>();
        if let Some(pending) = self.pending_delivery.take() {
            // Only re-drain events that were not yet committed.
            drained.extend(pending.events[pending.committed_prefix..].iter().cloned());
        }
        drained.extend(self.buffered_events.drain(..));
        drained.extend(self.pending_source_events.drain(..));
        self.commit_barrier.clear_pending();
        for event in &drained {
            self.observability()
                .tracer
                .trace_event_end(&Self::event_trace_id(event), "force_stopped");
        }
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
        Ok(drained)
    }
}
