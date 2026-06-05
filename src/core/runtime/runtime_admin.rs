use super::*;

fn runtime_state_label(state: RuntimeState) -> &'static str {
    match state {
        RuntimeState::Idle => "idle",
        RuntimeState::Running => "running",
        RuntimeState::Stopping => "stopping",
        RuntimeState::Stopped => "stopped",
    }
}

impl CdcRuntime {
    /// Return the current lifecycle state.
    pub fn state(&self) -> RuntimeState {
        self.state
    }

    /// Report capabilities for the configured source.
    pub fn source_capabilities(&self) -> ConnectorCapabilities {
        self.config.source.capabilities()
    }

    /// Return an embeddable admin snapshot for runtime health and capabilities introspection.
    pub fn admin_snapshot(&self) -> RuntimeAdminSnapshot {
        let now_ms = now_millis();
        let checkpoint_age_ms = self
            .last_checkpoint_saved_at_ms
            .map(|checkpoint_time| now_ms.saturating_sub(checkpoint_time));

        RuntimeAdminSnapshot {
            source_type: self.config.source.source_type().map(str::to_string),
            state: runtime_state_label(self.state).to_string(),
            readiness: self.state == RuntimeState::Running
                && (matches!(self.config.source, RuntimeSourceConfig::Disabled)
                    || self.stream.is_some()
                    || self.snapshot.is_some()),
            liveness: self.state != RuntimeState::Stopped,
            capabilities: self.source_capabilities(),
            buffer_depth: self.buffered_events.len()
                + self.injected_events.len()
                + self.pending_source_events.len(),
            in_flight_events: self.pending_delivery.as_ref().map_or(0, |pending| {
                pending
                    .events
                    .len()
                    .saturating_sub(pending.committed_prefix)
            }),
            snapshot_active: self.snapshot.is_some(),
            stream_active: self.stream.is_some(),
            handoff_complete: self.handoff_complete,
            total_events_polled: self.total_events_polled,
            total_events_committed: self.total_events_committed,
            total_events_deduplicated: self.total_events_deduplicated,
            started_at_ms: self.started_at_ms,
            last_poll_at_ms: self.last_poll_at_ms,
            last_commit_at_ms: self.last_commit_at_ms,
            checkpoint_age_ms,
            replication_lag_ms: self.estimate_replication_lag_ms(),
        }
    }

    /// Estimate replication lag from source event timestamps when available.
    ///
    /// Returns `None` until the first source-stamped event is observed so that
    /// callers receive a reliable signal rather than a misleading poll-age proxy.
    pub(super) fn estimate_replication_lag_ms(&self) -> Option<u64> {
        let now = now_millis();
        let source_ts = self.last_source_event_ts_ms?;
        Some(now.saturating_sub(source_ts.min(now)))
    }

    /// Render the current admin snapshot as JSON.
    pub fn admin_snapshot_json(&self) -> Result<String> {
        serde_json::to_string(&self.admin_snapshot())
            .map_err(|error| Error::SerializationError(error.to_string()))
    }

    /// Write runtime admin metrics in Prometheus text exposition format to any
    /// [`std::io::Write`] sink.
    ///
    /// Prefer this over [`Self::admin_metrics_prometheus`] when writing directly
    /// to an HTTP response body or file: it avoids the intermediate `String`
    /// allocation and lets the caller drive the output buffer.
    ///
    /// # Errors
    ///
    /// Propagates any `std::io::Error` from the underlying writer. Writes to an
    /// in-memory `Vec<u8>` are infallible in practice.
    pub fn write_admin_metrics_prometheus<W: std::io::Write>(
        &self,
        w: &mut W,
    ) -> std::io::Result<()> {
        let admin = self.admin_snapshot();
        let source_type = admin.source_type.as_deref().unwrap_or("unknown");

        writeln!(
            w,
            "# HELP cdc_runtime_readiness Runtime readiness (1=ready, 0=not ready).\n\
             # TYPE cdc_runtime_readiness gauge\n\
             cdc_runtime_readiness{{source_type=\"{source_type}\",state=\"{}\"}} {}",
            admin.state,
            u8::from(admin.readiness)
        )?;

        writeln!(
            w,
            "# HELP cdc_runtime_liveness Runtime liveness (1=alive, 0=stopped).\n\
             # TYPE cdc_runtime_liveness gauge\n\
             cdc_runtime_liveness{{source_type=\"{source_type}\",state=\"{}\"}} {}",
            admin.state,
            u8::from(admin.liveness)
        )?;

        writeln!(
            w,
            "# HELP cdc_runtime_buffer_depth Number of buffered events waiting for delivery.\n\
             # TYPE cdc_runtime_buffer_depth gauge\n\
             cdc_runtime_buffer_depth{{source_type=\"{source_type}\"}} {}",
            admin.buffer_depth
        )?;

        writeln!(
            w,
            "# HELP cdc_runtime_in_flight_events Number of delivered but uncommitted events.\n\
             # TYPE cdc_runtime_in_flight_events gauge\n\
             cdc_runtime_in_flight_events{{source_type=\"{source_type}\"}} {}",
            admin.in_flight_events
        )?;

        writeln!(
            w,
            "# HELP cdc_runtime_events_polled_total Total events delivered by runtime batches.\n\
             # TYPE cdc_runtime_events_polled_total counter\n\
             cdc_runtime_events_polled_total{{source_type=\"{source_type}\"}} {}",
            admin.total_events_polled
        )?;

        writeln!(
            w,
            "# HELP cdc_runtime_events_committed_total Total events acknowledged and checkpointed.\n\
             # TYPE cdc_runtime_events_committed_total counter\n\
             cdc_runtime_events_committed_total{{source_type=\"{source_type}\"}} {}",
            admin.total_events_committed
        )?;

        writeln!(
            w,
            "# HELP cdc_runtime_events_deduplicated_total Total events suppressed by runtime idempotency guard.\n\
             # TYPE cdc_runtime_events_deduplicated_total counter\n\
             cdc_runtime_events_deduplicated_total{{source_type=\"{source_type}\"}} {}",
            admin.total_events_deduplicated
        )?;

        if let Some(checkpoint_age_ms) = admin.checkpoint_age_ms {
            writeln!(
                w,
                "# HELP cdc_runtime_checkpoint_age_ms Age of last durable checkpoint in milliseconds.\n\
                 # TYPE cdc_runtime_checkpoint_age_ms gauge\n\
                 cdc_runtime_checkpoint_age_ms{{source_type=\"{source_type}\"}} {}",
                checkpoint_age_ms
            )?;
        }

        if let Some(lag_ms) = admin.replication_lag_ms {
            writeln!(
                w,
                "# HELP cdc_runtime_replication_lag_ms Estimated replication lag in milliseconds (source event timestamp preferred; poll recency fallback).\n\
                 # TYPE cdc_runtime_replication_lag_ms gauge\n\
                 cdc_runtime_replication_lag_ms{{source_type=\"{source_type}\"}} {}",
                lag_ms
            )?;
        }

        writeln!(
            w,
            "# HELP cdc_runtime_source_capability Connector capability flags.\n\
             # TYPE cdc_runtime_source_capability gauge"
        )?;

        for (name, enabled) in [
            ("snapshot", admin.capabilities.snapshot),
            ("handoff", admin.capabilities.handoff),
            ("ddl_capture", admin.capabilities.ddl_capture),
            ("heartbeat", admin.capabilities.heartbeat),
            ("tls", admin.capabilities.tls),
            (
                "schema_introspection",
                admin.capabilities.schema_introspection,
            ),
            ("truncate", admin.capabilities.truncate),
            (
                "incremental_snapshot",
                admin.capabilities.incremental_snapshot,
            ),
        ] {
            writeln!(
                w,
                "cdc_runtime_source_capability{{source_type=\"{source_type}\",capability=\"{name}\"}} {}",
                u8::from(enabled)
            )?;
        }

        Ok(())
    }

    /// Render runtime admin metrics in a Prometheus-friendly text exposition format.
    ///
    /// For zero-copy output (e.g., HTTP response streaming), prefer
    /// [`Self::write_admin_metrics_prometheus`] which writes directly to any
    /// [`std::io::Write`] sink without the intermediate `String` allocation.
    pub fn admin_metrics_prometheus(&self) -> String {
        let mut buf = Vec::with_capacity(2048);
        self.write_admin_metrics_prometheus(&mut buf)
            .expect("writing to Vec<u8> is infallible");
        // SAFETY: all format strings above produce valid UTF-8.
        String::from_utf8(buf).expect("prometheus metrics output is always valid UTF-8")
    }
}
