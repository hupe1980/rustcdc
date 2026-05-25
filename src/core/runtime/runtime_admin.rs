use super::*;

fn runtime_state_label(state: RuntimeState) -> &'static str {
    match state {
        RuntimeState::Idle => "idle",
        RuntimeState::Running => "running",
        RuntimeState::Stopping => "stopping",
        RuntimeState::Stopped => "stopped",
    }
}

impl<C, H> CdcRuntime<C, H>
where
    C: crate::checkpoint::Checkpoint + Send + Sync + 'static,
    H: SchemaHistory + Send + Sync + 'static,
{
    /// Return the current lifecycle state.
    pub fn state(&self) -> RuntimeState {
        self.state
    }

    /// Report capabilities for the configured source.
    pub const fn source_capabilities(&self) -> ConnectorCapabilities {
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
            in_flight_events: self
                .pending_delivery
                .as_ref()
                .map_or(0, |pending| pending.events.len()),
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
    /// Falls back to poll recency until a source timestamp is observed.
    pub(super) fn estimate_replication_lag_ms(&self) -> Option<u64> {
        let now = now_millis();
        if let Some(source_ts) = self.last_source_event_ts_ms {
            return Some(now.saturating_sub(source_ts.min(now)));
        }
        self.last_poll_at_ms
            .map(|poll_time| now.saturating_sub(poll_time))
    }

    /// Render the current admin snapshot as JSON.
    pub fn admin_snapshot_json(&self) -> Result<String> {
        serde_json::to_string(&self.admin_snapshot())
            .map_err(|error| Error::SerializationError(error.to_string()))
    }

    /// Render runtime admin metrics in a Prometheus-friendly text exposition format.
    pub fn admin_metrics_prometheus(&self) -> String {
        let admin = self.admin_snapshot();
        let mut out = String::new();

        out.push_str("# HELP cdc_runtime_readiness Runtime readiness (1=ready, 0=not ready).\n");
        out.push_str("# TYPE cdc_runtime_readiness gauge\n");
        out.push_str(&format!(
            "cdc_runtime_readiness{{state=\"{}\"}} {}\n",
            admin.state,
            if admin.readiness { 1 } else { 0 }
        ));

        out.push_str("# HELP cdc_runtime_liveness Runtime liveness (1=alive, 0=stopped).\n");
        out.push_str("# TYPE cdc_runtime_liveness gauge\n");
        out.push_str(&format!(
            "cdc_runtime_liveness{{state=\"{}\"}} {}\n",
            admin.state,
            if admin.liveness { 1 } else { 0 }
        ));

        out.push_str(
            "# HELP cdc_runtime_buffer_depth Number of buffered events waiting for delivery.\n",
        );
        out.push_str("# TYPE cdc_runtime_buffer_depth gauge\n");
        out.push_str(&format!(
            "cdc_runtime_buffer_depth {}\n",
            admin.buffer_depth
        ));

        out.push_str(
            "# HELP cdc_runtime_in_flight_events Number of delivered but uncommitted events.\n",
        );
        out.push_str("# TYPE cdc_runtime_in_flight_events gauge\n");
        out.push_str(&format!(
            "cdc_runtime_in_flight_events {}\n",
            admin.in_flight_events
        ));

        out.push_str(
            "# HELP cdc_runtime_events_polled_total Total events delivered by runtime batches.\n",
        );
        out.push_str("# TYPE cdc_runtime_events_polled_total counter\n");
        out.push_str(&format!(
            "cdc_runtime_events_polled_total {}\n",
            admin.total_events_polled
        ));

        out.push_str("# HELP cdc_runtime_events_committed_total Total events acknowledged and checkpointed.\n");
        out.push_str("# TYPE cdc_runtime_events_committed_total counter\n");
        out.push_str(&format!(
            "cdc_runtime_events_committed_total {}\n",
            admin.total_events_committed
        ));

        out.push_str(
            "# HELP cdc_runtime_events_deduplicated_total Total events suppressed by runtime idempotency guard.\n",
        );
        out.push_str("# TYPE cdc_runtime_events_deduplicated_total counter\n");
        out.push_str(&format!(
            "cdc_runtime_events_deduplicated_total {}\n",
            admin.total_events_deduplicated
        ));

        if let Some(checkpoint_age_ms) = admin.checkpoint_age_ms {
            out.push_str("# HELP cdc_runtime_checkpoint_age_ms Age of last durable checkpoint in milliseconds.\n");
            out.push_str("# TYPE cdc_runtime_checkpoint_age_ms gauge\n");
            out.push_str(&format!(
                "cdc_runtime_checkpoint_age_ms {}\n",
                checkpoint_age_ms
            ));
        }

        if let Some(lag_ms) = admin.replication_lag_ms {
            out.push_str("# HELP cdc_runtime_replication_lag_ms Estimated replication lag in milliseconds (source event timestamp preferred; poll recency fallback).\n");
            out.push_str("# TYPE cdc_runtime_replication_lag_ms gauge\n");
            out.push_str(&format!("cdc_runtime_replication_lag_ms {}\n", lag_ms));
        }

        out.push_str("# HELP cdc_runtime_source_capability Connector capability flags.\n");
        out.push_str("# TYPE cdc_runtime_source_capability gauge\n");
        out.push_str(&format_capability_metric(
            "snapshot",
            admin.capabilities.snapshot,
        ));
        out.push_str(&format_capability_metric(
            "handoff",
            admin.capabilities.handoff,
        ));
        out.push_str(&format_capability_metric(
            "ddl_capture",
            admin.capabilities.ddl_capture,
        ));
        out.push_str(&format_capability_metric(
            "heartbeat",
            admin.capabilities.heartbeat,
        ));
        out.push_str(&format_capability_metric("tls", admin.capabilities.tls));
        out.push_str(&format_capability_metric(
            "schema_introspection",
            admin.capabilities.schema_introspection,
        ));

        out
    }
}
