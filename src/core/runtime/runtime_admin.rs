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
                    || self.snapshot.is_some())
                // An unconfirmed-but-committed source position means the source is
                // replaying events the runtime has already committed, and the source
                // is retaining its log (WAL on a PostgreSQL primary) meanwhile.
                // Reporting ready here is what made this failure silent.
                && self.pending_confirmation_lsn.is_none(),
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
            total_events_skipped: self.total_events_skipped,
            health: self.derive_health(now_ms),
            started_at_ms: self.started_at_ms,
            last_poll_at_ms: self.last_poll_at_ms,
            last_commit_at_ms: self.last_commit_at_ms,
            checkpoint_age_ms,
            replication_lag_ms: self.estimate_replication_lag_ms(),
            replication_slot_lag_bytes: self
                .stream
                .as_ref()
                .and_then(|s| s.replication_slot_lag_bytes()),
        }
    }

    /// Derive a health verdict from the signals that distinguish idle from stalled.
    ///
    /// Ordered most-specific first, so the reported reason is the most actionable one
    /// rather than whichever check happens to run first.
    pub(super) fn derive_health(&self, now_ms: u64) -> HealthVerdict {
        if self.state != RuntimeState::Running {
            return HealthVerdict::NotRunning;
        }

        // 1. A durably committed position the source refuses to confirm. The source is
        //    replaying committed events and retaining its log meanwhile — on a
        //    PostgreSQL primary that grows WAL until the disk fills.
        if let Some(lsn) = self.pending_confirmation_lsn {
            return HealthVerdict::Stalled {
                reason: format!(
                    "source position {lsn} was durably checkpointed but could not be \
                     confirmed to the source; the source keeps replaying committed events \
                     and retains its log (WAL on a PostgreSQL primary) until this clears"
                ),
            };
        }

        // 2. The poll loop itself is not completing. Compared against the configured
        //    wait plus a generous multiple, so a slow-but-working poll is not flagged.
        let stall_threshold_ms = self
            .config
            .options
            .max_poll_wait_ms
            .saturating_mul(HEALTH_POLL_STALL_MULTIPLIER)
            .max(HEALTH_MIN_POLL_STALL_MS);
        if let Some(last_poll) = self.last_poll_at_ms {
            let since_poll = now_ms.saturating_sub(last_poll);
            if since_poll > stall_threshold_ms {
                return HealthVerdict::Stalled {
                    reason: format!(
                        "no poll has completed for {since_poll}ms (threshold \
                         {stall_threshold_ms}ms); the poll loop is blocked, not idle"
                    ),
                };
            }
        }

        // 3. Events delivered but not committed, with no recent commit. This is a
        //    *consumer* stall — the source is fine and the caller has stopped
        //    acknowledging — which looks identical to source idleness in the counters.
        let uncommitted = self
            .total_events_polled
            .saturating_sub(self.total_events_committed);
        if uncommitted > 0 {
            let since_commit = self
                .last_commit_at_ms
                .map(|at| now_ms.saturating_sub(at))
                .unwrap_or(u64::MAX);
            if since_commit > stall_threshold_ms {
                return HealthVerdict::Stalled {
                    reason: format!(
                        "{uncommitted} event(s) delivered but not committed, and no commit \
                         in {}ms; the consumer has stopped acknowledging (call commit_ack)",
                        if since_commit == u64::MAX {
                            "any".to_string()
                        } else {
                            since_commit.to_string()
                        }
                    ),
                };
            }
        }

        // Polling on schedule with nothing outstanding. If events have flowed recently
        // this is healthy; otherwise the source genuinely has no changes.
        match self.last_source_event_ts_ms {
            Some(_) if self.total_events_polled > 0 => HealthVerdict::Healthy,
            _ => HealthVerdict::Idle,
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
            "# HELP rustcdc_runtime_readiness Runtime readiness (1=ready, 0=not ready).\n\
             # TYPE rustcdc_runtime_readiness gauge\n\
             rustcdc_runtime_readiness{{source_type=\"{source_type}\",state=\"{}\"}} {}",
            admin.state,
            u8::from(admin.readiness)
        )?;

        writeln!(
            w,
            "# HELP rustcdc_runtime_liveness Runtime liveness (1=alive, 0=stopped).\n\
             # TYPE rustcdc_runtime_liveness gauge\n\
             rustcdc_runtime_liveness{{source_type=\"{source_type}\",state=\"{}\"}} {}",
            admin.state,
            u8::from(admin.liveness)
        )?;

        writeln!(
            w,
            "# HELP rustcdc_runtime_buffer_depth Number of buffered events waiting for delivery.\n\
             # TYPE rustcdc_runtime_buffer_depth gauge\n\
             rustcdc_runtime_buffer_depth{{source_type=\"{source_type}\"}} {}",
            admin.buffer_depth
        )?;

        writeln!(
            w,
            "# HELP rustcdc_runtime_in_flight_events Number of delivered but uncommitted events.\n\
             # TYPE rustcdc_runtime_in_flight_events gauge\n\
             rustcdc_runtime_in_flight_events{{source_type=\"{source_type}\"}} {}",
            admin.in_flight_events
        )?;

        writeln!(
            w,
            "# HELP rustcdc_runtime_events_polled_total Total events delivered by runtime batches.\n\
             # TYPE rustcdc_runtime_events_polled_total counter\n\
             rustcdc_runtime_events_polled_total{{source_type=\"{source_type}\"}} {}",
            admin.total_events_polled
        )?;

        writeln!(
            w,
            "# HELP rustcdc_runtime_events_committed_total Total events acknowledged and checkpointed.\n\
             # TYPE rustcdc_runtime_events_committed_total counter\n\
             rustcdc_runtime_events_committed_total{{source_type=\"{source_type}\"}} {}",
            admin.total_events_committed
        )?;

        writeln!(
            w,
            "# HELP rustcdc_runtime_events_deduplicated_total Total events suppressed by runtime idempotency guard.\n\
             # TYPE rustcdc_runtime_events_deduplicated_total counter\n\
             rustcdc_runtime_events_deduplicated_total{{source_type=\"{source_type}\"}} {}",
            admin.total_events_deduplicated
        )?;

        writeln!(
            w,
            "# HELP rustcdc_runtime_health Derived health verdict (1 for the active verdict, 0 otherwise). Alert on verdict=\"stalled\"; idle is normal.\n\
             # TYPE rustcdc_runtime_health gauge"
        )?;

        writeln!(
            w,
            "# HELP rustcdc_runtime_events_skipped_total Events permanently dropped by TransformErrorPolicy::Skip. Any non-zero value means data was lost: the checkpoint advances past skipped events so they are never replayed.\n\
             # TYPE rustcdc_runtime_events_skipped_total counter\n\
             rustcdc_runtime_events_skipped_total{{source_type=\"{source_type}\"}} {}",
            admin.total_events_skipped
        )?;

        // Health verdict as a labelled gauge: exactly one label set is 1 at any time,
        // so `rustcdc_runtime_health{verdict="stalled"} == 1` is a complete alert rule.
        // `state` alone cannot express this — a healthy-idle and a stalled connector
        // both report `state="running"`.
        for verdict in ["healthy", "idle", "stalled", "not_running"] {
            writeln!(
                w,
                "rustcdc_runtime_health{{source_type=\"{source_type}\",verdict=\"{verdict}\"}} {}",
                u8::from(admin.health.as_str() == verdict)
            )?;
        }

        if let Some(checkpoint_age_ms) = admin.checkpoint_age_ms {
            writeln!(
                w,
                "# HELP rustcdc_runtime_checkpoint_age_ms Age of last durable checkpoint in milliseconds.\n\
                 # TYPE rustcdc_runtime_checkpoint_age_ms gauge\n\
                 rustcdc_runtime_checkpoint_age_ms{{source_type=\"{source_type}\"}} {}",
                checkpoint_age_ms
            )?;
        }

        if let Some(lag_ms) = admin.replication_lag_ms {
            writeln!(
                w,
                "# HELP rustcdc_runtime_replication_lag_ms Estimated replication lag in milliseconds (source event timestamp preferred; poll recency fallback).\n\
                 # TYPE rustcdc_runtime_replication_lag_ms gauge\n\
                 rustcdc_runtime_replication_lag_ms{{source_type=\"{source_type}\"}} {}",
                lag_ms
            )?;
        }

        if let Some(lag_bytes) = admin.replication_slot_lag_bytes {
            writeln!(
                w,
                "# HELP rustcdc_replication_slot_lag_bytes PostgreSQL replication slot WAL lag in bytes (pg_current_wal_lsn - confirmed_flush_lsn). Non-zero during idle periods is expected; monotonically growing indicates a stalled slot.\n\
                 # TYPE rustcdc_replication_slot_lag_bytes gauge\n\
                 rustcdc_replication_slot_lag_bytes{{source_type=\"{source_type}\"}} {}",
                lag_bytes
            )?;
        }

        writeln!(
            w,
            "# HELP rustcdc_runtime_source_capability Connector capability flags.\n\
             # TYPE rustcdc_runtime_source_capability gauge"
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
                "rustcdc_runtime_source_capability{{source_type=\"{source_type}\",capability=\"{name}\"}} {}",
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
