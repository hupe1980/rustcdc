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
    /// Snapshot additional tables on a **running** pipeline.
    ///
    /// The library equivalent of Debezium's `execute-snapshot` signal — and without its
    /// prerequisite: rustcdc needs no signal table in the source, so this works against a
    /// read-only role and a read replica. Returns the number of tables enqueued.
    ///
    /// Use it to backfill a table just added to the publication, to rebuild a downstream store,
    /// or to re-run history through a corrected transform. The live stream is never paused; the
    /// new tables are chunked into it exactly like the initially configured ones, under the same
    /// DBLog watermark suppression.
    ///
    /// # Semantics
    ///
    /// * A table **not currently tracked** is added and read from the start.
    /// * A table **already in progress** is a no-op, so retrying a request is safe.
    /// * A table **already complete** is rewound and read again.
    ///
    /// Every name is resolved against the catalog before anything is mutated, so one bad
    /// reference fails the whole call rather than half-applying it. Enqueued tables reach the
    /// checkpoint with the next commit and are picked up again after a restart, even though they
    /// are not in [`RuntimeConfig::with_incremental_snapshot`]'s static list.
    ///
    /// # Errors
    ///
    /// * [`Error::StateError`] if the runtime is not running.
    /// * [`Error::NotImplemented`] if this runtime was not configured with
    ///   [`RuntimeConfig::with_incremental_snapshot`] — there is no snapshot to add to, and
    ///   pretending otherwise would report a backfill that never happens.
    /// * [`Error::ConfigError`] if a table does not exist or has no primary key, which is
    ///   required to chunk it resumably.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use rustcdc::CdcRuntime;
    /// # async fn example(runtime: &mut CdcRuntime) -> rustcdc::Result<()> {
    /// let enqueued = runtime
    ///     .request_incremental_snapshot(vec!["public.orders".to_string()])
    ///     .await?;
    /// println!("{enqueued} table(s) queued for snapshotting");
    /// # Ok(())
    /// # }
    /// ```
    pub async fn request_incremental_snapshot(&mut self, tables: Vec<String>) -> Result<usize> {
        if self.state != RuntimeState::Running {
            return Err(Error::StateError(format!(
                "cannot request an incremental snapshot while the runtime is {}; start it first",
                self.state
            )));
        }

        let stream = self.stream.as_mut().ok_or_else(|| {
            Error::StateError(
                "cannot request an incremental snapshot: the runtime has no active stream".into(),
            )
        })?;

        let enqueued = stream.request_snapshot_tables(tables).await?;
        if enqueued > 0 {
            tracing::info!(
                target: "rustcdc::core::runtime",
                enqueued,
                "incremental snapshot requested for {enqueued} table(s) on a running pipeline",
            );
        }
        Ok(enqueued)
    }

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
            unmatched_transform_rules: self.transform_pipeline.unmatched_rules(),
            idempotency_evictions: self
                .idempotency_guard
                .as_ref()
                .map(|guard| guard.eviction_count()),
            idempotency_unidentifiable_passthrough: self
                .idempotency_guard
                .as_ref()
                .map(|guard| guard.unidentifiable_passthrough_count()),
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

        if let Some(evictions) = admin.idempotency_evictions {
            writeln!(
                w,
                "# HELP rustcdc_runtime_idempotency_evictions_total Fingerprints evicted because the idempotency window filled. Growing steadily means the window is too small for this deployment's replay distance, so older duplicates stop being suppressed; raise IdempotencyOptions::capacity.\n\
                 # TYPE rustcdc_runtime_idempotency_evictions_total counter\n\
                 rustcdc_runtime_idempotency_evictions_total{{source_type=\"{source_type}\"}} {evictions}"
            )?;
        }

        if let Some(passthrough) = admin.idempotency_unidentifiable_passthrough {
            writeln!(
                w,
                "# HELP rustcdc_runtime_idempotency_unidentifiable_total Events passed through undeduplicated because they carry neither transaction metadata nor a resolvable primary key. Expected for keyless tables; deduplicating them could drop distinct rows.\n\
                 # TYPE rustcdc_runtime_idempotency_unidentifiable_total counter\n\
                 rustcdc_runtime_idempotency_unidentifiable_total{{source_type=\"{source_type}\"}} {passthrough}"
            )?;
        }

        // A transform rule that has never matched is a *silent* misconfiguration: masking
        // fails open into clear text, routing fails open into the default destination.
        // Neither errors, so an accessor read at shutdown is the only other way to see it —
        // which means an operator has to go looking. As a gauge it becomes an alert rule.
        //
        // Emitted only when non-empty: a series per configured rule on every scrape would
        // add cardinality for the overwhelmingly common all-rules-firing case. The absent
        // series is the healthy state, so alert on presence.
        if !admin.unmatched_transform_rules.is_empty() {
            writeln!(
                w,
                "# HELP rustcdc_transform_rules_unmatched Configured transform rules that have never matched an event. A masking rule that never fires means a column is shipping in clear text; a routing rule that never fires means events are going to the default destination. Only meaningful after real traffic — every rule is unmatched before the first event.\n\
                 # TYPE rustcdc_transform_rules_unmatched gauge"
            )?;
            for rule in &admin.unmatched_transform_rules {
                writeln!(
                    w,
                    "rustcdc_transform_rules_unmatched{{source_type=\"{source_type}\",transform=\"{}\",kind=\"{}\",rule=\"{}\"}} 1",
                    escape_prometheus_label(&rule.transform),
                    escape_prometheus_label(&rule.kind),
                    escape_prometheus_label(&rule.rule),
                )?;
            }
        }

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

/// Escape a string for use as a Prometheus label value.
///
/// Every other label this module emits is a value the crate controls. Rule identifiers are
/// not: they are JSON paths, regexes and filter predicates an operator wrote, so a `"` or a
/// backslash in one would otherwise produce an exposition body a scraper rejects — taking
/// *every* metric on the endpoint down, not just this one.
///
/// Per the exposition format: backslash, double quote and newline are escaped. A carriage
/// return is escaped too — the format does not require it, but a raw `\r` mid-line makes
/// some scrapers truncate the sample, which is the same failure with a subtler symptom.
fn escape_prometheus_label(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        match character {
            '\\' => escaped.push_str("\\\\"),
            '"' => escaped.push_str("\\\""),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            other => escaped.push(other),
        }
    }
    escaped
}

#[cfg(test)]
mod escape_tests {
    use super::escape_prometheus_label;

    #[test]
    fn every_character_that_can_break_a_scrape_is_escaped() {
        assert_eq!(escape_prometheus_label(r#"a"b"#), r#"a\"b"#);
        assert_eq!(escape_prometheus_label(r"a\b"), r"a\\b");
        assert_eq!(escape_prometheus_label("a\nb"), r"a\nb");
        assert_eq!(escape_prometheus_label("a\rb"), r"a\rb");
        // Non-ASCII is legal in a label value and must pass through unchanged: a column
        // name is whatever the database allows, and mangling it would make the series
        // unmatchable.
        assert_eq!(escape_prometheus_label("café.☕"), "café.☕");
    }

    #[test]
    fn escaping_a_clean_value_changes_nothing() {
        assert_eq!(escape_prometheus_label("user.ssn"), "user.ssn");
    }
}
