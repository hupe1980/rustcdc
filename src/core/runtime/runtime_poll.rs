use super::*;

impl<C, H> CdcRuntime<C, H>
where
    C: crate::checkpoint::Checkpoint + Send + Sync + 'static,
    H: SchemaHistory + Send + Sync + 'static,
{
    /// Poll the next event batch with an opaque acknowledgement token.
    pub async fn poll_event_batch(&mut self) -> Result<EventBatch> {
        if self.state != RuntimeState::Running {
            let error = Error::StateError("runtime must be running before polling".into());
            self.record_runtime_error("runtime.poll.state", &error);
            return Err(error);
        }

        if let Some(batch) = self.current_pending_batch() {
            return Ok(batch);
        }

        let metrics = Arc::clone(&self.observability().metrics);

        if !self.buffered_events.is_empty() {
            return Ok(self.deliver_buffered_batch());
        }

        if !self.pending_source_events.is_empty() {
            return self.flush_pending_source_events();
        }

        if !self.injected_events.is_empty() {
            let mut chunk = Vec::new();
            while chunk.len() < self.config.options.max_buffer_size {
                let Some(event) = self.injected_events.pop_front() else {
                    break;
                };
                chunk.push(event);
            }

            // Deduplicate source events before transform stages mutate payloads.
            let deduplicated = self.filter_idempotent_events(chunk)?;
            let transformed = self.apply_transforms(deduplicated).await?;
            self.enqueue_pending_source_events(transformed);
            return self.flush_pending_source_events();
        }

        if let Some(snapshot) = self.snapshot.as_mut() {
            let chunk = snapshot
                .next_chunk(self.config.options.max_buffer_size)
                .await
                .inspect_err(|error| metrics.record_error(error, "runtime.poll.snapshot_chunk"))?;
            if !chunk.is_empty() {
                // Deduplicate source events before transform stages mutate payloads.
                let deduplicated = self.filter_idempotent_events(chunk)?;
                let transformed = self.apply_transforms(deduplicated).await?;
                self.enqueue_pending_source_events(transformed);
                return self.flush_pending_source_events();
            }

            if !self.handoff_complete {
                let stream = self.stream.as_mut().ok_or_else(|| {
                    Error::StateError("snapshot-to-stream handoff requires active stream".into())
                })?;
                self.source
                    .perform_handoff(snapshot.as_mut(), stream.as_mut())
                    .await
                    .inspect_err(|error| metrics.record_error(error, "runtime.poll.handoff"))?;
                self.handoff_complete = true;
            }
            self.snapshot = None;
        }

        if let Some(stream) = self.stream.as_mut() {
            let result = if let Some(policy) = self.config.options.connection_retry {
                let mut attempt: u32 = 0;
                let mut delay_ms = policy.initial_delay_ms;
                loop {
                    match stream.next_events(self.config.options.max_poll_wait_ms).await {
                        Ok(events) => break Ok(events),
                        Err(error) if error.is_recoverable() => {
                            let exhausted = policy
                                .max_retries
                                .map(|max| attempt >= max)
                                .unwrap_or(false);
                            if exhausted {
                                break Err(error);
                            }
                            tracing::warn!(
                                target: "cdc_rs::core::runtime",
                                attempt = attempt + 1,
                                delay_ms,
                                error = %error,
                                "recoverable source error; retrying stream poll",
                            );
                            metrics.record_error(&error, "runtime.poll.stream_retry");
                            tokio::time::sleep(tokio::time::Duration::from_millis(delay_ms))
                                .await;
                            delay_ms = (delay_ms.saturating_mul(2)).min(policy.max_delay_ms);
                            attempt = attempt.saturating_add(1);
                        }
                        Err(error) => break Err(error),
                    }
                }
            } else {
                stream
                    .next_events(self.config.options.max_poll_wait_ms)
                    .await
            };
            let events = result
                .inspect_err(|error| metrics.record_error(error, "runtime.poll.stream_events"))?;
            if events.is_empty() {
                return Ok(EventBatch::empty());
            }
            // Deduplicate source events before transform stages mutate payloads.
            let deduplicated = self.filter_idempotent_events(events)?;
            let transformed = self.apply_transforms(deduplicated).await?;
            self.enqueue_pending_source_events(transformed);
            return self.flush_pending_source_events();
        }

        Ok(EventBatch::empty())
    }

    /// Expose the runtime as a batch stream that yields non-empty deliveries.
    pub fn event_batches(&mut self) -> BoxStream<'_, Result<EventBatch>> {
        stream::unfold(self, |runtime| async move {
            loop {
                match runtime.poll_event_batch().await {
                    Ok(batch) if batch.is_empty() => continue,
                    Ok(batch) => return Some((Ok(batch), runtime)),
                    Err(error) => return Some((Err(error), runtime)),
                }
            }
        })
        .boxed()
    }

    pub(super) async fn apply_transforms(&self, events: Vec<Event>) -> Result<Vec<Event>> {
        let mut out = Vec::with_capacity(events.len());
        for event in events {
            let table = event.table.clone();
            let offset = event.source.offset.clone();
            match self.transform_pipeline.apply(event).await {
                Ok(Some(event)) => out.push(event),
                Ok(None) => {}
                Err(error) => match self.config.options.transform_error_policy {
                    TransformErrorPolicy::Halt => {
                        self.record_runtime_error("runtime.transform.halt", &error);
                        return Err(error);
                    }
                    TransformErrorPolicy::Skip => {
                        self.record_runtime_error("runtime.transform.skip", &error);
                        tracing::warn!(
                            target: "cdc_rs::core::runtime",
                            table = %table,
                            offset = %offset,
                            error = %error,
                            "runtime transform error; skipping event",
                        );
                        continue;
                    }
                },
            }
        }
        Ok(out)
    }

    fn filter_idempotent_events(&mut self, events: Vec<Event>) -> Result<Vec<Event>> {
        let Some(guard) = self.idempotency_guard.as_mut() else {
            return Ok(events);
        };

        let mut out = Vec::with_capacity(events.len());
        for event in events {
            if guard.should_process(&event)? {
                out.push(event);
            } else {
                self.total_events_deduplicated = self.total_events_deduplicated.saturating_add(1);
            }
        }

        Ok(out)
    }

    fn enqueue_pending_source_events(&mut self, events: Vec<Event>) {
        self.pending_source_events.extend(events);
    }

    fn flush_pending_source_events(&mut self) -> Result<EventBatch> {
        if self.pending_source_events.is_empty() {
            return Ok(EventBatch::empty());
        }

        let available = self
            .config
            .options
            .max_buffer_size
            .saturating_sub(self.commit_barrier.pending_count());

        if available == 0 {
            let error = Error::StateError(
                "runtime commit barrier is full; commit acknowledgements before polling more events"
                    .into(),
            );
            self.record_runtime_error("runtime.poll.buffer_full", &error);
            return Err(error);
        }

        let mut chunk = Vec::with_capacity(available.min(self.pending_source_events.len()));
        while chunk.len() < available {
            let Some(event) = self.pending_source_events.pop_front() else {
                break;
            };
            chunk.push(event);
        }

        self.buffer_and_deliver(chunk)
    }

    fn buffer_and_deliver(&mut self, events: Vec<Event>) -> Result<EventBatch> {
        for event in events {
            if self.config.options.validate_events {
                event.validate_or_error()?;
            }
            if event.snapshot.is_some() {
                // Snapshot checkpoints are persisted via SnapshotHandle::checkpoint
                // using connector-native structured state; avoid clobbering them
                // with per-event offsets at commit barrier flush time.
                self.commit_barrier.add_non_persistent_event()?;
            } else {
                let offset = self.build_checkpoint_offset(&event)?;
                self.commit_barrier.add_event(offset)?;
            }
            self.buffered_events.push_back(event);
        }
        Ok(self.deliver_buffered_batch())
    }

    fn build_checkpoint_offset(&self, event: &Event) -> Result<GenericOffset> {
        let source_type = self
            .config
            .source
            .source_type()
            .unwrap_or(event.source.source_name.as_str());

        #[cfg(feature = "postgres")]
        if let RuntimeSourceConfig::Postgres(config) = &self.config.source {
            let lsn = parse_postgres_lsn(&event.source.offset)?;
            let slot_name = config.replication_slot_name.clone();
            let offset = PostgresOffset { lsn, slot_name };
            return Ok(GenericOffset::new(
                "postgres",
                offset
                    .encode()
                    .map_err(|error| Error::CheckpointError(error.to_string()))?,
            ));
        }

        #[cfg(feature = "mysql")]
        if matches!(&self.config.source, RuntimeSourceConfig::Mysql(_)) {
            let (binlog_file, binlog_pos, gtid) = parse_mysql_stream_offset(&event.source.offset)?;
            let offset = MysqlOffset {
                gtid,
                binlog_file,
                binlog_pos,
            };
            return Ok(GenericOffset::new(
                "mysql",
                offset
                    .encode()
                    .map_err(|error| Error::CheckpointError(error.to_string()))?,
            ));
        }

        Ok(GenericOffset::new(
            source_type.to_string(),
            serde_json::to_vec(&event.source.offset)
                .map_err(|error| Error::SerializationError(error.to_string()))?,
        ))
    }

    fn current_pending_batch(&self) -> Option<EventBatch> {
        let pending = self.pending_delivery.as_ref()?;
        let uncommitted = &pending.events[pending.committed_prefix..];
        Some(EventBatch {
            events: Arc::new(uncommitted.to_vec()),
            ack_token: Some(AckToken {
                delivery_id: pending.delivery_id,
                event_count: uncommitted.len(),
            }),
        })
    }

    fn deliver_buffered_batch(&mut self) -> EventBatch {
        let mut events = Vec::new();
        while events.len() < self.config.options.max_buffer_size {
            let Some(event) = self.buffered_events.pop_front() else {
                break;
            };
            events.push(event);
        }

        if events.is_empty() {
            return EventBatch::empty();
        }

        let now_ms = now_millis();
        self.total_events_polled = self.total_events_polled.saturating_add(events.len() as u64);
        self.last_poll_at_ms = Some(now_ms);
        for event in &events {
            self.observability()
                .tracer
                .trace_event_start(&Self::event_trace_id(event));
            let source_ts = normalize_source_timestamp_ms(event.source.timestamp).min(now_ms);
            let latency_ms = now_ms.saturating_sub(source_ts);
            self.observability()
                .metrics
                .record_event_processed(event.op, latency_ms);
        }
        if let Some(latest_source_ts) = events
            .iter()
            .map(|event| normalize_source_timestamp_ms(event.source.timestamp))
            .max()
        {
            self.last_source_event_ts_ms = Some(
                self.last_source_event_ts_ms
                    .map_or(latest_source_ts, |previous| previous.max(latest_source_ts)),
            );
        }
        self.record_replication_lag_metric();

        let delivery_id = self.next_delivery_id;
        self.next_delivery_id = self.next_delivery_id.saturating_add(1);
        self.delivered_not_committed = self.delivered_not_committed.saturating_add(events.len());
        let event_count = events.len();
        let events = Arc::new(events);
        self.pending_delivery = Some(PendingDelivery {
            delivery_id,
            events: Arc::clone(&events),
            committed_prefix: 0,
        });

        EventBatch {
            events,
            ack_token: Some(AckToken {
                delivery_id,
                event_count,
            }),
        }
    }

    /// Inject a test event directly into the runtime buffer.
    pub fn enqueue_event(&mut self, event: Event) -> Result<()> {
        let queued_events = self.buffered_events.len() + self.injected_events.len();
        if queued_events >= self.config.options.max_buffer_size {
            return Err(Error::StateError("runtime buffer is full".into()));
        }

        self.injected_events.push_back(event);
        Ok(())
    }

    /// Parse and persist a DDL statement, then emit a canonical `schema_change` event.
    ///
    /// Returns `Ok(None)` when the statement is not a supported DDL command.
    pub async fn capture_ddl_statement(
        &mut self,
        dialect: DdlDialect,
        statement: &str,
        source_name: &str,
        offset: String,
        ts_ms: u64,
    ) -> Result<Option<Event>> {
        let Some(parsed) = parse_ddl_statement(dialect, statement) else {
            return Ok(None);
        };

        let mut captured = parsed.into_captured();
        captured.ts = ts_ms;

        let schema_version = match captured.to_schema_event() {
            Some(schema_event) => {
                let version = self.config.schema_history.record_ddl(schema_event).await?;
                if let Some(retention) = self.config.options.schema_history_retention {
                    self.config.schema_history.apply_retention(retention).await?;
                }
                Some(version)
            }
            None => None,
        };

        let mut event = captured.to_event(source_name, offset, ts_ms);
        if let Some(version) = schema_version {
            if let Some(after) = event.after.as_mut().and_then(|value| value.as_object_mut()) {
                after.insert("schema_version".into(), serde_json::json!(version));
            }
        }

        self.enqueue_event(event.clone())?;
        Ok(Some(event))
    }
}
