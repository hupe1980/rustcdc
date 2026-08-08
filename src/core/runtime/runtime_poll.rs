use super::*;

impl CdcRuntime {
    /// Re-open the source stream after a reconnect, preserving the capture mode.
    ///
    /// This exists because `start_stream` and `start_incremental_snapshot` return *different
    /// shapes of stream*, and a reconnect must reproduce the one `start()` chose. An
    /// incremental snapshot is delivered by a driver that **wraps** the log stream: it owns
    /// the per-table chunk cursors and reports them through
    /// [`StreamHandle::incremental_snapshot_state`], which is what puts them in every
    /// checkpoint record.
    ///
    /// Reconnecting with a plain `start_stream` therefore did two damaging things at once, and
    /// neither was visible:
    ///
    /// 1. The snapshot **stopped progressing**. The driver was gone, so no further chunk was
    ///    ever read and the snapshot never completed.
    /// 2. Worse, a plain stream reports no snapshot state, so every checkpoint written after
    ///    the reconnect **erased the progress record**. A later restart then found no snapshot
    ///    in flight at all — the un-read tables were neither resumed nor reported missing.
    ///
    /// Any transient network error during an incremental snapshot reached this path, and a
    /// snapshot of a large table is a long window. Mirroring `start()`'s choice is the whole
    /// fix; the resume offset already carries the cursors, so the rebuilt driver picks up
    /// exactly where the old one left off.
    async fn resume_stream_after_reconnect(
        &mut self,
        resume_from: Option<&dyn Offset>,
    ) -> Result<Box<dyn crate::source::StreamHandle>> {
        // Both futures are boxed rather than awaited inline. `poll_event_batch`'s state
        // machine already holds a lot across await points, and inlining
        // `start_incremental_snapshot` — which builds a backend, describes every table and
        // constructs the driver — pushed the composed future past the default 2 MiB test
        // thread stack and aborted with a stack overflow. Boxing moves each branch's state to
        // the heap, so the caller's future stays small.
        //
        // Cloned first so the `self.source` borrow below does not overlap `self.config`.
        match self.config.incremental_snapshot.clone() {
            Some(incremental) => {
                Box::pin(
                    self.source
                        .start_incremental_snapshot(incremental, resume_from),
                )
                .await
            }
            None => Box::pin(self.source.start_stream(resume_from)).await,
        }
    }

    /// Poll the next batch of events.
    ///
    /// Returns an **empty batch** when nothing is available within the poll budget —
    /// that is normal, and is not end of stream. A non-empty batch carries an
    /// [`AckToken`] that must be passed to [`CdcRuntime::commit_ack`] before the
    /// checkpoint advances.
    ///
    /// An unacknowledged batch is **redelivered** by the next call rather than skipped,
    /// so a consumer that drops one loses nothing.
    ///
    /// # Errors
    ///
    /// - [`Error::Backpressure`] when the commit barrier is full. This is flow control,
    ///   not a failure: acknowledge the outstanding batch and call again.
    /// - [`Error::StateError`] if the runtime is not running.
    /// - Source, transform and checkpoint errors, classified by
    ///   [`Error::kind`].
    pub async fn poll_event_batch(&mut self) -> Result<EventBatch> {
        if self.state != RuntimeState::Running {
            let error = Error::StateError("runtime is not running".into());
            self.record_runtime_error("runtime.poll.state", &error);
            return Err(error);
        }

        if let Some(batch) = self.current_pending_batch() {
            return Ok(batch);
        }

        let metrics = Arc::clone(&self.observability().metrics);

        // Retry a source confirmation that failed after a durable commit, BEFORE any
        // new events are polled and passed through the idempotency guard.
        //
        // Ordering is the whole point: the source is still replaying events the runtime
        // already committed, and the guard would suppress all of them, producing an
        // empty batch and a silent no-progress loop. Confirming first stops the replay
        // at the source, so there is nothing to suppress.
        self.retry_pending_confirmation().await?;

        if !self.buffered_events.is_empty() {
            return Ok(self.deliver_buffered_batch());
        }

        if !self.pending_source_events.is_empty() {
            let batch = self.flush_pending_source_events()?;
            if !batch.is_empty() {
                return Ok(batch);
            }
            // An empty batch here means every queued event was withheld to keep a
            // transaction whole under `PreserveTransactions`. Returning would re-cut the
            // same events on the next poll and never ask the source for more — the rest of
            // the transaction could never arrive, and the pipeline would wedge. Falling
            // through to the source is what lets it complete.
            tracing::debug!(
                target: "rustcdc::core::runtime",
                withheld = self.pending_source_events.len(),
                "withholding a partial transaction; polling the source for the remainder",
            );
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
            self.record_schema_change_events(&chunk).await?;
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
                self.record_schema_change_events(&chunk).await?;
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

        if self.stream.is_some() {
            let result = if let Some(policy) = self.config.options.connection_retry {
                let mut attempt: u32 = 0;
                let mut delay_ms = policy.initial_delay_ms;
                loop {
                    // If stream is None (reconnect failed on a previous attempt), try to
                    // reconnect before polling again.
                    if self.stream.is_none() {
                        if let Err(_elapsed) = tokio::time::timeout(
                            tokio::time::Duration::from_secs(30),
                            self.source.close(),
                        )
                        .await
                        {
                            tracing::warn!(
                                target: "rustcdc::core::runtime",
                                "source close timed out during reconnect; proceeding with reconnect regardless",
                            );
                        }
                        if let Err(connect_error) = self.source.connect().await {
                            tracing::warn!(
                                target: "rustcdc::core::runtime",
                                attempt = attempt + 1,
                                error = %connect_error.report(),
                                "source reconnect failed; will retry on next attempt",
                            );
                            metrics.record_error(&connect_error, "runtime.poll.stream_reconnect");
                            let exhausted = policy
                                .max_retries
                                .map(|max| attempt >= max)
                                .unwrap_or(false);
                            if exhausted {
                                // Not `SourceError`: that is classified `Transient`,
                                // so an embedder following the crate's own retry
                                // guidance would retry a failure whose entire meaning
                                // is "retrying has already been exhausted".
                                return Err(crate::core::Error::Unrecoverable(format!(
                                    "connection retries exhausted after {} attempt(s) during \
                                     reconnect; the configured ConnectionRetryPolicy has given \
                                     up. Check source connectivity and credentials, then \
                                     restart the runtime. Last error is in the preceding \
                                     warn-level log lines.",
                                    attempt + 1
                                ))
                                .context("resuming the stream after a source disconnect"));
                            }
                            tokio::time::sleep(tokio::time::Duration::from_millis(delay_ms)).await;
                            delay_ms = (delay_ms.saturating_mul(2)).min(policy.max_delay_ms);
                            attempt = attempt.saturating_add(1);
                            continue;
                        }
                        // A checkpoint-load *failure* must never be collapsed into
                        // "no checkpoint exists".  `start_stream(None)` resumes from the
                        // live head of the log (MySQL `SHOW MASTER STATUS`, SQL Server
                        // `fn_cdc_get_max_lsn`), so treating a transient checkpoint-store
                        // error as `None` silently skips every change written since the
                        // last durable checkpoint.  Fail loud instead: the checkpoint
                        // store being unreachable is exactly the condition under which we
                        // must not guess a resume position.
                        let resume_offset =
                            self.config.checkpoint.load().await.map_err(|error| {
                                crate::core::Error::CheckpointError(format!(
                                    "failed loading checkpoint while resuming the stream after \
                                 reconnect: {error}; refusing to restart the stream from the \
                                 live log head because that would silently skip all changes \
                                 since the last durable checkpoint"
                                ))
                            })?;
                        match self.resume_stream_after_reconnect(resume_offset.as_deref()).await {
                            Ok(new_stream) => {
                                self.stream = Some(new_stream);
                                tracing::info!(
                                    target: "rustcdc::core::runtime",
                                    "source reconnected; stream resumed from checkpoint",
                                );
                            }
                            Err(start_error) => {
                                tracing::warn!(
                                    target: "rustcdc::core::runtime",
                                    attempt = attempt + 1,
                                    error = %start_error.report(),
                                    "stream restart after reconnect failed; will retry",
                                );
                                metrics.record_error(&start_error, "runtime.poll.stream_reconnect");
                                let exhausted = policy
                                    .max_retries
                                    .map(|max| attempt >= max)
                                    .unwrap_or(false);
                                if exhausted {
                                    // See the equivalent guard above: classifying an
                                    // exhausted retry budget as retryable is a loop.
                                    return Err(crate::core::Error::Unrecoverable(format!(
                                        "stream restart retries exhausted after {} attempt(s); \
                                         the configured ConnectionRetryPolicy has given up. \
                                         Check source connectivity and credentials, then \
                                         restart the runtime.",
                                        attempt + 1
                                    ))
                                    .context(
                                        "restarting the stream after a recoverable source error",
                                    ));
                                }
                                tokio::time::sleep(tokio::time::Duration::from_millis(delay_ms))
                                    .await;
                                delay_ms = (delay_ms.saturating_mul(2)).min(policy.max_delay_ms);
                                attempt = attempt.saturating_add(1);
                                continue;
                            }
                        }
                    }
                    let stream = self.stream.as_mut().ok_or_else(|| {
                        crate::core::Error::SourceError(
                            "poll loop entered with no active stream".into(),
                        )
                    })?;
                    match stream
                        .next_events(self.config.options.max_poll_wait_ms)
                        .await
                    {
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
                                target: "rustcdc::core::runtime",
                                attempt = attempt + 1,
                                delay_ms,
                                error = %error.report(),
                                "recoverable source error; reconnecting and retrying stream poll",
                            );
                            metrics.record_error(&error, "runtime.poll.stream_retry");

                            // Drop the dead stream handle **before** backing off, not after.
                            // For a source that holds a server-side resource for the life of
                            // the stream — a PostgreSQL replication slot is held by its
                            // walsender until the socket closes — the backoff is exactly the
                            // window the server needs to release it. Sleeping first and then
                            // closing means the reconnect races the server's own cleanup and
                            // is refused ("replication slot is active for PID N"), burning an
                            // attempt on every retry.
                            //
                            // Resuming afterwards uses the last durable checkpoint offset, so
                            // at-least-once delivery is preserved with no data loss.
                            self.stream = None;
                            tokio::time::sleep(tokio::time::Duration::from_millis(delay_ms)).await;
                            if let Err(_elapsed) = tokio::time::timeout(
                                tokio::time::Duration::from_secs(30),
                                self.source.close(),
                            )
                            .await
                            {
                                tracing::warn!(
                                    target: "rustcdc::core::runtime",
                                    "source close timed out during retry reconnect; proceeding with reconnect regardless",
                                );
                            }
                            if let Err(connect_error) = self.source.connect().await {
                                tracing::warn!(
                                    target: "rustcdc::core::runtime",
                                    attempt = attempt + 1,
                                    error = %connect_error.report(),
                                    "source reconnect failed; will retry on next attempt",
                                );
                                metrics
                                    .record_error(&connect_error, "runtime.poll.stream_reconnect");
                            } else {
                                // See the equivalent guard above: a checkpoint-load failure
                                // must never be collapsed into "no checkpoint exists",
                                // because `start_stream(None)` resumes from the live log
                                // head and silently skips everything since the last
                                // durable checkpoint.
                                let resume_offset =
                                    self.config.checkpoint.load().await.map_err(|error| {
                                        crate::core::Error::CheckpointError(format!(
                                            "failed loading checkpoint while resuming the stream \
                                             after a recoverable source error: {error}; refusing \
                                             to restart the stream from the live log head because \
                                             that would silently skip all changes since the last \
                                             durable checkpoint"
                                        ))
                                    })?;
                                match self
                                    .resume_stream_after_reconnect(resume_offset.as_deref())
                                    .await
                                {
                                    Ok(new_stream) => {
                                        self.stream = Some(new_stream);
                                        tracing::info!(
                                            target: "rustcdc::core::runtime",
                                            attempt = attempt + 1,
                                            "source reconnected; stream resumed from checkpoint",
                                        );
                                    }
                                    Err(start_error) => {
                                        tracing::warn!(
                                            target: "rustcdc::core::runtime",
                                            attempt = attempt + 1,
                                            error = %start_error.report(),
                                            "stream restart after reconnect failed; will retry",
                                        );
                                        metrics.record_error(
                                            &start_error,
                                            "runtime.poll.stream_reconnect",
                                        );
                                    }
                                }
                            }

                            delay_ms = (delay_ms.saturating_mul(2)).min(policy.max_delay_ms);
                            attempt = attempt.saturating_add(1);
                        }
                        Err(error) => break Err(error),
                    }
                }
            } else {
                self.stream
                    .as_mut()
                    .ok_or_else(|| {
                        crate::core::Error::SourceError(
                            "poll loop entered with no active stream".into(),
                        )
                    })?
                    .next_events(self.config.options.max_poll_wait_ms)
                    .await
            };
            let events = result
                .inspect_err(|error| metrics.record_error(error, "runtime.poll.stream_events"))?;
            if events.is_empty() {
                return Ok(EventBatch::empty());
            }
            // Deduplicate source events before transform stages mutate payloads.
            // Record any schema changes durably BEFORE the events announcing them are
            // enqueued, so a consumer can never see a schema change the history lacks.
            self.record_schema_change_events(&events).await?;
            let deduplicated = self.filter_idempotent_events(events)?;
            let transformed = self.apply_transforms(deduplicated).await?;
            self.enqueue_pending_source_events(transformed);
            return self.flush_pending_source_events();
        }

        Ok(EventBatch::empty())
    }

    /// Expose the runtime as a batch stream that yields non-empty deliveries.
    ///
    /// Empty polls are absorbed rather than yielded, so the stream only produces
    /// batches a consumer can act on.
    pub fn event_batches(&mut self) -> BoxStream<'_, Result<EventBatch>> {
        stream::unfold(self, |runtime| async move {
            loop {
                match runtime.poll_event_batch().await {
                    Ok(batch) if batch.is_empty() => {
                        // Yield to the scheduler before looping.
                        //
                        // A source that returns empty *synchronously* — a disabled
                        // source, or any handle that does not honour
                        // `max_poll_wait_ms` — turns this into an async fn that never
                        // awaits, which starves its tokio worker thread and can wedge
                        // an entire single-threaded runtime. `yield_now` costs nothing
                        // on the normal path, where the source poll already awaited.
                        tokio::task::yield_now().await;
                        continue;
                    }
                    Ok(batch) => return Some((Ok(batch), runtime)),
                    Err(error) => return Some((Err(error), runtime)),
                }
            }
        })
        .boxed()
    }

    pub(super) async fn apply_transforms(&mut self, events: Vec<Event>) -> Result<Vec<Event>> {
        if self.transform_pipeline.is_empty() || events.is_empty() {
            return Ok(events);
        }

        // Fast path: run the whole batch through each stage in turn.
        //
        // The pipeline used to be driven one event at a time, and because the trait was
        // `async`, `#[async_trait]` boxed a future per stage per event — O(events × stages)
        // heap allocations on the hottest path in the library, to await work that never
        // yields. Batching also lets a stage amortise per-batch setup, and lets the WASM
        // stage take its instance lock once instead of `batch.len()` times.
        //
        // `Halt` is the default, and under it a failure aborts the batch anyway, so there
        // is nothing to attribute per event.
        if self.config.options.transform_error_policy == TransformErrorPolicy::Halt {
            let mut batch = events;
            return match self.transform_pipeline.apply_batch(&mut batch).await {
                Ok(()) => Ok(batch),
                Err(error) => {
                    self.record_runtime_error("runtime.transform.halt", &error);
                    Err(error)
                }
            };
        }

        // `Skip` needs per-event attribution: which event failed, so it can be counted,
        // logged with its offset, and handed to the dead-letter handler. That is
        // inherently per-event work, and `Skip` already clones each event for the DLQ, so
        // there is no fast path to lose here.
        let has_dlq = self.config.options.dead_letter_handler.is_some();
        let mut out = Vec::with_capacity(events.len());
        for event in events {
            let table = event.table.clone();
            let offset = event.source.offset.clone();
            // Only preserve a DLQ copy when a handler is configured — avoids a
            // full Event clone (including before/after JSON Values) on the common path.
            let dlq_copy = has_dlq.then(|| event.clone());
            match self.transform_pipeline.apply(event).await {
                Ok(Some(event)) => out.push(event),
                Ok(None) => {}
                Err(error) => {
                    // The checkpoint will advance past this event, so it is
                    // unrecoverable. Count it — this is the metric the docs
                    // promised and the crate never emitted.
                    self.total_events_skipped = self.total_events_skipped.saturating_add(1);
                    self.record_runtime_error("runtime.transform.skip", &error);
                    tracing::warn!(
                        target: "rustcdc::core::runtime",
                        table = %table,
                        offset = %offset,
                        error = %error.report(),
                        "runtime transform error; skipping event",
                    );
                    if let Some((handler, original)) = self
                        .config
                        .options
                        .dead_letter_handler
                        .as_ref()
                        .zip(dlq_copy)
                    {
                        handler(original, error);
                    }
                    continue;
                }
            }
        }
        Ok(out)
    }

    /// Retry a source confirmation that failed after a durable checkpoint commit.
    ///
    /// Succeeds silently when there is nothing pending. When the retry itself fails
    /// the LSN is retained and the caller continues — a single transient failure must
    /// not take the pipeline down. Escalation to a hard error happens in
    /// [`Self::filter_idempotent_events`], once the failure has demonstrably produced
    /// a no-progress loop.
    async fn retry_pending_confirmation(&mut self) -> Result<()> {
        let Some(lsn) = self.pending_confirmation_lsn else {
            return Ok(());
        };
        let Some(stream) = self.stream.as_mut() else {
            return Ok(());
        };

        match stream.confirm_lsn(lsn).await {
            Ok(()) => {
                tracing::info!(
                    target: "rustcdc::core::runtime",
                    lsn,
                    "runtime confirmed a previously failed source position; \
                     replay of already-committed events will stop",
                );
                self.pending_confirmation_lsn = None;
                self.unconfirmed_stall_polls = 0;
            }
            Err(error) => {
                self.record_runtime_error("runtime.poll.confirm_lsn_retry", &error);
                tracing::warn!(
                    target: "rustcdc::core::runtime",
                    lsn,
                    error = %error.report(),
                    "runtime could not confirm a durably committed source position; \
                     the source will keep replaying committed events",
                );
            }
        }
        Ok(())
    }

    fn filter_idempotent_events(&mut self, events: Vec<Event>) -> Result<Vec<Event>> {
        let Some(guard) = self.idempotency_guard.as_mut() else {
            return Ok(events);
        };

        let input_len = events.len();
        let mut out = Vec::with_capacity(input_len);
        for event in events {
            if guard.should_process(&event)? {
                out.push(event);
            } else {
                self.total_events_deduplicated = self.total_events_deduplicated.saturating_add(1);
            }
        }

        // Detect the no-progress loop: the source delivered events, the guard
        // suppressed every one of them, and a durably committed position remains
        // unconfirmed. Left alone this returns empty batches forever while every
        // health signal reports green, and on PostgreSQL the slot pins WAL on the
        // primary until the disk fills.
        if input_len > 0 && out.is_empty() && self.pending_confirmation_lsn.is_some() {
            self.unconfirmed_stall_polls = self.unconfirmed_stall_polls.saturating_add(1);
            if self.unconfirmed_stall_polls >= UNCONFIRMED_STALL_POLL_LIMIT {
                let lsn = self.pending_confirmation_lsn.unwrap_or_default();
                let error = Error::Unrecoverable(format!(
                    "runtime is not making progress: source position {lsn} was durably \
                     checkpointed but could not be confirmed to the source, so the source \
                     keeps replaying already-committed events and the idempotency guard \
                     suppresses all of them. {} consecutive polls produced no deliverable \
                     events. Operator action required: restore connectivity to the source \
                     so the position can be confirmed (for PostgreSQL this is \
                     pg_replication_slot_advance — check the slot still exists and the \
                     role retains REPLICATION). Until then the source retains its log \
                     (on PostgreSQL, WAL on the primary).",
                    self.unconfirmed_stall_polls
                ));
                self.record_runtime_error("runtime.poll.unconfirmed_stall", &error);
                return Err(error);
            }
        } else if !out.is_empty() {
            self.unconfirmed_stall_polls = 0;
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
            // Backpressure is flow control, not a failure. Classifying it as
            // `StateError` made it `ErrorKind::Terminal` — documented as "a permanent
            // problem that retrying will not resolve" — so an embedder following the
            // crate's own retry guidance would shut down on routine buffer pressure.
            let error = Error::Backpressure(
                "runtime commit barrier is full; acknowledge the outstanding batch with \
                 commit_ack() before polling again. This is normal flow control: the same \
                 poll succeeds once in-flight events are committed."
                    .into(),
            );
            self.record_runtime_error("runtime.poll.buffer_full", &error);
            return Err(error);
        }

        // Cut the batch on the byte budget as well as the event count.
        //
        // `max_event_bytes` was previously declared, defaulted, settable and documented
        // as a flush limit — and never read. An operator setting it to protect a
        // downstream with a hard message-size limit got no protection and no warning.
        let max_bytes = self.config.options.max_event_bytes;
        let mut chunk = Vec::with_capacity(available.min(self.pending_source_events.len()));
        let mut chunk_bytes = 0usize;

        while chunk.len() < available {
            let Some(event) = self.pending_source_events.pop_front() else {
                break;
            };

            if let Some(limit) = max_bytes {
                let event_bytes = estimate_event_bytes(&event);
                // Always deliver at least one event, even if it alone exceeds the
                // budget. Refusing would stall the pipeline permanently on a single
                // oversized row with no way for the caller to make progress; the batch
                // simply ends up over budget and the caller can see why.
                if !chunk.is_empty() && chunk_bytes.saturating_add(event_bytes) > limit {
                    self.pending_source_events.push_front(event);
                    break;
                }
                chunk_bytes = chunk_bytes.saturating_add(event_bytes);
            }

            chunk.push(event);
        }

        self.trim_to_transaction_boundary(&mut chunk);
        self.buffer_and_deliver(chunk)
    }

    /// Withhold a trailing partial transaction from `chunk` under
    /// [`TransactionBoundaryPolicy::PreserveTransactions`].
    ///
    /// A batch is cut on buffer capacity, byte budget, or simply on what the source has
    /// delivered so far — none of which knows anything about transactions. This holds back
    /// the trailing run of events belonging to a transaction that is not yet known to have
    /// ended, so every delivered batch ends on a boundary.
    ///
    /// # Knowing that a transaction ended
    ///
    /// Two signals count as proof, and nothing else does:
    ///
    /// 1. The last event says so — `event_index + 1 == total_events`. Streaming decoders
    ///    usually cannot fill `total_events` in, so this is the weaker signal.
    /// 2. A later event belongs to a **different** transaction. Seeing the next
    ///    transaction begin is how a log-based source proves the previous one is complete.
    ///
    /// Absence of proof is not proof: an earlier version returned early when the queue
    /// behind the cut was empty, treating "I have not seen the rest yet" as "there is no
    /// rest". That made the guarantee hold only for cuts caused by buffer limits — a
    /// transaction spread across two polls, which is the *common* case for a streaming
    /// source, was delivered split anyway.
    ///
    /// # Why this cannot wedge
    ///
    /// Holding back could stall delivery if a transaction never completes. It is bounded
    /// by `max_buffer_size`: once a single unfinished transaction fills the batch, it is
    /// delivered split with a WARN naming the transaction. A permanent silent stall is
    /// strictly worse than the split this policy exists to avoid.
    pub(super) fn trim_to_transaction_boundary(&mut self, chunk: &mut Vec<Event>) {
        if self.config.options.transaction_boundary
            != TransactionBoundaryPolicy::PreserveTransactions
        {
            return;
        }

        // An event with no transaction metadata — a snapshot row, or a connector that does
        // not report boundaries — is its own boundary and is never withheld.
        let Some(last_tx) = chunk
            .last()
            .and_then(|event| event.transaction.as_ref())
            .map(|tx| tx.tx_id)
        else {
            return;
        };

        // Signal 1: the transaction declared its size and this is the final event.
        if chunk
            .last()
            .and_then(|event| event.transaction.as_ref())
            .is_some_and(|tx| {
                tx.total_events
                    .is_some_and(|total| tx.event_index.saturating_add(1) >= total)
            })
        {
            return;
        }

        // Signal 2: something already queued belongs to a different transaction, which
        // proves this one ended.
        let ended = self.pending_source_events.iter().any(|event| {
            event
                .transaction
                .as_ref()
                .is_none_or(|tx| tx.tx_id != last_tx)
        });
        if ended {
            return;
        }

        let same_tx = |event: &Event| {
            event
                .transaction
                .as_ref()
                .is_some_and(|tx| tx.tx_id == last_tx)
        };

        match chunk.iter().rposition(|event| !same_tx(event)) {
            Some(index) => {
                // Push the trailing partial transaction back, preserving order.
                for event in chunk.drain(index + 1..).rev() {
                    self.pending_source_events.push_front(event);
                }
            }
            None => {
                // The whole chunk is one transaction with no end in sight. Hold it back
                // and wait — unless it already fills the batch, in which case waiting
                // would be a permanent stall.
                if chunk.len() >= self.config.options.max_buffer_size {
                    tracing::warn!(
                        target: "rustcdc::core::runtime",
                        tx_id = last_tx,
                        max_buffer_size = self.config.options.max_buffer_size,
                        "transaction does not fit in one batch; delivering it split despite \
                         TransactionBoundaryPolicy::PreserveTransactions. Raise \
                         max_buffer_size above the largest transaction this source produces \
                         to restore the guarantee.",
                    );
                    return;
                }
                for event in chunk.drain(..).rev() {
                    self.pending_source_events.push_front(event);
                }
            }
        }
    }

    fn buffer_and_deliver(&mut self, events: Vec<Event>) -> Result<EventBatch> {
        for event in events {
            if self.config.options.validate_events {
                event.validate_or_error()?;
            }
            if event.snapshot.is_some() {
                // A snapshot row carries a chunk cursor, not a log position, so it has
                // no offset of its own. Ask the stream handle for one: during an
                // incremental snapshot that offset carries the chunk cursors, and
                // without it a restart re-reads every table from row zero. A bulk
                // snapshot handle returns `None`, and those rows stay non-persistent —
                // their progress is persisted by `SnapshotHandle::checkpoint` instead,
                // and a per-event offset here would clobber it.
                match self
                    .stream
                    .as_ref()
                    .filter(|_| self.snapshot.is_none())
                    .and_then(|stream| stream.position_offset())
                {
                    Some(offset) => self.commit_barrier.add_boxed_event(offset)?,
                    None => self.commit_barrier.add_non_persistent_event()?,
                }
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

        // Incremental-snapshot chunk cursors must become durable in the *same* write
        // as the stream position they were captured against — two separately written
        // records could disagree after a crash between them.
        let incremental_snapshot = self
            .stream
            .as_ref()
            .and_then(|stream| stream.incremental_snapshot_state());

        #[cfg(feature = "postgres")]
        if let RuntimeSourceConfig::Postgres(config) = &self.config.source {
            let lsn = parse_postgres_lsn(&event.source.offset)?;
            let slot_name = config.replication_slot_name.clone();
            let offset =
                PostgresOffset::new(lsn, slot_name).with_incremental_snapshot(incremental_snapshot);
            return Ok(GenericOffset::new(
                "postgres",
                offset
                    .encode()
                    .map_err(|error| Error::CheckpointError(error.to_string()))?,
            ));
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
            let (binlog_file, binlog_pos, gtid) = parse_mysql_stream_offset(&event.source.offset)?;
            // Carry the flavor so the checkpoint lands in the right file — a MariaDB
            // stream writing checkpoint_mysql.json finds nothing on restart and
            // silently resumes from the current binlog position.
            let flavor = self
                .config
                .source
                .source_type()
                .unwrap_or("mysql")
                .to_string();
            let offset = MysqlOffset::new(flavor, binlog_file, binlog_pos, gtid)
                .with_incremental_snapshot(incremental_snapshot);
            return Ok(GenericOffset::new(
                source_type.to_string(),
                offset
                    .encode()
                    .map_err(|error| Error::CheckpointError(error.to_string()))?,
            ));
        }

        #[cfg(feature = "sqlserver")]
        if matches!(&self.config.source, RuntimeSourceConfig::SqlServer(_)) {
            let offset = crate::checkpoint::SqlServerOffset::new(event.source.offset.clone())
                .with_incremental_snapshot(incremental_snapshot);
            return Ok(GenericOffset::new(
                "sqlserver",
                offset
                    .encode()
                    .map_err(|error| Error::CheckpointError(error.to_string()))?,
            ));
        }

        let _ = incremental_snapshot;
        // A source this runtime does not know: persist `source.offset` **verbatim**, which
        // is what the `Source` docs promise and what `Offset::encode` requires ("whatever
        // `encode` produces has to be decodable back into a resumable position by the
        // connector that wrote it").
        //
        // This used to JSON-encode the string, so a connector whose offset was `42` got
        // `"42"` back on restart — quotes and all. Anything parsing its own offset format
        // either failed or, worse, parsed the quoted form into a different position.
        Ok(GenericOffset::new(
            source_type.to_string(),
            event.source.offset.clone().into_bytes(),
        ))
    }

    fn current_pending_batch(&self) -> Option<EventBatch> {
        let pending = self.pending_delivery.as_ref()?;
        let uncommitted_len = pending.events.len() - pending.committed_prefix;
        // Share the buffer and carry an offset rather than copying the suffix — a
        // redelivered batch is re-derived on every poll until it is acknowledged.
        Some(EventBatch {
            events: Arc::clone(&pending.events),
            offset: pending.committed_prefix,
            ack_token: Some(AckToken {
                delivery_id: pending.delivery_id,
                event_count: uncommitted_len,
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
        // `event_trace_id` costs two String allocations per event; skip it entirely
        // when the tracer discards what it is given (the default).
        let tracing_enabled = self.observability().tracer.is_enabled();
        for event in &events {
            if tracing_enabled {
                self.observability()
                    .tracer
                    .trace_event_start(&Self::event_trace_id(event));
            }
            let source_ts = normalize_source_timestamp_ms(event.source.timestamp).min(now_ms);
            let latency_ms = now_ms.saturating_sub(source_ts);
            self.observability()
                .metrics
                .record_event_processed(event.op, latency_ms);
            // FR-3: Structured per-event trace including tx_id and WAL offset.
            // At tracing::trace level so production deployments are not flooded;
            // operators can enable it per-target for deep debugging.
            tracing::trace!(
                target: "rustcdc::core::runtime",
                table = %event.table,
                op = %event.op,
                offset = %event.source.offset,
                tx_id = event.transaction.as_ref().map(|tx| tx.tx_id),
                event_index = event.transaction.as_ref().map(|tx| tx.event_index),
                source_ts,
                latency_ms,
                "event delivered to caller",
            );
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
            // A fresh delivery starts at the head of its own buffer.
            offset: 0,
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
            // Flow control, not a failure — same reasoning as the commit-barrier
            // guard in `flush_pending_source_events`.
            return Err(Error::Backpressure(
                "runtime buffer is full; poll and acknowledge the buffered events before \
                 enqueuing more"
                    .into(),
            ));
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
                    self.config
                        .schema_history
                        .apply_retention(retention)
                        .await?;
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

    /// Record connector-emitted schema-change events into the durable schema history.
    ///
    /// The connectors synthesize `Operation::SchemaChange` events directly (PostgreSQL
    /// from a changed RELATION message, MySQL from a binlog QUERY event, SQL Server
    /// from a capture-instance metadata refresh) and hand them to the runtime like any
    /// other event. Before this hook existed, `record_ddl` had exactly one caller —
    /// `capture_ddl_statement` — which itself had no non-test callers, so the schema
    /// history was **never populated in any production path** while `start()`
    /// nonetheless hard-required a retention policy for it.
    ///
    /// This is the one place every connector's events converge, so recording here
    /// makes the history real for all of them at a single site. Ordering matters and
    /// is preserved: the DDL is durably recorded *before* the event that announces it
    /// is enqueued for delivery, so a consumer can never observe a schema change the
    /// history does not already contain.
    async fn record_schema_change_events(&mut self, events: &[Event]) -> Result<()> {
        for event in events.iter().filter(|event| event.op.is_schema_change()) {
            let Some(after) = event.after.as_ref() else {
                continue;
            };
            let Some(captured) = crate::ddl_capture::CapturedDdl::from_event_payload(after) else {
                // Not a shape we can turn into a schema-history entry (for example a
                // synthetic relation-change event with no parseable statement). The
                // event still reaches the consumer; only the history entry is skipped.
                continue;
            };
            let Some(schema_event) = captured.to_schema_event() else {
                continue;
            };

            self.config.schema_history.record_ddl(schema_event).await?;
            if let Some(retention) = self.config.options.schema_history_retention {
                self.config
                    .schema_history
                    .apply_retention(retention)
                    .await?;
            }
        }
        Ok(())
    }

    /// Returns an async stream of [`EventBatch`] values that stops when `token` is cancelled.
    ///
    /// Each item yielded from the stream must be acknowledged via [`CdcRuntime::commit_ack`]
    /// before the next poll to preserve at-least-once delivery guarantees.
    ///
    /// The stream terminates with the cancellation signal; no error is emitted for normal
    /// cancellation. The runtime remains in `Running` state after the stream ends — call
    /// [`CdcRuntime::stop`] or [`CdcRuntime::drain_and_stop`] to shut down cleanly.
    pub fn event_batches_cancellable(
        &mut self,
        token: tokio_util::sync::CancellationToken,
    ) -> impl futures_util::Stream<Item = Result<EventBatch>> + '_ {
        futures_util::stream::unfold((self, token), |(runtime, token)| async move {
            tokio::select! {
                biased;
                _ = token.cancelled() => None,
                result = runtime.poll_event_batch() => Some((result, (runtime, token))),
            }
        })
    }
}

/// Approximate serialized size of an event, for `max_event_bytes` accounting.
///
/// Uses the JSON payload sizes plus the fixed-ish envelope overhead rather than
/// serializing the whole event: this runs per event on the poll path, and an exact
/// figure would mean paying a full `serde_json::to_vec` for a number used only to
/// decide where to cut the batch. Overestimating slightly is the safe direction for a
/// downstream size limit.
fn estimate_event_bytes(event: &Event) -> usize {
    fn payload_len(value: Option<&serde_json::Value>) -> usize {
        value.map_or(0, |value| match value {
            serde_json::Value::Null => 4,
            serde_json::Value::Bool(_) => 5,
            serde_json::Value::Number(n) => n.to_string().len(),
            serde_json::Value::String(s) => s.len() + 2,
            // Objects and arrays dominate real payloads; serialize only these.
            other => serde_json::to_string(other).map_or(0, |s| s.len()),
        })
    }

    payload_len(event.before.as_ref())
        + payload_len(event.after.as_ref())
        + event.table.len()
        + event.schema.as_deref().map_or(0, str::len)
        + event.source.source_name.len()
        + event.source.offset.len()
        + event
            .primary_key
            .as_deref()
            .map_or(0, |keys| keys.iter().map(String::len).sum::<usize>())
        // Envelope scaffolding: field names, quoting, separators, timestamps.
        + 128
}
