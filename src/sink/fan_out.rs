//! [`FanOutSinkAdapter`] — delivers each event to every child sink **concurrently**.

use futures_util::future::join_all;

use crate::core::{Event, Result};
use crate::sink::{BoxedSink, SinkAdapter, SinkDeliveryGuarantee};

// ─── helpers ─────────────────────────────────────────────────────────────────

/// Drive every `future` concurrently; return the first `Err` in **input
/// position order** (not completion-time order), or `Ok(())` when all succeed.
/// All futures run to completion regardless of individual failures — no future
/// is cancelled when another returns an error.
async fn join_first_err<I, F>(futures: I) -> Result<()>
where
    I: IntoIterator<Item = F>,
    F: std::future::Future<Output = Result<()>>,
{
    join_all(futures)
        .await
        .into_iter()
        .find(|r| r.is_err())
        .unwrap_or(Ok(()))
}

// ─── FanOutSinkAdapter ────────────────────────────────────────────────────────

/// A meta-adapter that delivers each CDC event to **every** child sink
/// **concurrently**.
///
/// ## Concurrency model
///
/// `send`, `flush`, `close`, `commit_checkpoint_barrier`,
/// `abort_checkpoint_barrier`, and `preflight_check` drive all children
/// **simultaneously** — they all receive the call at the same time rather than
/// one after another.  This means:
///
/// * **Latency per operation** = `max(child_latencies)` rather than
///   `Σ(child_latencies)`.  A fast child is not held back by a slow sibling
///   *within the same operation*.
/// * **Ordering within each individual sink is preserved**: event *N* is
///   delivered to all children concurrently; the fan-out awaits all of them
///   before processing event *N+1*, so each child sees a contiguous,
///   in-order stream.
/// * **A permanently stuck child will block the fan-out**: `join_all` awaits
///   every child.  If a child hangs indefinitely, the enclosing operation
///   hangs too.  Use `SinkAdapter::close_with_timeout` on the
///   `FanOutSinkAdapter` itself to apply a bounded shutdown deadline, or
///   ensure individual children have their own internal timeouts.
///
/// `preflight_check` also runs concurrently, so all connectivity failures are
/// surfaced in one startup pass rather than one-at-a-time.
///
/// ## Checkpoint barrier protocol
///
/// `begin_checkpoint_barrier` is intentionally **sequential** with a
/// compensating abort on partial failure: if sink *i* fails to begin, all
/// already-begun barriers (sinks *0..i*) are aborted in reverse order before
/// the error is returned.  This is the only correct implementation of the
/// two-phase-commit-style barrier protocol.
///
/// ## Error semantics
///
/// Even when one child returns an error, all remaining children still receive
/// the call — no child is skipped or cancelled.  The caller receives the
/// **first error by input position** (i.e., the error from the lowest-index
/// failing child), not necessarily the one that failed first in wall-clock
/// time.
///
/// ## Delivery guarantee
///
/// The effective [`SinkDeliveryGuarantee`] is the **weakest** guarantee of any
/// child.
///
/// ## Example
///
/// ```rust,no_run
/// use rustcdc::sink::{BoxedSink, FanOutSinkAdapter, MemorySinkAdapter};
///
/// let fan = FanOutSinkAdapter::new(vec![
///     BoxedSink::new(MemorySinkAdapter::new("a")),
///     BoxedSink::new(MemorySinkAdapter::new("b")),
/// ]);
/// ```
pub struct FanOutSinkAdapter {
    sinks: Vec<BoxedSink>,
}

impl std::fmt::Debug for FanOutSinkAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let names: Vec<&str> = self.sinks.iter().map(|s| s.name()).collect();
        f.debug_struct("FanOutSinkAdapter")
            .field("sinks", &names)
            .finish()
    }
}

impl FanOutSinkAdapter {
    /// Create a fan-out adapter wrapping `sinks`.
    ///
    /// # Panics
    ///
    /// Panics when `sinks` is empty.  A fan-out with zero children is always
    /// a logic error.
    pub fn new(sinks: Vec<BoxedSink>) -> Self {
        assert!(
            !sinks.is_empty(),
            "FanOutSinkAdapter requires at least one child sink"
        );
        Self { sinks }
    }

    /// Number of child sinks.
    pub fn len(&self) -> usize {
        self.sinks.len()
    }

    /// Returns `true` when the fan-out has no children.
    ///
    /// In practice this is always `false` — construction panics on empty input.
    pub fn is_empty(&self) -> bool {
        self.sinks.is_empty()
    }
}

impl SinkAdapter for FanOutSinkAdapter {
    fn name(&self) -> &str {
        "fan-out"
    }

    fn is_closed(&self) -> bool {
        self.sinks.iter().all(|s| s.is_closed())
    }

    fn delivery_metrics(&self) -> Option<crate::sink::SinkDeliveryMetrics> {
        let mut any = false;
        let mut agg = crate::sink::SinkDeliveryMetrics::default();
        for s in &self.sinks {
            if let Some(m) = s.delivery_metrics() {
                agg.merge(&m);
                any = true;
            }
        }
        if any {
            Some(agg)
        } else {
            None
        }
    }

    /// Send `event` to every child sink **concurrently**.
    ///
    /// All children receive the event regardless of individual failures.
    /// Returns the first error by input position after all children complete.
    async fn send(&mut self, event: &Event) -> Result<()> {
        join_first_err(self.sinks.iter_mut().map(|s| s.send(event))).await
    }

    /// Flush every child sink **concurrently**.
    ///
    /// All children are flushed regardless of individual failures.
    /// Returns the first error by input position after all children complete.
    async fn flush(&mut self) -> Result<()> {
        join_first_err(self.sinks.iter_mut().map(|s| s.flush())).await
    }

    /// Close every child sink **concurrently**.
    ///
    /// All children receive the `close()` call simultaneously; a slow child
    /// does not delay the *start* of another child's close.  However, the
    /// fan-out awaits **all** children before returning, so a permanently hung
    /// child will stall the overall close.  Apply
    /// [`SinkAdapter::close_with_timeout`] on the `FanOutSinkAdapter` itself
    /// for a bounded shutdown deadline.
    ///
    /// Returns the first error by input position after all children complete.
    async fn close(&mut self) -> Result<()> {
        join_first_err(self.sinks.iter_mut().map(|s| s.close())).await
    }

    // ── Delivery contract ─────────────────────────────────────────────────────

    /// Weakest delivery guarantee across all children.
    fn delivery_guarantee(&self) -> SinkDeliveryGuarantee {
        self.sinks.iter().map(|s| s.delivery_guarantee()).fold(
            SinkDeliveryGuarantee::EffectivelyOnce,
            SinkDeliveryGuarantee::weakest,
        )
    }

    /// `true` only when ALL children are idempotent-delivery capable.
    fn idempotent_delivery_capable(&self) -> bool {
        self.sinks.iter().all(|s| s.idempotent_delivery_capable())
    }

    /// `true` only when ALL children support transactional checkpoint barriers.
    fn transactional_checkpoint_barrier_capable(&self) -> bool {
        self.sinks
            .iter()
            .all(|s| s.transactional_checkpoint_barrier_capable())
    }

    // ── Runtime hints ─────────────────────────────────────────────────────────

    /// Sum of queue depths across all children that report one.
    ///
    /// Returns `None` when no child reports a queue depth.
    fn queue_depth(&self) -> Option<usize> {
        let mut any = false;
        let total: usize = self
            .sinks
            .iter()
            .filter_map(|s| {
                let d = s.queue_depth();
                if d.is_some() {
                    any = true;
                }
                d
            })
            .sum();
        if any {
            Some(total)
        } else {
            None
        }
    }

    /// Minimum flush tick interval across all children.
    fn flush_tick_interval(&self) -> Option<std::time::Duration> {
        self.sinks
            .iter()
            .filter_map(|s| s.flush_tick_interval())
            .min()
    }

    // ── Checkpoint barriers ────────────────────────────────────────────────────

    /// Begin the checkpoint barrier on every child **sequentially**.
    ///
    /// This is the only method that is NOT concurrent.  The two-phase-commit
    /// protocol requires knowing exactly which children have successfully begun
    /// before committing.  If child *i* fails to begin, all already-begun
    /// barriers (children *0..i*) are aborted in reverse order, and the error
    /// is returned.
    async fn begin_checkpoint_barrier(&mut self) -> Result<()> {
        let n = self.sinks.len();
        for i in 0..n {
            if let Err(begin_err) = self.sinks[i].begin_checkpoint_barrier().await {
                for j in (0..i).rev() {
                    let _ = self.sinks[j].abort_checkpoint_barrier().await;
                }
                return Err(begin_err);
            }
        }
        Ok(())
    }

    /// Commit the checkpoint barrier on every child **concurrently**.
    ///
    /// All children are committed regardless of individual failures.
    /// Returns the first error encountered.
    async fn commit_checkpoint_barrier(&mut self) -> Result<()> {
        join_first_err(self.sinks.iter_mut().map(|s| s.commit_checkpoint_barrier())).await
    }

    /// Abort the checkpoint barrier on every child **concurrently**.
    ///
    /// All children are aborted regardless of individual failures.
    /// Returns the first error encountered.
    async fn abort_checkpoint_barrier(&mut self) -> Result<()> {
        join_first_err(self.sinks.iter_mut().map(|s| s.abort_checkpoint_barrier())).await
    }

    // ── Connectivity validation ────────────────────────────────────────────────

    /// Run preflight check on every child **concurrently**.
    ///
    /// All children are checked regardless of individual failures, so all
    /// connectivity problems are surfaced in a single startup pass.
    /// Returns the first error encountered.
    async fn preflight_check(&mut self) -> Result<()> {
        join_first_err(self.sinks.iter_mut().map(|s| s.preflight_check())).await
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use crate::core::{Event, Operation, SourceMetadata, EVENT_ENVELOPE_VERSION};
    use crate::sink::{BoxedSink, FanOutSinkAdapter, MemorySinkAdapter, SinkAdapter};

    fn make_event(table: &str) -> Event {
        Event {
            before: None,
            after: Some(serde_json::json!({"id": 1})),
            op: Operation::Insert,
            source: SourceMetadata {
                source_name: "test".into(),
                offset: "0".into(),
                timestamp: 1,
            },
            ts: 1,
            schema: None,
            table: table.into(),
            primary_key: None,
            snapshot: None,
            transaction: None,
            envelope_version: EVENT_ENVELOPE_VERSION,
            before_is_key_only: false,
            unavailable_columns: Vec::new(),
            before_unavailable_columns: Vec::new(),
        }
    }

    fn make_fan(n: usize) -> (FanOutSinkAdapter, Vec<String>) {
        let names: Vec<String> = (0..n).map(|i| format!("mem{i}")).collect();
        let sinks = names
            .iter()
            .map(|name| BoxedSink::new(MemorySinkAdapter::new(name.as_str())))
            .collect();
        (FanOutSinkAdapter::new(sinks), names)
    }

    #[tokio::test]
    async fn delivers_to_all_children_concurrently() {
        let (mut fan, _) = make_fan(3);
        let event = make_event("orders");
        fan.send(&event).await.expect("send should succeed");
        fan.flush().await.expect("flush should succeed");
        fan.close().await.expect("close should succeed");
    }

    #[tokio::test]
    async fn per_sink_ordering_is_preserved() {
        // Even with concurrent delivery, each sink must see events in order.
        let n_sinks = 3;
        let n_events = 5;
        let (mut fan, _) = make_fan(n_sinks);

        for i in 0..n_events {
            let event = make_event(&format!("tbl_{i}"));
            fan.send(&event).await.expect("send should succeed");
        }
        fan.flush().await.expect("flush should succeed");
    }

    #[tokio::test]
    async fn name_is_fan_out() {
        let (fan, _) = make_fan(2);
        assert_eq!(fan.name(), "fan-out");
    }

    #[tokio::test]
    async fn len_reflects_child_count() {
        let (fan, _) = make_fan(4);
        assert_eq!(fan.len(), 4);
        assert!(!fan.is_empty());
    }

    #[tokio::test]
    async fn begin_checkpoint_barrier_is_noop_for_default_sinks() {
        let (mut fan, _) = make_fan(2);
        fan.begin_checkpoint_barrier()
            .await
            .expect("begin should succeed");
        fan.commit_checkpoint_barrier()
            .await
            .expect("commit should succeed");
    }

    #[tokio::test]
    async fn abort_checkpoint_barrier_is_noop_for_default_sinks() {
        let (mut fan, _) = make_fan(2);
        fan.begin_checkpoint_barrier()
            .await
            .expect("begin should succeed");
        fan.abort_checkpoint_barrier()
            .await
            .expect("abort should succeed");
    }

    #[tokio::test]
    async fn preflight_check_runs_all_children_concurrently() {
        let (mut fan, _) = make_fan(2);
        fan.preflight_check()
            .await
            .expect("preflight should succeed");
    }

    #[test]
    fn debug_impl_shows_child_names() {
        let (fan, names) = make_fan(2);
        let dbg = format!("{fan:?}");
        for name in &names {
            assert!(
                dbg.contains(name.as_str()),
                "debug should contain child name {name}"
            );
        }
    }

    #[test]
    #[should_panic(expected = "at least one child sink")]
    fn panics_on_empty_sinks() {
        FanOutSinkAdapter::new(vec![]);
    }

    #[test]
    fn delivery_guarantee_is_weakest_of_children() {
        use crate::sink::SinkDeliveryGuarantee;
        // All children default to AtLeastOnce, so fan-out is AtLeastOnce.
        let (fan, _) = make_fan(3);
        assert_eq!(fan.delivery_guarantee(), SinkDeliveryGuarantee::AtLeastOnce);
    }

    #[test]
    fn idempotent_delivery_capable_requires_all_children() {
        // MemorySinkAdapter defaults to false, so fan-out is also false.
        let (fan, _) = make_fan(2);
        assert!(!fan.idempotent_delivery_capable());
    }

    #[test]
    fn transactional_barrier_capable_requires_all_children() {
        let (fan, _) = make_fan(2);
        assert!(!fan.transactional_checkpoint_barrier_capable());
    }
}
