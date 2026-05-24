//! Metrics and tracing abstractions used by the runtime.

use crate::core::{Error, Operation};

/// Abstract metrics collector for runtime and source instrumentation.
pub trait MetricsCollector: Send + Sync {
    /// Record the observed latency for a processed event.
    fn record_event_processed(&self, op: Operation, latency_ms: u64);
    /// Record checkpoint commit throughput and latency.
    fn record_checkpoint_committed(&self, event_count: u64, latency_ms: u64);
    /// Record replication lag in milliseconds and events.
    fn record_replication_lag_ms(&self, lag_ms: u64, lag_events: u64);
    /// Record a typed error with an execution context label.
    fn record_error(&self, error: &Error, context: &str);
}

/// Abstract event tracer for lifecycle hooks and barrier transitions.
pub trait EventTracer: Send + Sync {
    /// Trace the start of processing for a logical event.
    fn trace_event_start(&self, event_id: &str);
    /// Trace the end of processing for a logical event.
    fn trace_event_end(&self, event_id: &str, status: &str);
    /// Trace the current state of the checkpoint barrier.
    fn trace_checkpoint_barrier(&self, state: &str);
}

/// No-op metrics collector used by default in tests and skeleton deployments.
#[derive(Debug, Default)]
pub struct NoOpMetricsCollector;

impl MetricsCollector for NoOpMetricsCollector {
    fn record_event_processed(&self, _op: Operation, _latency_ms: u64) {}
    fn record_checkpoint_committed(&self, _event_count: u64, _latency_ms: u64) {}
    fn record_replication_lag_ms(&self, _lag_ms: u64, _lag_events: u64) {}
    fn record_error(&self, _error: &Error, _context: &str) {}
}

/// No-op tracer used by default in tests and skeleton deployments.
#[derive(Debug, Default)]
pub struct NoOpEventTracer;

impl EventTracer for NoOpEventTracer {
    fn trace_event_start(&self, _event_id: &str) {}
    fn trace_event_end(&self, _event_id: &str, _status: &str) {}
    fn trace_checkpoint_barrier(&self, _state: &str) {}
}

#[cfg(test)]
mod tests {
    use super::{EventTracer, MetricsCollector, NoOpEventTracer, NoOpMetricsCollector};
    use crate::core::{Error, Operation};

    #[test]
    fn noop_collectors_are_infallible() {
        let metrics = NoOpMetricsCollector;
        metrics.record_event_processed(Operation::Insert, 1);
        metrics.record_checkpoint_committed(10, 5);
        metrics.record_replication_lag_ms(42, 7);
        metrics.record_error(&Error::ConfigError("bad".into()), "test");

        let tracer = NoOpEventTracer;
        tracer.trace_event_start("e1");
        tracer.trace_event_end("e1", "ok");
        tracer.trace_checkpoint_barrier("open");
    }
}
