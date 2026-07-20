use std::sync::atomic::{AtomicU64, Ordering};

pub(crate) static KAFKA_TOPIC_STATE_CORRUPTION_DETECTED_TOTAL: AtomicU64 = AtomicU64::new(0);
pub(crate) static KAFKA_TOPIC_STATE_BOOTSTRAP_SEEDED_TOTAL: AtomicU64 = AtomicU64::new(0);
/// Total OpenDAL checkpoint / schema-history write failures since process start.
/// Incremented in `opendal_kv` whenever an OpenDAL `op.write(...)` call returns
/// an error, providing early visibility into remote state-backend degradation
/// without waiting for the next alert scrape cycle (CR-007).
pub(crate) static OPENDAL_STATE_WRITE_FAILURES_TOTAL: AtomicU64 = AtomicU64::new(0);

pub(crate) fn kafka_topic_state_corruption_detected_total() -> u64 {
    KAFKA_TOPIC_STATE_CORRUPTION_DETECTED_TOTAL.load(Ordering::Relaxed)
}

pub(crate) fn kafka_topic_state_bootstrap_seeded_total() -> u64 {
    KAFKA_TOPIC_STATE_BOOTSTRAP_SEEDED_TOTAL.load(Ordering::Relaxed)
}

pub(crate) fn opendal_state_write_failures_total() -> u64 {
    OPENDAL_STATE_WRITE_FAILURES_TOTAL.load(Ordering::Relaxed)
}

pub(crate) fn mark_corruption_detected() {
    KAFKA_TOPIC_STATE_CORRUPTION_DETECTED_TOTAL.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn mark_bootstrap_seeded() {
    KAFKA_TOPIC_STATE_BOOTSTRAP_SEEDED_TOTAL.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn mark_opendal_write_failure() {
    OPENDAL_STATE_WRITE_FAILURES_TOTAL.fetch_add(1, Ordering::Relaxed);
}
