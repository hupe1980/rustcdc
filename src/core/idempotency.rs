//! Consumer-side idempotency helpers for at-least-once delivery boundaries.

use std::collections::{HashMap, VecDeque};
use std::hash::{Hash, Hasher};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::core::{Error, Event, Result};

/// Sliding-window guard that suppresses duplicate event deliveries.
///
/// This helper is intended for sink-side consumers that need to absorb replay
/// without requiring exactly-once source semantics.
#[derive(Debug, Clone)]
pub struct EventIdempotencyGuard {
    capacity: usize,
    ttl_ms: Option<u64>,
    seen: HashMap<u64, u64>,
    order: VecDeque<(u64, u64)>,
}

impl EventIdempotencyGuard {
    /// Create a guard with a fixed in-memory fingerprint capacity.
    pub fn new(capacity: usize) -> Result<Self> {
        if capacity == 0 {
            return Err(Error::ConfigError(
                "idempotency guard capacity must be greater than zero".into(),
            ));
        }

        Ok(Self {
            capacity,
            ttl_ms: None,
            seen: HashMap::with_capacity(capacity),
            order: VecDeque::with_capacity(capacity),
        })
    }

    /// Configure an optional TTL for fingerprints.
    ///
    /// A TTL allows expected long-tail replays after retention windows while
    /// still suppressing immediate duplicates.
    pub fn with_ttl_ms(mut self, ttl_ms: u64) -> Result<Self> {
        if ttl_ms == 0 {
            return Err(Error::ConfigError(
                "idempotency guard ttl_ms must be greater than zero".into(),
            ));
        }
        self.ttl_ms = Some(ttl_ms);
        Ok(self)
    }

    /// Return true when the event should be processed, false when duplicate.
    pub fn should_process(&mut self, event: &Event) -> Result<bool> {
        let now = now_millis();
        self.prune_expired(now);

        let fingerprint = fingerprint_event(event)?;
        if self.seen.contains_key(&fingerprint) {
            return Ok(false);
        }

        self.insert(fingerprint, now);
        Ok(true)
    }

    fn insert(&mut self, fingerprint: u64, seen_at_ms: u64) {
        self.seen.insert(fingerprint, seen_at_ms);
        self.order.push_back((fingerprint, seen_at_ms));

        while self.seen.len() > self.capacity {
            if let Some((expired_key, _)) = self.order.pop_front() {
                self.seen.remove(&expired_key);
            }
        }
    }

    fn prune_expired(&mut self, now: u64) {
        let Some(ttl_ms) = self.ttl_ms else {
            return;
        };

        while let Some((fingerprint, seen_at_ms)) = self.order.front().copied() {
            if now.saturating_sub(seen_at_ms) < ttl_ms {
                break;
            }
            self.order.pop_front();
            self.seen.remove(&fingerprint);
        }
    }
}

/// Build a stable fingerprint suitable for sink-side duplicate suppression.
///
/// The fingerprint includes source position and intra-transaction sequence so
/// that events sharing coarse offsets remain distinguishable.
pub fn fingerprint_event(event: &Event) -> Result<u64> {
    if event.source.source_name.trim().is_empty() {
        return Err(Error::ValidationError(vec![
            "cannot fingerprint event with empty source.source_name".into(),
        ]));
    }
    if event.source.offset.trim().is_empty() {
        return Err(Error::ValidationError(vec![
            "cannot fingerprint event with empty source.offset".into(),
        ]));
    }

    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    event.source.source_name.hash(&mut hasher);
    event.source.offset.hash(&mut hasher);
    event.table.hash(&mut hasher);
    event.op.to_string().hash(&mut hasher);
    event.primary_key.hash(&mut hasher);

    // Different events can share a source offset inside a transaction; include
    // sequence metadata and payload shape so they remain unique.
    if let Some(tx) = &event.transaction {
        tx.tx_id.hash(&mut hasher);
        tx.event_index.hash(&mut hasher);
        tx.total_events.hash(&mut hasher);
    }

    if let Some(before) = &event.before {
        before.to_string().hash(&mut hasher);
    }
    if let Some(after) = &event.after {
        after.to_string().hash(&mut hasher);
    }

    Ok(hasher.finish())
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use std::thread;
    use std::time::Duration;

    use serde_json::json;

    use crate::core::{
        Event, Operation, SourceMetadata, TransactionMetadata, EVENT_ENVELOPE_VERSION,
    };

    use super::{fingerprint_event, EventIdempotencyGuard};

    fn make_event(offset: &str, tx_event_index: Option<u32>) -> Event {
        Event {
            before: None,
            after: Some(json!({"id": 1, "name": "alice"})),
            op: Operation::Insert,
            source: SourceMetadata {
                source_name: "postgres".into(),
                offset: offset.into(),
                timestamp: 1,
            },
            ts: 1,
            schema: Some("public".into()),
            table: "users".into(),
            primary_key: Some(vec!["id".into()]),
            snapshot: None,
            transaction: tx_event_index.map(|event_index| TransactionMetadata {
                tx_id: 42,
                total_events: 2,
                event_index,
            }),
            envelope_version: EVENT_ENVELOPE_VERSION,
        }
    }

    #[test]
    fn duplicate_event_is_suppressed() {
        let mut guard = EventIdempotencyGuard::new(8).unwrap();
        let event = make_event("0/16B6A70", Some(0));

        assert!(guard.should_process(&event).unwrap());
        assert!(!guard.should_process(&event).unwrap());
    }

    #[test]
    fn different_transaction_indexes_are_distinct() {
        let event_a = make_event("same-offset", Some(0));
        let event_b = make_event("same-offset", Some(1));

        let key_a = fingerprint_event(&event_a).unwrap();
        let key_b = fingerprint_event(&event_b).unwrap();
        assert_ne!(key_a, key_b);
    }

    #[test]
    fn capacity_evicts_oldest_fingerprint() {
        let mut guard = EventIdempotencyGuard::new(1).unwrap();
        let first = make_event("off-1", None);
        let second = make_event("off-2", None);

        assert!(guard.should_process(&first).unwrap());
        assert!(guard.should_process(&second).unwrap());

        // first was evicted due to capacity=1
        assert!(guard.should_process(&first).unwrap());
    }

    #[test]
    fn ttl_allows_late_replay_after_expiry() {
        let mut guard = EventIdempotencyGuard::new(8)
            .unwrap()
            .with_ttl_ms(20)
            .unwrap();
        let event = make_event("ttl-offset", None);

        assert!(guard.should_process(&event).unwrap());
        assert!(!guard.should_process(&event).unwrap());

        thread::sleep(Duration::from_millis(30));
        assert!(guard.should_process(&event).unwrap());
    }
}
