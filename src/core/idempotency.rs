//! Consumer-side idempotency helpers for at-least-once delivery boundaries.

use std::collections::VecDeque;
use std::hash::{Hash, Hasher};
use std::time::{SystemTime, UNIX_EPOCH};

use ahash::{AHashMap as HashMap, AHasher};
use sha2::{Digest, Sha256};

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

        let fingerprint = fingerprint_event_transient(event)?;
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

/// Build a **transient** in-process fingerprint for the runtime idempotency guard.
///
/// Uses [`AHasher`] with a per-process random seed (HashDoS protection).
/// Fingerprints are **not stable across process restarts** — do not persist
/// or compare them across process boundaries.  Use [`fingerprint_event_stable`]
/// when you need a deterministic, cross-restart identifier.
///
/// The fingerprint includes source position and intra-transaction sequence so
/// that events sharing coarse offsets remain distinguishable within a session.
pub fn fingerprint_event_transient(event: &Event) -> Result<u64> {
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

    let mut hasher = AHasher::default();
    event.source.source_name.hash(&mut hasher);
    event.source.offset.hash(&mut hasher);
    event.table.hash(&mut hasher);
    event.op.hash(&mut hasher);
    event.primary_key.hash(&mut hasher);

    // Different events can share a source offset inside a transaction; include
    // sequence metadata and payload shape so they remain unique.
    if let Some(tx) = &event.transaction {
        tx.tx_id.hash(&mut hasher);
        tx.event_index.hash(&mut hasher);
        tx.total_events.hash(&mut hasher);
    }

    // Hash JSON payloads without allocating an intermediate String.
    // serde_json::to_writer writes directly into the hasher's byte sink.
    if let Some(before) = &event.before {
        hash_json_value(before, &mut hasher);
    }
    if let Some(after) = &event.after {
        hash_json_value(after, &mut hasher);
    }

    Ok(hasher.finish())
}

/// Build a **stable, cross-process-safe** fingerprint as a hex-encoded SHA-256 digest.
///
/// Unlike [`fingerprint_event_transient`], this function produces the same output
/// for the same event regardless of which process or restart generated it.  Safe
/// to persist in Redis, a database, or a dedup log across restarts.
///
/// The digest covers: `source_name`, `offset`, `table`, `op`, `primary_key`,
/// optional transaction metadata, and the full `before`/`after` JSON payloads.
///
/// # Performance
/// SHA-256 is ~3–5× slower than `AHasher` on the same input.  For the runtime's
/// internal in-process idempotency guard, prefer [`fingerprint_event_transient`].
/// Reserve this function for cross-restart dedup use cases.
pub fn fingerprint_event_stable(event: &Event) -> Result<String> {
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

    let mut digest = Sha256::new();

    // Domain separator so different logical fields cannot collide.
    digest.update(b"rustcdc/v1/fingerprint\x00");

    // Each field is length-prefixed to prevent boundary collisions.
    let update_str = |d: &mut Sha256, s: &str| {
        d.update((s.len() as u64).to_le_bytes());
        d.update(s.as_bytes());
    };

    update_str(&mut digest, &event.source.source_name);
    update_str(&mut digest, &event.source.offset);
    update_str(&mut digest, &event.table);
    update_str(&mut digest, event.op.to_str());

    if let Some(pks) = &event.primary_key {
        digest.update((pks.len() as u64).to_le_bytes());
        for pk in pks {
            update_str(&mut digest, pk);
        }
    } else {
        digest.update(0u64.to_le_bytes());
    }

    if let Some(tx) = &event.transaction {
        digest.update(1u8.to_le_bytes());
        digest.update(tx.tx_id.to_le_bytes());
        digest.update(tx.event_index.to_le_bytes());
        digest.update(tx.total_events.to_le_bytes());
    } else {
        digest.update(0u8.to_le_bytes());
    }

    if let Some(before) = &event.before {
        digest.update(1u8.to_le_bytes());
        // Canonical JSON serialisation is deterministic for serde_json's Map
        // (preserves insertion order), which matches source row order.
        let bytes = serde_json::to_vec(before)
            .map_err(|e| Error::ValidationError(vec![format!("fingerprint before: {e}")]))?;
        digest.update((bytes.len() as u64).to_le_bytes());
        digest.update(&bytes);
    } else {
        digest.update(0u8.to_le_bytes());
    }

    if let Some(after) = &event.after {
        digest.update(1u8.to_le_bytes());
        let bytes = serde_json::to_vec(after)
            .map_err(|e| Error::ValidationError(vec![format!("fingerprint after: {e}")]))?;
        digest.update((bytes.len() as u64).to_le_bytes());
        digest.update(&bytes);
    } else {
        digest.update(0u8.to_le_bytes());
    }

    Ok(format!("{:x}", digest.finalize()))
}

/// Hash a serde_json Value into the hasher without allocating an intermediate String.
///
/// Walks the JSON tree recursively, tagging each variant with a discriminant byte
/// so `null` ≠ `""` ≠ `false` etc.  For composite values (Array, Object) the
/// structural traversal is canonical (Object keys are iterated in insertion order
/// as stored by serde_json's `Map`, which is ordered for reproducible iteration).
fn hash_json_value(value: &serde_json::Value, hasher: &mut AHasher) {
    match value {
        serde_json::Value::Null => 0_u8.hash(hasher),
        serde_json::Value::Bool(v) => {
            1_u8.hash(hasher);
            v.hash(hasher);
        }
        serde_json::Value::Number(n) => {
            2_u8.hash(hasher);
            if let Some(v) = n.as_i64() {
                v.hash(hasher);
            } else if let Some(v) = n.as_u64() {
                v.hash(hasher);
            } else if let Some(v) = n.as_f64() {
                v.to_bits().hash(hasher);
            } else {
                n.to_string().hash(hasher);
            }
        }
        serde_json::Value::String(v) => {
            3_u8.hash(hasher);
            v.hash(hasher);
        }
        serde_json::Value::Array(arr) => {
            4_u8.hash(hasher);
            arr.len().hash(hasher);
            for item in arr {
                hash_json_value(item, hasher);
            }
        }
        serde_json::Value::Object(map) => {
            5_u8.hash(hasher);
            map.len().hash(hasher);
            for (k, v) in map {
                k.hash(hasher);
                hash_json_value(v, hasher);
            }
        }
    }
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

    use super::{fingerprint_event_stable, fingerprint_event_transient, EventIdempotencyGuard};

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

        let key_a = fingerprint_event_transient(&event_a).unwrap();
        let key_b = fingerprint_event_transient(&event_b).unwrap();
        assert_ne!(key_a, key_b);
    }

    #[test]
    fn stable_fingerprint_is_deterministic() {
        let event = make_event("0/16B6A70", Some(0));
        let a = fingerprint_event_stable(&event).unwrap();
        let b = fingerprint_event_stable(&event).unwrap();
        assert_eq!(a, b, "stable fingerprint must be deterministic");
        assert_eq!(a.len(), 64, "SHA-256 hex digest must be 64 chars");
    }

    #[test]
    fn stable_and_transient_produce_independent_values() {
        // The two functions use different algorithms; their outputs should differ
        // (extremely unlikely to collide even by chance).
        let event = make_event("0/16B6A70", Some(0));
        let transient = fingerprint_event_transient(&event).unwrap().to_string();
        let stable = fingerprint_event_stable(&event).unwrap();
        // They have different types (u64 vs String) so this is just a sanity check.
        assert_ne!(stable, transient);
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
