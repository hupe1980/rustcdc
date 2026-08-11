//! Consumer-side idempotency helpers for at-least-once delivery boundaries.

use std::collections::VecDeque;
use std::hash::{Hash, Hasher};
use std::time::{SystemTime, UNIX_EPOCH};

use ahash::{AHashMap as HashMap, AHasher};
use sha2::{Digest, Sha256};

use crate::core::{Error, Event, FingerprintError, Result};

/// Sliding-window guard that suppresses duplicate event deliveries.
///
/// This helper is intended for sink-side consumers that need to absorb replay
/// without requiring exactly-once source semantics.
///
/// # Safety property: it never suppresses what it cannot prove is a duplicate
///
/// The fingerprint is content-derived, so two genuinely distinct rows that happen
/// to be byte-identical also hash identically. That is not hypothetical: an audit
/// or event-log table with no primary key can legitimately contain
/// `INSERT INTO pings VALUES ('ok'), ('ok')`, and on a connector that does not
/// supply intra-transaction sequencing both rows share one source offset. A naive
/// guard drops the second row and the checkpoint advances past it — permanent,
/// silent, unlogged data loss, in the component whose job is to *protect* delivery.
///
/// The guard therefore suppresses only events it can identify: those carrying
/// transaction metadata (`tx_id` + `event_index`) or a resolvable primary-key
/// value. Everything else passes through and is counted in
/// [`EventIdempotencyGuard::unidentifiable_passthrough_count`]. Passing a duplicate
/// through is at-least-once — the guarantee the pipeline already documents — while
/// dropping a distinct row is not recoverable by any downstream consumer.
///
/// # A deliberate re-read is not a duplicate, and the fingerprint has to say so
///
/// The same reasoning has a second edge, and it is subtler because the event really is
/// byte-identical. A snapshot `Read` event's offset identifies the **row**, not a log
/// position, so re-reading an unchanged row produces the same fingerprint — and an
/// operator who re-requests a snapshot then gets `enqueued: 1` and no rows, because this
/// guard drops every one of them as a replay. The guard whose job is to protect delivery
/// discards the delivery that was asked for, silently.
///
/// What distinguishes the two cases is *which snapshot attempt* produced the row, so
/// [`IncrementalSnapshotState::generation`](crate::source::IncrementalSnapshotState::generation)
/// is part of the synthetic offset. A chunk re-read after a mid-snapshot reconnect keeps its
/// generation and is still deduplicated; a new request starts a generation whose rows cannot
/// collide with the previous one's.
///
/// The dependency runs the other way from how it looks: this guard does not know about
/// snapshots, and the driver is responsible for making distinct reads distinguishable. Anything
/// that changes the snapshot offset format has to preserve that, which is what
/// `a_re_snapshotted_row_survives_the_idempotency_guard` pins.
#[derive(Debug, Clone)]
pub struct EventIdempotencyGuard {
    capacity: usize,
    ttl_ms: Option<u64>,
    seen: HashMap<u64, u64>,
    order: VecDeque<(u64, u64)>,
    evictions: u64,
    unidentifiable_passthrough: u64,
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
            evictions: 0,
            unidentifiable_passthrough: 0,
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

    /// Number of fingerprints evicted because the window filled.
    ///
    /// A non-zero and growing value means the window is too small for the replay
    /// distance in this deployment: duplicates older than the window are no longer
    /// suppressed. Delivery stays correct (at-least-once), but a downstream that
    /// relies on the guard for deduplication will start seeing repeats. Raise
    /// `IdempotencyOptions::capacity`.
    pub fn eviction_count(&self) -> u64 {
        self.evictions
    }

    /// Number of events passed through because they could not be identified.
    ///
    /// These are events with neither transaction metadata nor a resolvable
    /// primary-key value — typically rows from a table with no primary key. The
    /// guard deliberately does not deduplicate them; see the type-level docs.
    pub fn unidentifiable_passthrough_count(&self) -> u64 {
        self.unidentifiable_passthrough
    }

    /// Return true when the event should be processed, false when duplicate.
    pub fn should_process(&mut self, event: &Event) -> Result<bool> {
        let now = now_millis();
        self.prune_expired(now);

        if !event_is_identifiable(event) {
            self.unidentifiable_passthrough = self.unidentifiable_passthrough.saturating_add(1);
            return Ok(true);
        }

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
                self.evictions = self.evictions.saturating_add(1);
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

/// Whether an event carries enough identity for duplicate detection to be safe.
///
/// Two conditions qualify:
///
/// * **Transaction metadata.** `tx_id` + `event_index` uniquely order the event
///   within its transaction, so two identical rows in one transaction hash apart.
/// * **A resolvable primary key.** Every declared key column is present in the
///   before- or after-image, so two rows that differ only in identity still hash
///   apart, and a genuine replay of the same row hashes the same.
///
/// A `Truncate` event has no row image at all but is idempotent by nature and
/// carries a distinct source offset, so it qualifies too.
///
/// Anything else — a keyless table on a connector without intra-transaction
/// sequencing — is *not* identifiable, and [`EventIdempotencyGuard`] passes it
/// through rather than risk dropping a distinct row.
fn event_is_identifiable(event: &Event) -> bool {
    event.transaction.is_some()
        || matches!(
            event.op,
            crate::core::Operation::Truncate | crate::core::Operation::SchemaChange
        )
        // Delegated rather than reimplemented. This function used to walk the key columns
        // itself, which made it the *third* place in the crate answering "does this event
        // have a usable key" — and a previous audit found a silent-corruption bug in
        // exactly that shape, where two of those answers disagreed. One implementation
        // means they cannot.
        || event.has_resolvable_key()
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
pub fn fingerprint_event_transient(event: &Event) -> std::result::Result<u64, FingerprintError> {
    if event.source.source_name.trim().is_empty() {
        return Err(FingerprintError::EmptySourceName);
    }
    if event.source.offset.trim().is_empty() {
        return Err(FingerprintError::EmptyOffset);
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
        tx.total_events.unwrap_or(0).hash(&mut hasher);
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
pub fn fingerprint_event_stable(event: &Event) -> std::result::Result<String, FingerprintError> {
    if event.source.source_name.trim().is_empty() {
        return Err(FingerprintError::EmptySourceName);
    }
    if event.source.offset.trim().is_empty() {
        return Err(FingerprintError::EmptyOffset);
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
        digest.update(tx.total_events.unwrap_or(0).to_le_bytes());
    } else {
        digest.update(0u8.to_le_bytes());
    }

    if let Some(before) = &event.before {
        digest.update(1u8.to_le_bytes());
        // Deterministic because `serde_json::Map` is a `BTreeMap` here — the
        // `preserve_order` feature is deliberately **not** enabled — so keys serialise in
        // sorted order regardless of the order the connector inserted them. Sorted is what a
        // cross-process fingerprint needs: insertion order would make the digest depend on a
        // connector's column ordering, and two capture paths for the same row would hash
        // apart. Enabling `preserve_order` anywhere in the dependency graph would silently
        // change every stable fingerprint, which is why this says which property is relied
        // on rather than just asserting determinism.
        let bytes = serde_json::to_vec(before).map_err(FingerprintError::SerializationFailed)?;
        digest.update((bytes.len() as u64).to_le_bytes());
        digest.update(&bytes);
    } else {
        digest.update(0u8.to_le_bytes());
    }

    if let Some(after) = &event.after {
        digest.update(1u8.to_le_bytes());
        let bytes = serde_json::to_vec(after).map_err(FingerprintError::SerializationFailed)?;
        digest.update((bytes.len() as u64).to_le_bytes());
        digest.update(&bytes);
    } else {
        digest.update(0u8.to_le_bytes());
    }

    // RustCrypto 0.11 returns a `hybrid-array::Array`, which no longer implements
    // `LowerHex`. Format the bytes explicitly so the digest encoding stays exactly what it
    // was — a stable fingerprint that changed shape would silently invalidate every
    // persisted dedup record on the consumer side.
    Ok(digest
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

/// Hash a serde_json Value into the hasher without allocating an intermediate String.
///
/// Walks the JSON tree recursively, tagging each variant with a discriminant byte
/// so `null` ≠ `""` ≠ `false` etc.  For composite values (Array, Object) the
/// structural traversal is canonical: `serde_json::Map` is a `BTreeMap` here, so object
/// keys are visited in **sorted** order rather than insertion order, and two rows with the
/// same columns hash the same however the connector ordered them.
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
                total_events: Some(2),
                event_index,
            }),
            envelope_version: EVENT_ENVELOPE_VERSION,
            before_is_key_only: false,
            unavailable_columns: Vec::new(),
            before_unavailable_columns: Vec::new(),
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
    fn capacity_evicts_oldest_fingerprint_and_counts_the_eviction() {
        let mut guard = EventIdempotencyGuard::new(1).unwrap();
        let first = make_event("off-1", None);
        let second = make_event("off-2", None);

        assert!(guard.should_process(&first).unwrap());
        assert!(guard.should_process(&second).unwrap());

        // first was evicted due to capacity=1
        assert!(guard.should_process(&first).unwrap());
        assert!(
            guard.eviction_count() > 0,
            "evictions must be observable — a silently undersized window stops \
             deduplicating with no signal"
        );
    }

    /// Two byte-identical rows from a keyless table, sharing one source offset.
    fn keyless_event(offset: &str) -> Event {
        let mut event = make_event(offset, None);
        event.primary_key = None;
        event.after = Some(json!({"payload": "ping"}));
        event
    }

    #[test]
    fn identical_rows_from_a_keyless_table_are_never_suppressed() {
        // `INSERT INTO pings VALUES ('ok'), ('ok')` on a table with no primary key
        // produces two distinct rows that fingerprint identically. Suppressing the
        // second is permanent data loss: the checkpoint advances past it and nothing
        // downstream can recover it.
        let mut guard = EventIdempotencyGuard::new(8).unwrap();
        let event = keyless_event("same-offset");

        assert!(guard.should_process(&event).unwrap());
        assert!(
            guard.should_process(&event).unwrap(),
            "an unidentifiable event must pass through, not be dropped"
        );
        assert_eq!(guard.unidentifiable_passthrough_count(), 2);
    }

    #[test]
    fn an_event_whose_key_column_is_absent_from_the_row_is_not_identifiable() {
        // A declared primary key that the row image does not actually carry cannot
        // distinguish two rows, so it must not be treated as identity.
        let mut event = make_event("off", None);
        event.primary_key = Some(vec!["id".into()]);
        event.after = Some(json!({"name": "alice"}));

        let mut guard = EventIdempotencyGuard::new(8).unwrap();
        assert!(guard.should_process(&event).unwrap());
        assert!(guard.should_process(&event).unwrap());
        assert_eq!(guard.unidentifiable_passthrough_count(), 2);
    }

    #[test]
    fn transaction_metadata_alone_makes_a_keyless_event_identifiable() {
        let mut guard = EventIdempotencyGuard::new(8).unwrap();
        let mut event = keyless_event("same-offset");
        event.transaction = Some(TransactionMetadata {
            tx_id: 7,
            total_events: Some(2),
            event_index: 0,
        });

        assert!(guard.should_process(&event).unwrap());
        assert!(
            !guard.should_process(&event).unwrap(),
            "with tx sequencing a true replay is still suppressed"
        );
        assert_eq!(guard.unidentifiable_passthrough_count(), 0);
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

#[cfg(test)]
mod canonical_ordering_tests {
    use super::{fingerprint_event_stable, fingerprint_event_transient};
    use crate::core::{Event, Operation, SourceMetadata};

    fn event_with(after: serde_json::Value) -> Event {
        Event::builder("t", Operation::Insert)
            .after(after)
            .primary_key(["id"])
            .source(SourceMetadata::new("pg", "0/1", 1))
            .ts(1)
            .build()
    }

    /// Both fingerprints must be independent of the order a connector inserted columns in.
    ///
    /// This rests on `serde_json::Map` being a `BTreeMap` — the `preserve_order` feature is
    /// deliberately not enabled. If anything in the dependency graph turned it on, object keys
    /// would serialise in insertion order and every stable fingerprint would change silently,
    /// so the property is pinned rather than assumed.
    #[test]
    fn a_fingerprint_does_not_depend_on_column_insertion_order() {
        let one = event_with(serde_json::json!({ "id": "1", "a": "x", "z": "y" }));
        let other = event_with(serde_json::json!({ "z": "y", "id": "1", "a": "x" }));

        assert_eq!(
            fingerprint_event_stable(&one).expect("digest"),
            fingerprint_event_stable(&other).expect("digest"),
            "the stable digest is persisted and compared across processes, so it must not \
             depend on a connector's column ordering"
        );
        assert_eq!(
            fingerprint_event_transient(&one).expect("hash"),
            fingerprint_event_transient(&other).expect("hash"),
        );
    }

    /// The stable digest is persisted by consumers, so a change to it is a breaking change to
    /// their dedup state. Pinned against a literal so a refactor cannot move it quietly.
    #[test]
    fn the_stable_digest_is_a_fixed_value_for_a_fixed_event() {
        let digest = fingerprint_event_stable(&event_with(serde_json::json!({ "id": "1" })))
            .expect("digest");
        assert_eq!(
            digest, "f76b872c487aa0b8f7cad0bcc6ccb0e3b337f02f24043c82eb8c1e35fcf43cd7",
            "the stable fingerprint is persisted in consumers' dedup stores; changing it \
             invalidates theirs, so it may only move as a documented breaking change"
        );
    }
}
