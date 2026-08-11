//! Durable record of signal-ingress records whose action has already been decided.
//!
//! # Why a command channel needs one
//!
//! A signal is a command. `execute_snapshot` is **not** idempotent: rustcdc rewinds an
//! already-complete table and reads it again, so executing one twice is a full re-scan of
//! that table plus the duplicate `read` events downstream. That is why the Kafka ingress
//! consumer uses `AutoOffsetReset::Latest` and why the file channel starts at EOF.
//!
//! Those defences only cover the *absence* of a committed offset. Nothing there survives
//! ordinary at-least-once redelivery: the in-memory `audit_recent_entries` dedupe holds
//! 512 entries and starts empty on every boot. Without a durable key the consumer has to
//! choose between committing before the action (a crash loses the command silently) and
//! after it (a crash re-runs a full table scan).
//!
//! This ledger removes the choice — redelivery becomes safe, so the consumer commits
//! *after* the action reaches a terminal state:
//!
//! * crash before the terminal state → not ledgered, not committed → redelivered → runs;
//! * crash after the terminal state, before the commit → ledgered → redelivered → skipped.
//!
//! At-least-once delivery plus a durable idempotency key is effectively-once execution.
//!
//! # Why the key is the Kafka coordinate, not the signal id
//!
//! `signal_id` is optional in the wire format, and when it is absent
//! `process_signal_ingress_record` generates one — a *fresh* one per delivery. Keying on
//! it would therefore fail to dedupe exactly the payloads that carry no idempotency key of
//! their own, which is the common case for a hand-written record. `topic:partition:offset`
//! is stable across redelivery by construction and unique per record, and it needs nothing
//! from the payload.
//!
//! # Durability
//!
//! Append-and-fsync per entry, in the state directory alongside the checkpoint. An entry
//! is one line, so a torn tail is a partial line that `load` drops — the worst case is
//! re-executing the command that was being recorded when the power went out, which is the
//! same outcome as crashing a moment earlier.

use std::collections::HashSet;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use crate::error::AppError;

/// Entries kept after compaction.
///
/// A signal ingress record is settled once its offset is committed and the consumer has
/// moved on; the ledger only has to outlive the window between the terminal state and the
/// commit, which is milliseconds. The bound is generous by four orders of magnitude so
/// that an operator inspecting the file sees useful recent history, and so a consumer that
/// is stuck redelivering the same batch cannot grow it without limit.
const MAX_ENTRIES: usize = 10_000;

/// Rewrite the file once it exceeds this many lines.
///
/// Compacting at exactly `MAX_ENTRIES` would rewrite on nearly every append once the
/// steady state is reached. Twice the bound means a rewrite amortises over `MAX_ENTRIES`
/// appends.
const COMPACT_AT_LINES: usize = MAX_ENTRIES * 2;

/// Signal ingress records whose action has reached a terminal state.
pub(super) struct ProcessedSignalLedger {
    path: PathBuf,
    /// Entries kept after a compaction. A field rather than a constant so the tests can
    /// exercise compaction without performing `COMPACT_AT_LINES` fsyncs — at the real
    /// bound that is 20 000 of them, which took over three minutes.
    max_entries: usize,
    /// Line count that triggers a rewrite.
    compact_at_lines: usize,
    /// Insertion-ordered, so compaction can drop the oldest.
    ///
    /// A `HashSet` for the membership test plus a `Vec` for the order: the set is
    /// consulted once per ingress record and the order is only needed when rewriting.
    inner: Mutex<LedgerState>,
}

/// Take the ledger lock, recovering a poisoned one instead of panicking.
///
/// This was `.expect("signal ledger mutex")` at three call sites, which turned a single
/// panic anywhere under the lock into a permanent outage of the whole signal-ingress path:
/// the first panic poisons the mutex, and every subsequent `contains` and `record` then
/// panics too. `catch_unwind` in the signal worker (added for the same defect class) keeps
/// the *process* alive, so the result was a worker that stayed up and refused every command
/// for the rest of the run.
///
/// Recovery is sound here because the state is two collections and a counter with no
/// cross-field invariant that a half-finished mutation can break: `record` inserts into the
/// set, pushes onto the order and bumps the count, and a duplicate key is a no-op by
/// construction. The worst outcome of resuming from a poisoned guard is one entry counted
/// but not appended — which re-runs one command that had already run, exactly the case
/// at-least-once redelivery already tolerates.
fn lock_ledger(inner: &Mutex<LedgerState>) -> std::sync::MutexGuard<'_, LedgerState> {
    inner.lock().unwrap_or_else(|poisoned| {
        tracing::warn!(
            "signal ledger mutex was poisoned by a panicking holder; recovering rather \
             than failing every subsequent signal for the life of the process"
        );
        poisoned.into_inner()
    })
}

struct LedgerState {
    seen: HashSet<String>,
    order: Vec<String>,
    /// Lines currently in the file, including ones compaction will drop.
    lines_on_disk: usize,
}

impl ProcessedSignalLedger {
    /// Open (and compact) the ledger under `state_dir`.
    ///
    /// A missing file is an empty ledger, not an error: the first run has processed
    /// nothing. An *unreadable* one is also treated as empty, with a warning — refusing to
    /// start because a dedup cache is corrupt would trade a possible duplicate for a
    /// certain outage, and the file is a cache of decisions, not a source of truth.
    pub(super) fn open(state_dir: &Path) -> Result<Self, AppError> {
        Self::open_with_bounds(state_dir, MAX_ENTRIES, COMPACT_AT_LINES)
    }

    fn open_with_bounds(
        state_dir: &Path,
        max_entries: usize,
        compact_at_lines: usize,
    ) -> Result<Self, AppError> {
        debug_assert!(compact_at_lines >= max_entries);
        let dir = state_dir.join("signals");
        std::fs::create_dir_all(&dir).map_err(AppError::from)?;
        let path = dir.join("processed.log");

        let mut order: Vec<String> = Vec::new();
        match std::fs::File::open(&path) {
            Ok(file) => {
                for line in BufReader::new(file).lines() {
                    match line {
                        Ok(line) => {
                            let line = line.trim();
                            if !line.is_empty() {
                                order.push(line.to_string());
                            }
                        }
                        // A torn final line from an interrupted append. Everything before
                        // it is intact, so keep it and drop the remainder.
                        Err(error) => {
                            tracing::warn!(
                                error = %error,
                                path = %path.display(),
                                "signal ledger has an unreadable tail; keeping the entries \
                                 before it"
                            );
                            break;
                        }
                    }
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                tracing::warn!(
                    error = %error,
                    path = %path.display(),
                    "signal ledger could not be read; starting empty. A redelivered signal \
                     that had already completed may run a second time."
                );
            }
        }

        let lines_on_disk = order.len();
        let ledger = Self {
            path,
            max_entries,
            compact_at_lines,
            inner: Mutex::new(LedgerState {
                seen: HashSet::new(),
                order: Vec::new(),
                lines_on_disk,
            }),
        };

        {
            let mut state = lock_ledger(&ledger.inner);
            for key in order {
                if state.seen.insert(key.clone()) {
                    state.order.push(key);
                }
            }
            if state.lines_on_disk > ledger.compact_at_lines {
                ledger.compact_locked(&mut state)?;
            }
        }

        Ok(ledger)
    }

    /// Has this record's action already been decided?
    pub(super) fn contains(&self, key: &str) -> bool {
        lock_ledger(&self.inner).seen.contains(key)
    }

    /// Record that this record's action reached a terminal state.
    ///
    /// Durable before it returns: the caller commits the consumer offset next, and a crash
    /// between the two must leave the ledger ahead, never behind. Ahead means a
    /// redelivered record is skipped; behind means it runs twice.
    pub(super) fn record(&self, key: &str) -> Result<(), AppError> {
        let mut state = lock_ledger(&self.inner);
        if !state.seen.insert(key.to_string()) {
            return Ok(());
        }
        state.order.push(key.to_string());
        state.lines_on_disk += 1;

        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .map_err(AppError::from)?;
        writeln!(file, "{key}").map_err(AppError::from)?;
        file.sync_all().map_err(AppError::from)?;

        if state.lines_on_disk > self.compact_at_lines {
            self.compact_locked(&mut state)?;
        }
        Ok(())
    }

    /// Rewrite the file with only the newest `MAX_ENTRIES`.
    ///
    /// Write-to-temp-then-rename, so a crash mid-compaction leaves the previous complete
    /// file rather than a truncated one. Losing entries here would mean re-executing a
    /// completed command.
    fn compact_locked(&self, state: &mut LedgerState) -> Result<(), AppError> {
        if state.order.len() > self.max_entries {
            let drop_count = state.order.len() - self.max_entries;
            for key in state.order.drain(..drop_count) {
                state.seen.remove(&key);
            }
        }

        let temp = self.path.with_extension("compacting");
        {
            let mut file = std::fs::File::create(&temp).map_err(AppError::from)?;
            for key in &state.order {
                writeln!(file, "{key}").map_err(AppError::from)?;
            }
            file.sync_all().map_err(AppError::from)?;
        }
        std::fs::rename(&temp, &self.path).map_err(AppError::from)?;
        state.lines_on_disk = state.order.len();
        Ok(())
    }
}

/// The ledger key for a Kafka signal-ingress record.
///
/// Stable across redelivery and unique per record, which the payload is not — see the
/// module docs.
pub(super) fn kafka_ingress_key(topic: &str, partition: i32, offset: i64) -> String {
    format!("kafka:{topic}:{partition}:{offset}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_recorded_key_survives_a_reopen() {
        let dir = tempfile::tempdir().expect("tempdir");
        let key = kafka_ingress_key("cdc.signals", 0, 42);

        {
            let ledger = ProcessedSignalLedger::open(dir.path()).expect("open");
            assert!(!ledger.contains(&key), "nothing recorded yet");
            ledger.record(&key).expect("record");
            assert!(ledger.contains(&key));
        }

        // The restart is the whole point: the in-memory audit ring that used to be the
        // only dedup starts empty here, and this must not.
        let reopened = ProcessedSignalLedger::open(dir.path()).expect("reopen");
        assert!(
            reopened.contains(&key),
            "a completed signal must stay deduped across a restart"
        );
        assert!(!reopened.contains(&kafka_ingress_key("cdc.signals", 0, 43)));
    }

    #[test]
    fn recording_the_same_key_twice_is_a_no_op() {
        let dir = tempfile::tempdir().expect("tempdir");
        let ledger = ProcessedSignalLedger::open(dir.path()).expect("open");
        let key = kafka_ingress_key("cdc.signals", 1, 7);

        ledger.record(&key).expect("first");
        ledger.record(&key).expect("second");

        let contents =
            std::fs::read_to_string(dir.path().join("signals/processed.log")).expect("ledger file");
        assert_eq!(
            contents.lines().filter(|l| !l.trim().is_empty()).count(),
            1,
            "a duplicate record must not append a second line"
        );
    }

    /// Compaction keeps the newest entries and drops the oldest.
    ///
    /// Unbounded growth matters here: a consumer stuck redelivering, or a busy signal
    /// topic, would otherwise fsync an ever-growing file on the ingress path.
    ///
    /// Run against small bounds. At the production ones this is 20 000 fsyncs and took
    /// over three minutes, which is the sort of test people delete rather than wait for.
    #[test]
    fn the_ledger_compacts_and_keeps_the_newest_entries() {
        const KEEP: usize = 4;
        const TRIGGER: usize = 8;

        let dir = tempfile::tempdir().expect("tempdir");
        let ledger =
            ProcessedSignalLedger::open_with_bounds(dir.path(), KEEP, TRIGGER).expect("open");

        let total = TRIGGER + 3;
        for offset in 0..total {
            ledger
                .record(&kafka_ingress_key("cdc.signals", 0, offset as i64))
                .expect("record");
        }

        let contents =
            std::fs::read_to_string(dir.path().join("signals/processed.log")).expect("ledger file");
        let lines = contents.lines().filter(|l| !l.trim().is_empty()).count();
        assert!(
            lines <= TRIGGER,
            "compaction must keep the file under the trigger, got {lines} lines"
        );

        let newest = kafka_ingress_key("cdc.signals", 0, (total - 1) as i64);
        assert!(
            ledger.contains(&newest),
            "the most recent entry must survive compaction — it is the one still in the \
             window between its terminal state and its offset commit"
        );
        assert!(
            !ledger.contains(&kafka_ingress_key("cdc.signals", 0, 0)),
            "the oldest entry is the one compaction is expected to drop"
        );

        // The bound has to survive the reopen too: `open` compacts as it loads, and a
        // ledger that grew past the trigger only while the process was down would
        // otherwise stay oversized forever.
        let reopened =
            ProcessedSignalLedger::open_with_bounds(dir.path(), KEEP, TRIGGER).expect("reopen");
        assert!(reopened.contains(&newest));
    }

    /// A truncated tail must not lose the entries before it.
    #[test]
    fn a_torn_tail_keeps_the_entries_before_it() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("signals");
        std::fs::create_dir_all(&path).expect("dir");
        // Two complete lines and one with no trailing newline, as an interrupted append
        // would leave.
        std::fs::write(
            path.join("processed.log"),
            "kafka:t:0:1\nkafka:t:0:2\nkafka:t:0:3",
        )
        .expect("seed");

        let ledger = ProcessedSignalLedger::open(dir.path()).expect("open");
        assert!(ledger.contains("kafka:t:0:1"));
        assert!(ledger.contains("kafka:t:0:2"));
    }
}
