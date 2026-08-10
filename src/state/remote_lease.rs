//! Owner lease for network-shared state backends.
//!
//! # Why this exists
//!
//! The whole design rests on exactly one process owning a pipeline's state, and until
//! now that was enforced by two filesystem-scoped mechanisms:
//!
//! * `StateDirLock` — a PID file in the local state directory. Two containers have
//!   separate filesystems and separate PID namespaces, so both acquire it
//!   unconditionally.
//! * rustcdc's `OwnerLease` — `HOSTNAME:PID` in a lease *file*. It does refuse
//!   cross-host conflicts, but only where the file itself is shared. For the OpenDAL
//!   backends it guards a per-instance local mirror directory, which is never
//!   contended, so it protects nothing.
//!
//! For `redis`, `postgresql` and `kafka_topic` the authoritative state lives on the
//! network and had no mutual exclusion at all: no lease, no fencing token, no CAS. Two
//! instances interleaved checkpoint writes on a last-write-wins basis, and if the
//! lagging one wrote last the durable position moved **backwards** — a silent replay on
//! the next restart.
//!
//! That is not a rare race. A Kubernetes `Deployment` without `strategy: Recreate`
//! surges to two pods on every rollout, so it happened on every deploy.
//!
//! # What this guarantees, and what it does not
//!
//! **Does:** a second instance that starts while a live lease is held refuses to run,
//! naming the current owner. A crashed owner's lease expires on its own after
//! `LEASE_TTL`, so recovery needs no manual step. An owner that loses the store or is
//! partitioned long enough for its lease to be stolen discovers this on its next
//! renewal and fences *itself* rather than continuing to write blind.
//!
//! **Does not:** this is not a consensus lease. Acquisition is read-then-write, not
//! compare-and-swap, because OpenDAL's Redis and PostgreSQL services do not expose a
//! uniform CAS primitive. Two instances starting within the same read-write window can
//! both observe a free slot and both proceed. That window is milliseconds against a
//! previously unbounded exposure, and it does not cover the case this was built for —
//! a rolling update, where the incumbent's lease is live and fresh.
//!
//! Anyone needing a hard guarantee should use a store with a real CAS (etcd, ZooKeeper,
//! Consul) or rely on source-level exclusivity. Note that source exclusivity is uneven:
//! a PostgreSQL replication slot admits one connection and MySQL rejects a duplicate
//! `server_id`, but **SQL Server CDC capture tables are ordinary reads with no
//! exclusivity whatsoever** — that connector has only this lease.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::error::AppError;

/// How long a lease stays valid without a renewal.
///
/// A crashed owner blocks a successor for at most this long. Too short and a paused
/// process (a long GC-like stall, a suspended VM) loses a lease it still believes it
/// holds; too long and recovery from a hard kill drags. Six heartbeats is a
/// conventional middle.
pub(crate) const LEASE_TTL: Duration = Duration::from_secs(60);

/// How often the owner refreshes its lease.
pub(crate) const LEASE_HEARTBEAT: Duration = Duration::from_secs(10);

/// The key the lease record is stored under, alongside the checkpoint.
pub(crate) const LEASE_KEY: &str = "owner_lease";

/// Who owns a pipeline's remote state, and when they last said so.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct LeaseRecord {
    /// Stable identity of the owning process.
    ///
    /// Hostname alone is not enough — a `StatefulSet` pod keeps its hostname across
    /// restarts, so a crashed-and-restarted pod would look like the same owner and
    /// silently steal from a still-live predecessor that had not yet exited. The PID
    /// disambiguates within a host and the nonce disambiguates a recycled PID.
    pub(crate) owner: String,
    /// Unix milliseconds of the last renewal.
    pub(crate) renewed_at_ms: u64,
    /// Monotonic per-acquisition counter, for diagnostics and for a future fencing
    /// token if the store ever grows a CAS primitive.
    pub(crate) epoch: u64,
}

impl LeaseRecord {
    /// Whether this record is still within its TTL as of `now_ms`.
    ///
    /// A record stamped in the *future* is treated as live. Clock skew between two
    /// hosts is real, and the conservative reading of "I cannot tell how old this is"
    /// is "assume someone holds it" — refusing to start is recoverable, two writers is
    /// not.
    pub(crate) fn is_live(&self, now_ms: u64, ttl: Duration) -> bool {
        if self.renewed_at_ms > now_ms {
            return true;
        }
        now_ms.saturating_sub(self.renewed_at_ms) < ttl.as_millis() as u64
    }
}

/// Identity for this process, stable for its lifetime and distinct from any other.
pub(crate) fn owner_id() -> String {
    let host = hostname();
    let pid = std::process::id();
    // A recycled PID on the same host after a crash would otherwise be
    // indistinguishable from the previous owner. The start instant is cheap and
    // sufficient — this only has to differ between two processes, not be a UUID.
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    format!("{host}:{pid}:{nonce:08x}")
}

/// This process's host component, as it appears in [`owner_id`].
pub(crate) fn owner_host() -> String {
    hostname()
}

fn hostname() -> String {
    std::env::var("HOSTNAME")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "unknown-host".to_string())
}

pub(crate) fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Build the error returned when another live owner holds the lease.
///
/// The message names the holder and the age, because "someone else has it" without
/// saying who sends an operator to read source code at the worst possible moment.
pub(crate) fn conflict_error(backend: &str, existing: &LeaseRecord, now_ms: u64) -> AppError {
    let age_seconds = now_ms.saturating_sub(existing.renewed_at_ms) as f64 / 1000.0;
    AppError::Other(format!(
        "{backend} state is already owned by '{}' (last renewed {age_seconds:.1}s ago, \
         epoch {}). Two instances writing one pipeline's state interleave checkpoints \
         on a last-write-wins basis, and the durable position can move backwards — so \
         this process will not start.\n\n\
         If that owner is gone, the lease expires by itself after {}s. If you are \
         deploying under Kubernetes, set `strategy: {{ type: Recreate }}` on the \
         Deployment: the default rolling update starts the new pod before stopping the \
         old one, which is exactly this conflict.",
        existing.owner,
        existing.epoch,
        LEASE_TTL.as_secs(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_record_within_its_ttl_is_live() {
        let record = LeaseRecord {
            owner: "host:1:abcd".to_string(),
            renewed_at_ms: 10_000,
            epoch: 1,
        };
        assert!(record.is_live(10_000 + 59_000, LEASE_TTL));
        assert!(!record.is_live(10_000 + 61_000, LEASE_TTL));
    }

    /// Clock skew must fail closed.
    ///
    /// Two hosts rarely agree to the millisecond. A record stamped ahead of our clock
    /// would compute a negative age; saturating arithmetic turns that into `0`, which
    /// would read as "brand new" — correct here by accident, but only by accident. The
    /// explicit future check makes the intent visible: an unreadable age means assume
    /// the lease is held, because refusing to start is recoverable and two concurrent
    /// writers are not.
    #[test]
    fn a_record_from_a_skewed_future_clock_is_treated_as_live() {
        let record = LeaseRecord {
            owner: "other-host:9:beef".to_string(),
            renewed_at_ms: 100_000,
            epoch: 3,
        };
        assert!(record.is_live(90_000, LEASE_TTL));
    }

    #[test]
    fn owner_ids_differ_between_calls_so_a_recycled_pid_is_not_mistaken_for_the_owner() {
        let first = owner_id();
        std::thread::sleep(std::time::Duration::from_millis(2));
        let second = owner_id();
        assert_ne!(
            first, second,
            "two acquisitions in one process must still be distinguishable; a recycled \
             PID on the same host would otherwise look like the previous owner"
        );
    }

    #[test]
    fn the_conflict_error_names_the_holder_and_the_remedy() {
        let record = LeaseRecord {
            owner: "rustcdc-0:7:1234abcd".to_string(),
            renewed_at_ms: 0,
            epoch: 2,
        };
        let message = conflict_error("redis", &record, 5_000).to_string();
        assert!(message.contains("rustcdc-0:7:1234abcd"));
        assert!(message.contains("Recreate"));
        assert!(message.contains("backwards"));
    }
}
