//! Owner lease for network-shared state backends.
//!
//! Exactly one process may own a pipeline's state. The two filesystem-scoped mechanisms
//! that enforce it elsewhere — the state-directory PID file and rustcdc's `OwnerLease`
//! file — are both no-ops when the authoritative state lives on the network, as it does
//! for `redis`, `postgresql` and `kafka_topic`: separate containers contend for neither.
//! Without this lease two instances interleave checkpoint writes last-write-wins, and a
//! lagging writer moves the durable position **backwards** — a silent replay on the next
//! restart. A Kubernetes `Deployment` without `strategy: Recreate` surges to two pods on
//! every rollout, so that is a per-deploy event, not a rare race.
//!
//! # What it guarantees
//!
//! A second instance starting against a live lease refuses to run and names the owner. A
//! crashed owner's lease expires after `LEASE_TTL`, so recovery needs no manual step. An
//! owner partitioned long enough to lose its lease discovers this on its next renewal and
//! fences *itself* rather than writing blind.
//!
//! # What it does not
//!
//! This is not a consensus lease: acquisition is read-then-write, not compare-and-swap,
//! because OpenDAL's Redis and PostgreSQL services expose no uniform CAS. Two instances
//! starting inside the same millisecond-wide window can both see a free slot — which is
//! not the rolling-update case this exists for, where the incumbent's lease is live.
//! A hard guarantee needs a store with real CAS (etcd, ZooKeeper, Consul) or source-level
//! exclusivity — and that is uneven: a PostgreSQL replication slot admits one connection
//! and MySQL rejects a duplicate `server_id`, but **SQL Server CDC capture tables are
//! ordinary reads with no exclusivity at all**, so that connector has only this lease.

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
///
/// `None` when the host cannot be determined. Callers that compare hosts must treat that
/// as "not comparable" rather than as a name — see [`HOST_UNKNOWN`].
pub(crate) fn owner_host() -> Option<String> {
    let host = hostname();
    (host != HOST_UNKNOWN).then_some(host)
}

/// The placeholder written into a lease when the host is genuinely unknown.
///
/// It is a diagnostic string, never an identity: two machines that both fail to resolve a
/// hostname would otherwise compare equal, and `is_dead_local_owner` would conclude that a
/// remote owner's PID could be checked against the local process table.
pub(crate) const HOST_UNKNOWN: &str = "unknown-host";

/// Best-effort hostname, in descending order of trustworthiness.
///
/// `$HOSTNAME` alone was the previous implementation and is not enough. It is a *shell*
/// variable: Docker and Kubernetes export it, but a unit started by systemd, a launchd
/// job, or anything spawned outside an interactive shell does not, so every such host
/// resolved to the same literal `"unknown-host"`. With a state directory on a shared
/// volume that made two different machines indistinguishable, and `is_dead_local_owner`
/// would then check a *remote* owner's PID against the local process table — a lease steal
/// from a live writer, which is the exact failure this module exists to prevent.
fn hostname() -> String {
    fn non_empty(value: String) -> Option<String> {
        let trimmed = value.trim();
        (!trimmed.is_empty()).then(|| trimmed.to_string())
    }

    std::env::var("HOSTNAME")
        .ok()
        .and_then(non_empty)
        // Linux exposes the kernel's own view here; unlike the env var it cannot be
        // stale or absent inside a namespace.
        .or_else(|| {
            std::fs::read_to_string("/proc/sys/kernel/hostname")
                .ok()
                .and_then(non_empty)
        })
        // The conventional file on Linux and most BSDs.
        .or_else(|| {
            std::fs::read_to_string("/etc/hostname")
                .ok()
                .and_then(non_empty)
        })
        // macOS and the BSDs have neither of the above: the hostname lives in a sysctl,
        // and without this every process on such a host resolves to `HOST_UNKNOWN`, which
        // disables the same-host PID-liveness shortcut in `is_dead_local_owner` — so a
        // crashed owner's lease can only ever be reclaimed by waiting out the full TTL.
        // `unsafe_code` is denied here, so this reads the value through `hostname(1)`
        // rather than `gethostname(2)`. It runs once, at startup.
        .or_else(|| {
            std::process::Command::new("hostname")
                .output()
                .ok()
                .filter(|out| out.status.success())
                .and_then(|out| non_empty(String::from_utf8_lossy(&out.stdout).into_owned()))
        })
        .unwrap_or_else(|| HOST_UNKNOWN.to_string())
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

    /// `$HOSTNAME` is a shell variable, not a process one.
    ///
    /// Docker and Kubernetes export it; systemd units, launchd jobs and anything else
    /// started outside an interactive shell do not. Reading only the env var makes every
    /// such host answer the literal `"unknown-host"` — so two machines sharing a state
    /// volume compare equal and the same-host PID-liveness shortcut in
    /// `is_dead_local_owner` becomes a lease steal from a live writer, while a host that
    /// resolves to the placeholder can never reclaim a crashed owner's lease early.
    ///
    /// The bar is **any** source that can name this host, not only the Linux files: an
    /// earlier version of this test checked `/proc/sys/kernel/hostname` and `/etc/hostname`
    /// alone, so on macOS — which has neither — it passed while resolution was in fact
    /// falling through to the placeholder on every run.
    #[test]
    fn the_hostname_survives_an_environment_that_does_not_export_hostname() {
        // Not `set_var`: that is process-global and would race sibling tests. The fallback
        // chain is exercised directly instead.
        let resolved = super::hostname();
        let nameable = std::fs::read_to_string("/proc/sys/kernel/hostname")
            .or_else(|_| std::fs::read_to_string("/etc/hostname"))
            .ok()
            .or_else(|| {
                std::process::Command::new("hostname")
                    .output()
                    .ok()
                    .filter(|out| out.status.success())
                    .map(|out| String::from_utf8_lossy(&out.stdout).into_owned())
            })
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());

        if nameable.is_some() {
            assert_ne!(
                resolved, HOST_UNKNOWN,
                "a host that can name itself by any means must never resolve to the \
                 placeholder, whatever the environment looks like"
            );
        }

        // And the placeholder is never handed out as an identity.
        if resolved == HOST_UNKNOWN {
            assert!(owner_host().is_none());
        } else {
            assert_eq!(owner_host().as_deref(), Some(resolved.as_str()));
        }
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
