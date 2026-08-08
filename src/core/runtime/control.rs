//! A cloneable, `&self` handle for driving control operations from another task.
//!
//! # Why this exists
//!
//! An embedder's event loop holds `&mut CdcRuntime` for its entire lifetime, because
//! [`poll_event_batch`](CdcRuntime::poll_event_batch) needs it. Every control operation is
//! therefore unreachable from an admin endpoint, a signal handler or a scheduler without
//! marshalling it through a channel and servicing it between polls.
//!
//! That machinery is the same every time — an `mpsc` of commands, a `oneshot` per reply,
//! a drain point in the loop — so it belongs here, in the place that owns the invariants,
//! rather than in each embedder. It also means the next control operation lands for free.
//!
//! # Reads and writes are handled differently, on purpose
//!
//! Progress is read from a shared snapshot the runtime refreshes on every poll, so
//! [`RuntimeControl::incremental_snapshot_state`] is a plain non-blocking `fn` that cannot
//! deadlock and cannot be starved by a busy pipeline. It is stale by at most one poll.
//!
//! Commands mutate the driver, so they go through the channel and complete when the runtime
//! next services it. That makes them `async` and makes them depend on the loop still
//! turning — see [`RuntimeControl`] for what happens when it is not.

use std::sync::{Arc, Mutex};

use tokio::sync::{mpsc, oneshot};

use crate::{
    core::{CdcRuntime, Error, Result},
    source::IncrementalSnapshotState,
};

/// Depth of the control-command queue.
///
/// Control operations are operator actions — a handful per hour at most — so this only has
/// to absorb a burst from a retrying admin client. A full queue fails fast with an
/// actionable error rather than blocking the caller behind a pipeline that has stopped
/// turning.
const CONTROL_QUEUE_DEPTH: usize = 64;

/// A control operation to be applied by the runtime between polls.
enum ControlCommand {
    RequestSnapshot {
        tables: Vec<String>,
        reply: oneshot::Sender<Result<usize>>,
    },
    SetSnapshotPaused {
        paused: bool,
        reply: oneshot::Sender<Result<bool>>,
    },
    StopSnapshot {
        reply: oneshot::Sender<Result<usize>>,
    },
}

/// Live state the runtime publishes for `&self` readers.
#[derive(Debug, Default)]
pub(super) struct ControlShared {
    incremental_snapshot: Mutex<Option<IncrementalSnapshotState>>,
}

impl ControlShared {
    /// Refresh the published snapshot. Called by the runtime once per poll.
    pub(super) fn publish(&self, state: Option<IncrementalSnapshotState>) {
        if let Ok(mut slot) = self.incremental_snapshot.lock() {
            *slot = state;
        }
    }

    fn incremental_snapshot(&self) -> Option<IncrementalSnapshotState> {
        self.incremental_snapshot
            .lock()
            .ok()
            .and_then(|slot| slot.clone())
    }
}

/// The runtime's half of the control channel.
pub(super) struct ControlEndpoint {
    shared: Arc<ControlShared>,
    /// Kept so `control_handle()` can hand out senders without the runtime owning one —
    /// a sender held by the runtime would keep the channel alive forever and mean
    /// `recv()` never reports "no handles left".
    sender: mpsc::Sender<ControlCommand>,
    receiver: mpsc::Receiver<ControlCommand>,
}

impl ControlEndpoint {
    pub(super) fn new() -> Self {
        let (sender, receiver) = mpsc::channel(CONTROL_QUEUE_DEPTH);
        Self {
            shared: Arc::new(ControlShared::default()),
            sender,
            receiver,
        }
    }

    pub(super) fn shared(&self) -> &Arc<ControlShared> {
        &self.shared
    }

    pub(super) fn handle(&self) -> RuntimeControl {
        RuntimeControl {
            sender: self.sender.clone(),
            shared: Arc::clone(&self.shared),
        }
    }
}

/// A cloneable handle for control operations, usable from a task other than the one
/// polling the runtime.
///
/// Obtain one with [`CdcRuntime::control_handle`] **before** the poll loop takes
/// `&mut CdcRuntime`.
///
/// ```no_run
/// # use rustcdc::{CdcRuntime, CancellationToken};
/// # async fn example(mut runtime: CdcRuntime, shutdown: CancellationToken) -> rustcdc::Result<()> {
/// let control = runtime.control_handle();
///
/// tokio::spawn(async move {
///     // An admin endpoint, a signal handler, a scheduler — anything but the poll loop.
///     let _ = control.request_incremental_snapshot(vec!["public.orders".into()]).await;
/// });
///
/// runtime.run_to_completion(shutdown).await?;
/// # Ok(())
/// # }
/// ```
///
/// # Commands are applied between polls
///
/// A command completes when the runtime next services the queue, which happens at the top
/// of every [`poll_event_batch`](CdcRuntime::poll_event_batch). Two consequences worth
/// designing around:
///
/// * Latency is bounded by how often you poll, not by the command itself. A loop with a
///   large `max_poll_wait_ms` on a quiet source makes control operations feel slow.
/// * If the loop **stops turning**, a command waits indefinitely. Wrap calls in
///   [`tokio::time::timeout`] if the caller has an SLO; a timeout firing here is a genuine
///   signal that the pipeline is stalled, and `admin_snapshot().health` will say why.
///
/// Dropping the runtime resolves outstanding commands with an error rather than hanging.
#[derive(Clone)]
pub struct RuntimeControl {
    sender: mpsc::Sender<ControlCommand>,
    shared: Arc<ControlShared>,
}

impl std::fmt::Debug for RuntimeControl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RuntimeControl")
            .field("connected", &!self.sender.is_closed())
            .finish_non_exhaustive()
    }
}

impl RuntimeControl {
    /// Live incremental-snapshot progress, or `None` when no snapshot is in flight.
    ///
    /// Non-blocking and infallible: read from a snapshot the runtime republishes on every
    /// poll rather than round-tripped through the command queue, so a progress readout can
    /// never be starved by a busy pipeline or hang behind a stalled one. It is therefore
    /// stale by at most one poll interval, which is the right trade for a number an
    /// operator refreshes in a dashboard.
    ///
    /// Returns `None` before the first poll completes, and after the runtime is dropped.
    pub fn incremental_snapshot_state(&self) -> Option<IncrementalSnapshotState> {
        self.shared.incremental_snapshot()
    }

    /// Whether the runtime this handle points at is still alive.
    ///
    /// `false` once the runtime has been dropped; every command then fails immediately
    /// rather than waiting.
    pub fn is_connected(&self) -> bool {
        !self.sender.is_closed()
    }

    /// Add tables to the in-flight incremental snapshot.
    ///
    /// See [`CdcRuntime::request_incremental_snapshot`] for the semantics; this is the
    /// same operation applied from another task.
    pub async fn request_incremental_snapshot(&self, tables: Vec<String>) -> Result<usize> {
        self.dispatch(|reply| ControlCommand::RequestSnapshot { tables, reply })
            .await
    }

    /// Suspend chunk reading. See [`CdcRuntime::pause_incremental_snapshot`].
    pub async fn pause_incremental_snapshot(&self) -> Result<bool> {
        self.dispatch(|reply| ControlCommand::SetSnapshotPaused {
            paused: true,
            reply,
        })
        .await
    }

    /// Resume chunk reading. See [`CdcRuntime::resume_incremental_snapshot`].
    pub async fn resume_incremental_snapshot(&self) -> Result<bool> {
        self.dispatch(|reply| ControlCommand::SetSnapshotPaused {
            paused: false,
            reply,
        })
        .await
    }

    /// Abandon the in-flight snapshot. See [`CdcRuntime::stop_incremental_snapshot`].
    pub async fn stop_incremental_snapshot(&self) -> Result<usize> {
        self.dispatch(|reply| ControlCommand::StopSnapshot { reply })
            .await
    }

    async fn dispatch<T, F>(&self, build: F) -> Result<T>
    where
        F: FnOnce(oneshot::Sender<Result<T>>) -> ControlCommand,
    {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.sender.send(build(reply_tx)).await.map_err(|_| {
            Error::StateError(
                "the runtime this control handle points at has been dropped; control \
                 operations are only serviced by a live poll loop"
                    .into(),
            )
        })?;
        reply_rx.await.map_err(|_| {
            Error::StateError(
                "the runtime stopped before it could apply this control operation; it was \
                 not applied"
                    .into(),
            )
        })?
    }
}

impl CdcRuntime {
    /// A cloneable handle for control operations, usable from another task while this
    /// runtime is being polled.
    ///
    /// Take it **before** the poll loop borrows the runtime mutably — after that the
    /// runtime is unreachable, which is the whole problem this solves.
    ///
    /// See [`RuntimeControl`] for how commands are scheduled and what happens when the
    /// loop stops turning.
    pub fn control_handle(&self) -> RuntimeControl {
        self.control.handle()
    }

    /// Apply every queued control command, and republish live progress.
    ///
    /// Called at the top of each poll. Draining without awaiting the source is deliberate:
    /// `poll_event_batch` is not cancel-safe, so servicing control operations as a
    /// `select!` arm alongside it would discard events. Between polls is the only safe
    /// point, and it is why control latency is bounded by the poll interval.
    pub(super) async fn service_control_commands(&mut self) -> Result<()> {
        while let Ok(command) = self.control.receiver.try_recv() {
            match command {
                ControlCommand::RequestSnapshot { tables, reply } => {
                    let result = self.request_incremental_snapshot(tables).await;
                    // A dropped receiver means the caller gave up (its timeout fired).
                    // The operation still happened, so this is not an error — but it is
                    // worth a log, because the caller now has no idea it took effect.
                    if reply.send(result).is_err() {
                        tracing::warn!(
                            target: "rustcdc::core::runtime",
                            "an incremental-snapshot request was applied after its caller \
                             stopped waiting; the snapshot is running but the caller was \
                             not told",
                        );
                    }
                }
                ControlCommand::SetSnapshotPaused { paused, reply } => {
                    let result = self.set_incremental_snapshot_paused(paused).await;
                    let _ = reply.send(result);
                }
                ControlCommand::StopSnapshot { reply } => {
                    let result = self.stop_incremental_snapshot().await;
                    let _ = reply.send(result);
                }
            }
        }

        self.control
            .shared()
            .publish(self.incremental_snapshot_state());
        Ok(())
    }
}
