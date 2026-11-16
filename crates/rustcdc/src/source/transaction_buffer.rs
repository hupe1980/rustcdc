//! The events of an open source transaction, held until its commit within a byte limit.
//!
//! A connector learns how many events a transaction has only at its commit, and stamps
//! that total on each one. Holding every event until then makes memory grow with the
//! transaction, which a large enough transaction turns into an out-of-memory kill — and,
//! since the source resends the same transaction after a restart, into a loop.
//!
//! [`TransactionBuffer`] bounds it. Past the limit, what happens depends on whether the
//! transaction may reach the sink in parts, which the runtime decides
//! ([`TransactionRelease`]): released early, without a total, or refused with an error.

use crate::core::{Error, Event, Result};

/// Whether a source may release an open transaction's events before its commit.
///
/// The runtime derives it from how batches are committed and passes it to the source
/// before each stream start. A source the runtime has not told keeps transactions whole:
/// the choice that never delivers part of a transaction twice.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum TransactionRelease {
    /// Past the buffer limit, release the transaction's events as they come, with
    /// `total_events: None`. Its last event still carries the total.
    ///
    /// Safe when a partly delivered transaction can be replayed whole: at-least-once
    /// delivery, with batches that may end inside a transaction.
    Early,
    /// Never release a transaction before its commit. Past the buffer limit the stream
    /// fails with [`Error::Unrecoverable`].
    ///
    /// Required when each batch commits its records with the checkpoint, or when batches
    /// must hold whole transactions: a partly delivered transaction could not be replayed
    /// without delivering its first part twice.
    Whole,
}

/// The events of the open transaction, with their estimated size.
#[derive(Debug)]
pub(crate) struct TransactionBuffer {
    events: Vec<Event>,
    bytes: u64,
    next_index: u32,
    limit_bytes: u64,
    release: TransactionRelease,
}

impl TransactionBuffer {
    pub(crate) fn new(limit_bytes: u64, release: TransactionRelease) -> Self {
        Self {
            events: Vec::new(),
            bytes: 0,
            next_index: 0,
            limit_bytes,
            release,
        }
    }

    /// Start a new transaction, dropping anything held for the previous one.
    pub(crate) fn begin(&mut self) {
        self.events.clear();
        self.bytes = 0;
        self.next_index = 0;
    }

    /// Events held for the open transaction.
    pub(crate) fn len(&self) -> usize {
        self.events.len()
    }

    /// The index the next event of the open transaction takes.
    pub(crate) fn next_index(&self) -> u32 {
        self.next_index
    }

    /// Add an event of the open transaction, numbering it, and return what to release now.
    ///
    /// Empty while the transaction fits. Past the limit under [`TransactionRelease::Early`],
    /// every held event but the newest: keeping one back means the transaction's last
    /// event is always still held at the commit, and carries the total that marks its end.
    /// So one event is held whatever its size. Under [`TransactionRelease::Whole`], any
    /// event that takes the transaction past the limit fails it, a lone one included.
    pub(crate) fn push(&mut self, mut event: Event) -> Result<Vec<Event>> {
        if let Some(tx) = event.transaction.as_mut() {
            tx.event_index = self.next_index;
        }
        self.next_index = self.next_index.saturating_add(1);
        self.bytes = self
            .bytes
            .saturating_add(event.estimated_serialized_bytes() as u64);
        self.events.push(event);

        if self.bytes <= self.limit_bytes {
            return Ok(Vec::new());
        }
        match self.release {
            TransactionRelease::Whole => Err(self.overflow_error()),
            // The newest is always kept, even alone past the limit: the transaction's last
            // event must still be held at the commit to carry the total.
            TransactionRelease::Early if self.events.len() < 2 => Ok(Vec::new()),
            TransactionRelease::Early => {
                let newest = self.events.pop().expect("at least two events are held");
                self.bytes = newest.estimated_serialized_bytes() as u64;
                Ok(std::mem::replace(&mut self.events, vec![newest]))
            }
        }
    }

    /// The open transaction committed: stamp its total on the events still held and
    /// return them.
    pub(crate) fn commit(&mut self) -> Vec<Event> {
        let total = self.next_index;
        for event in &mut self.events {
            if let Some(tx) = event.transaction.as_mut() {
                tx.total_events = Some(total);
            }
        }
        self.bytes = 0;
        self.next_index = 0;
        std::mem::take(&mut self.events)
    }

    fn overflow_error(&self) -> Error {
        let tx_id = self
            .events
            .first()
            .and_then(|event| event.transaction.as_ref())
            .map_or_else(|| "unknown".to_string(), |tx| tx.tx_id.to_string());
        Error::Unrecoverable(format!(
            "transaction {tx_id} outgrew transaction_buffer_bytes ({limit} bytes) before its \
             commit, and this pipeline must deliver each transaction whole: each batch is \
             committed with its checkpoint, or batches must keep transactions together. \
             Releasing part of it would deliver that part twice after a restart, and holding \
             more would grow memory with the transaction. Raise transaction_buffer_bytes \
             above this transaction's size; a restart replays it from its start.",
            limit = self.limit_bytes,
        ))
    }
}
