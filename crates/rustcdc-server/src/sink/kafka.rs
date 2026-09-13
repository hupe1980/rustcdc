use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;

use bytes::Bytes;
use krafka::admin::AdminClient;
use krafka::producer::{Acks, Producer, ProducerRecord, RecordMetadata, TransactionalProducer};
use rustcdc::core::Error as RtError;
use tokio::sync::Mutex;

use crate::config::schema::{
    KafkaDeliveryMode, KafkaRecordHeaders, KafkaSecurityConfig, KafkaSinkConfig,
};
use crate::error::AppError;
use crate::topic::{QualifiedTable, TopicResolveError, TopicResolver, TopicTemplate};

use super::SinkDeliveryGuarantee;

/// An accepted-but-unconfirmed send.
///
/// `'static` — hence the `Arc` around each producer — so it can outlive the
/// `send_encoded` call that created it and sit in [`KafkaSink::inflight`].
type InFlightSend = Pin<Box<dyn Future<Output = krafka::error::Result<RecordMetadata>> + Send>>;

/// Producers are held behind `Arc` so a send future can be detached from the
/// `&mut self` that created it. krafka's `send` takes `&self`, so this costs nothing
/// but the refcount and is what makes pipelining expressible at all.
#[derive(Clone)]
enum KafkaProducerClient {
    Idempotent(Arc<Producer>),
    Transactional(Arc<TransactionalProducer>),
}

enum SendSlot {
    Pending(InFlightSend),
    Done(krafka::error::Result<RecordMetadata>),
}

/// The outstanding-send window, polled in **submission order**.
///
/// # Why not `FuturesOrdered`
///
/// The obvious spelling of this is `FuturesOrdered`, and it is wrong here. It is built on
/// `FuturesUnordered`, which links a newly pushed future at the *head* of its intrusive
/// list and therefore polls a freshly pushed batch in **reverse** insertion order.
///
/// That only matters because a krafka send future does two separable things behind one
/// opaque `Future`: it appends the record to the producer's accumulator, and then it
/// waits for the broker acknowledgement. A send that has not yet appended — because the
/// accumulator's channel is momentarily full, which is routine when a whole batch is
/// submitted without yielding — appends on a *later* poll. Poll them backwards and they
/// append backwards.
///
/// `pipelined_sends_reach_the_partition_in_submission_order` reproduces this against a
/// real broker: with `FuturesOrdered` in place, 200 records arrive in reversed runs —
/// exactly the records that queued behind accumulator capacity. For a CDC stream that is
/// silent replica corruption: `balance=50` applied after `balance=100`.
///
/// # The invariant
///
/// Every outstanding send is polled on every wake, in submission order, front to back.
/// Combined with the documented FIFO fairness of the tokio primitives krafka blocks on
/// (the buffer-memory semaphore, the accumulator's mpsc channel, the in-flight
/// semaphore), the earliest record always gets the first opportunity at each stage — so
/// append order matches submission order no matter where a record parks.
///
/// The cost is an O(window) poll sweep per wake, bounded by `max_pipelined_sends`.
/// `FuturesUnordered` exists to avoid exactly that sweep; here the sweep *is* the
/// guarantee, and 128 cheap polls are worth rather less than ordered change data.
///
/// The clean fix belongs upstream: krafka should expose Java's
/// `send(record) -> Future<RecordMetadata>`, separating enqueue from await so the append
/// is complete before the call returns. Until it does, the ordered poll sweep below is
/// what supplies the guarantee.
#[derive(Default)]
struct SendWindow {
    slots: VecDeque<SendSlot>,
}

impl SendWindow {
    fn len(&self) -> usize {
        self.slots.len()
    }

    fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }

    /// Drop every outstanding send without collecting it.
    ///
    /// Cancelling a send future does not un-append a record already handed to the
    /// accumulator; only an aborted transaction, or the idempotent producer's
    /// deduplication on replay, makes it invisible.
    fn abandon(&mut self) {
        self.slots.clear();
    }

    fn push(&mut self, send: InFlightSend) {
        self.slots.push_back(SendSlot::Pending(send));
    }

    fn push_done(&mut self, result: krafka::error::Result<RecordMetadata>) {
        self.slots.push_back(SendSlot::Done(result));
    }

    /// Drive every outstanding send, and yield the oldest once it completes.
    ///
    /// Returns `None` only when the window is empty.
    async fn next_completed(&mut self) -> Option<krafka::error::Result<RecordMetadata>> {
        if self.slots.is_empty() {
            return None;
        }

        let slots = &mut self.slots;
        let completed = std::future::poll_fn(|cx| {
            // Front to back, every wake. This sweep is the ordering guarantee — see the
            // type's documentation for why a `FuturesUnordered` cannot provide it.
            for slot in slots.iter_mut() {
                // A future must never be polled after it returns `Ready`, so a completed
                // send is parked in place until the window drains down to it.
                if let SendSlot::Pending(send) = slot
                    && let Poll::Ready(result) = send.as_mut().poll(cx)
                {
                    *slot = SendSlot::Done(result);
                }
            }

            match slots.front() {
                Some(SendSlot::Done(_)) => match slots.pop_front() {
                    Some(SendSlot::Done(result)) => Poll::Ready(result),
                    _ => unreachable!("front was just observed to be Done"),
                },
                _ => Poll::Pending,
            }
        })
        .await;

        Some(completed)
    }
}

/// Transactional checkpoint barrier state machine.
///
/// For transactional Kafka producers, the barrier lifecycle is:
/// 1. `NotActive`: Normal state (ready to begin new barrier)
/// 2. `Active`: Between begin_transaction() and commit/abort
/// 3. `Closed`: Sink is closed or producer failed catastrophically
///
/// This enum makes state transitions explicit and prevents confusion from boolean flags.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BarrierState {
    /// No transaction in progress; ready to begin() a new barrier
    NotActive,
    /// Transaction active; between begin_transaction() and commit/abort
    Active,
    /// Sink is closed or in error state; no further barriers permitted
    Closed,
}

#[derive(Debug)]
struct BarrierStateMachine {
    state: BarrierState,
}

impl BarrierStateMachine {
    fn new() -> Self {
        Self {
            state: BarrierState::NotActive,
        }
    }

    #[cfg(test)]
    fn state(&self) -> BarrierState {
        self.state
    }

    fn ensure_can_begin(&self) -> rustcdc::core::Result<()> {
        if self.state != BarrierState::NotActive {
            return Err(RtError::StateError(format!(
                "cannot begin checkpoint barrier: state is {:?} (expected NotActive)",
                self.state
            )));
        }
        Ok(())
    }

    fn ensure_can_commit(&self) -> rustcdc::core::Result<()> {
        if self.state != BarrierState::Active {
            return Err(RtError::StateError(format!(
                "cannot commit checkpoint barrier: state is {:?} (expected Active)",
                self.state
            )));
        }
        Ok(())
    }

    fn is_active(&self) -> bool {
        self.state == BarrierState::Active
    }

    fn mark_active(&mut self) {
        self.state = BarrierState::Active;
    }

    fn mark_not_active(&mut self) {
        self.state = BarrierState::NotActive;
    }

    fn mark_closed(&mut self) {
        self.state = BarrierState::Closed;
    }
}

/// A share in the sink's Kafka transaction.
///
/// This is what makes end-to-end exactly-once possible. The source position is not a Kafka
/// consumer offset, so `send_offsets_to_transaction` does not apply; the only way to make
/// the data and the durable position atomic is to write the checkpoint record into the
/// *same* producer transaction as the data. That requires one producer, shared — which is
/// what this hands out.
///
/// Both halves are needed and neither is sufficient alone. The producer is what the
/// checkpoint record is sent through; the barrier is how the state writer knows whether a
/// transaction is already open, and therefore whether to join it or to open its own. A
/// state write outside the batch loop — bootstrap seeding, a schema-history flush during
/// startup — still needs its own transaction.
#[derive(Clone)]
pub struct KafkaTransactionHandle {
    producer: Arc<TransactionalProducer>,
    barrier: Arc<Mutex<BarrierStateMachine>>,
}

impl KafkaTransactionHandle {
    /// Send a record through the shared producer, joining an open transaction if there is
    /// one and opening a private one if there is not.
    ///
    /// Returns `true` when the record joined the caller's transaction, meaning it is *not*
    /// durable until that transaction commits.
    pub(crate) async fn send_in_transaction(
        &self,
        topic: &str,
        key: &[u8],
        payload: &[u8],
    ) -> Result<bool, AppError> {
        // Held across the send so a barrier cannot commit underneath a record that has not
        // reached the accumulator yet — that record would land in the *next* transaction
        // while this one's commit claimed it.
        let barrier = self.barrier.lock().await;
        let joined = barrier.is_active();

        if !joined {
            self.producer.begin_transaction().map_err(|e| {
                AppError::Other(format!("failed to begin kafka state transaction: {e}"))
            })?;
        }

        let result = self.producer.send(topic, Some(key), Some(payload)).await;

        let metadata = match result {
            Ok(metadata) => metadata,
            Err(error) => {
                if !joined {
                    // Best-effort: a fenced producer cannot abort either, and the send
                    // error is the one worth surfacing.
                    if let Err(abort_err) = self.producer.abort_transaction().await {
                        tracing::warn!(
                            error = %abort_err,
                            "aborting the kafka state transaction failed after a send error"
                        );
                    }
                }
                return Err(classify_krafka_error(&error, "state write failed"));
            }
        };

        if !joined {
            self.producer.commit_transaction().await.map_err(|e| {
                AppError::Other(format!("failed to commit the kafka state transaction: {e}"))
            })?;
            enforce_durable_confirmation(&metadata, "state")
                .map_err(|e| AppError::Other(e.to_string()))?;
        }

        Ok(joined)
    }
}

/// Re-classify a tombstone failure that would otherwise quarantine its own delete.
///
/// This closes a hole that only exists because the tombstone is the *second* record.
///
/// `classify_krafka_error` maps `MessageTooLarge`, `InvalidRecord`, `Serialization` and
/// `Compression` to `SinkPoisonRecord`, which becomes `ValidationError` and which
/// `AppError::is_dead_letterable` treats as "quarantine this event and advance". That is
/// the right answer for a *record*. It is the wrong answer here: by the time the
/// tombstone is attempted its delete has already been accepted into the send window and
/// will be published by the next flush. Quarantining the event would advance the
/// checkpoint past a delete that reaches the topic with no tombstone behind it — a key a
/// compacted log never reclaims, produced by the very mechanism meant to prevent it, and
/// silent because every counter reports success.
///
/// So a terminal, record-attributable tombstone failure is escalated to `Unrecoverable`,
/// which halts. Loud and wrong beats quiet and wrong when the quiet failure is
/// undetectable downstream.
///
/// Retriable failures are deliberately left alone. Those surface as `SinkTimeout`, the
/// batch loop retries the whole batch, and the delete is simply re-sent — a duplicate
/// that `at_least_once_idempotent` collapses by sequence number and that
/// `effectively_once` hides behind the aborted barrier.
fn escalate_tombstone_failure(error: RtError) -> RtError {
    match error {
        RtError::ValidationError(_) => RtError::Unrecoverable(format!(
            "the delete for this event was already accepted but its tombstone was \
             rejected: {error}. Halting rather than quarantining, because advancing past \
             it would leave a delete on the topic with no tombstone behind it — a key \
             log compaction can never reclaim, and one nothing downstream can detect."
        )),
        other => other,
    }
}

// ─── CDC provenance headers ───────────────────────────────────────────────────

/// The event's operation — `insert`, `update`, `delete`, `read`, `truncate`,
/// `schema_change`.
pub const HEADER_OP: &str = "__rustcdc.op";
/// The source schema (PostgreSQL/SQL Server schema, MySQL/MariaDB database).
///
/// **Omitted** when the event carries none, rather than sent with a null or empty value.
/// A null header value is a third state on the wire that nothing asked for, and an empty
/// one is indistinguishable from a schema genuinely named `""`. Absent means absent —
/// the same choice the CloudEvents encoder makes for `cdcschema`.
pub const HEADER_SOURCE_SCHEMA: &str = "__rustcdc.source.schema";
/// The source table name.
pub const HEADER_SOURCE_TABLE: &str = "__rustcdc.source.table";
/// The logical name of the source connector that produced the event.
pub const HEADER_SOURCE_NAME: &str = "__rustcdc.source.name";
/// The source log position — LSN, binlog coordinates, LSN/commit pair.
pub const HEADER_SOURCE_OFFSET: &str = "__rustcdc.source.offset";
/// The source commit timestamp, milliseconds since the Unix epoch, as decimal text.
///
/// Text rather than eight big-endian bytes because a header is read by
/// `kafka-console-consumer`, by a `jq` filter and by a human before it is read by a typed
/// consumer, and none of those can decode a binary integer. Every other header here is
/// text for the same reason; making one of them binary would be the surprise.
pub const HEADER_SOURCE_TS_MS: &str = "__rustcdc.source.ts_ms";

/// A source-supplied identifier can be no longer than this in a header value.
///
/// Every database bounds its own identifiers far below this (PostgreSQL 63 bytes, MySQL
/// 64, SQL Server 128) and every connector's offset is a short coordinate string, so this
/// is not a limit anyone reaches. It exists because the alternative to truncating is a
/// broker rejecting the whole record for oversized headers — which fails the *event* over
/// a diagnostic field, and the payload always carries the untruncated value anyway.
const MAX_IDENTIFIER_HEADER_BYTES: usize = 512;

/// Build the provenance headers for one event.
///
/// The `__rustcdc.*` namespace matches this crate's dead-letter headers
/// (`__rustcdc.dlq.*`), which in turn mirror krafka's `__krafka.dlq.*` without borrowing
/// its names. The field set is the one the CloudEvents encoder already publishes as
/// `cdcop` / `cdctable` / `cdcschema` / `cdcsource` / `cdcoffset`, so a deployment reading
/// both sees one vocabulary rather than two spellings of the same five facts.
///
/// Every value is `Some`: krafka distinguishes a null header value from a zero-length one,
/// and each of these is text a consumer reads. A `None` would be a different header on the
/// wire, and the one field that can legitimately be absent — the schema — is omitted
/// entirely instead.
fn cdc_headers(event: &rustcdc::Event) -> Vec<(String, Option<Bytes>)> {
    fn identifier(value: &str) -> Option<Bytes> {
        // Through `crate::text` so the byte budget can never split a character: a header
        // value that is not valid UTF-8 is unreadable by every consumer that assumes text.
        Some(Bytes::from(
            crate::text::truncate_utf8(value, MAX_IDENTIFIER_HEADER_BYTES, "…").into_owned(),
        ))
    }

    let mut headers = Vec::with_capacity(6);
    headers.push((HEADER_OP.to_string(), identifier(event.op.to_str())));
    if let Some(schema) = event.schema.as_deref().filter(|s| !s.is_empty()) {
        headers.push((HEADER_SOURCE_SCHEMA.to_string(), identifier(schema)));
    }
    headers.push((HEADER_SOURCE_TABLE.to_string(), identifier(&event.table)));
    headers.push((
        HEADER_SOURCE_NAME.to_string(),
        identifier(&event.source.source_name),
    ));
    headers.push((
        HEADER_SOURCE_OFFSET.to_string(),
        identifier(&event.source.offset),
    ));
    headers.push((
        HEADER_SOURCE_TS_MS.to_string(),
        identifier(&event.source.timestamp.to_string()),
    ));
    headers
}

/// Render a list of topic names for an error message or a log field.
///
/// One rendering, so the singular and plural forms of every preflight message agree
/// about quoting.
fn quoted_list(topics: &[String]) -> String {
    topics
        .iter()
        .map(|topic| format!("'{topic}'"))
        .collect::<Vec<_>>()
        .join(", ")
}

pub struct KafkaSink {
    producer: KafkaProducerClient,
    /// Resolves each event's topic from `sink.kafka.topic` — a constant for a literal
    /// name, a per-table render for a template. See [`crate::topic`].
    topics: TopicResolver,
    /// Concrete tables the configuration already names, rendered at preflight so a
    /// templated topic still fails at startup for the tables that are known then.
    ///
    /// Empty for a literal topic, which needs no per-table expansion.
    preflight_tables: Vec<QualifiedTable>,
    /// `sink.kafka.tombstones_on_delete`.
    tombstones_on_delete: bool,
    /// `sink.kafka.record_headers`.
    headers: KafkaRecordHeaders,
    /// Tombstones published, for `rustcdc_sink_kafka_tombstones_total`.
    tombstones_total: u64,
    /// Deletes that carried no row key, so no tombstone could be published.
    ///
    /// Separate from `tombstones_total` because the two answer different questions and
    /// only this one is alertable. "Tombstones are zero" is indistinguishable from "no
    /// rows were deleted", which is the normal state of most tables; "a delete could not
    /// be tombstoned" is unambiguous, and on a compacted topic it is a key that will
    /// never be reclaimed.
    unkeyed_deletes_total: u64,
    /// Tables whose deletes carry no row key, so the "cannot tombstone this" warning is
    /// logged once per table rather than once per delete.
    ///
    /// A keyless table produces a *delete per row* on a table that may have millions of
    /// them; logging per event would turn a configuration warning into an outage of its
    /// own. Bounded by the number of distinct keyless tables, which is bounded by the
    /// schema.
    unkeyed_delete_warned: std::collections::HashSet<String>,
    delivery_guarantee: SinkDeliveryGuarantee,
    /// Guards barrier state transitions (begin / commit / abort) and the
    /// barrier state machine itself.  Merging state into the lock makes the
    /// invariant structural: no code can inspect or modify the barrier state
    /// without holding this guard.  If future refactors introduce concurrent
    /// send/commit paths, the compiler enforces the serialization requirement
    /// rather than relying on call-site discipline.
    ///
    /// `Arc` because the Kafka state backend shares it — see [`KafkaTransactionHandle`].
    barrier: Arc<Mutex<BarrierStateMachine>>,
    closed: bool,
    /// Sends handed to krafka whose broker acknowledgement has not been collected yet.
    ///
    /// Every send used to be awaited to completion before
    /// the next one was created, so the sink produced exactly one record per broker
    /// round-trip: measured at 5 700 events/s against an in-process broker, and far worse
    /// across a real network. Worse, it made `linger_ms` actively harmful — a partially
    /// filled batch had nothing to wait *for*, so each record paid the full linger by
    /// itself and the documentation had to tell operators to leave it at zero.
    ///
    /// See [`SendWindow`] for why this is not a `FuturesOrdered`.
    inflight: SendWindow,
    /// Ceiling on `inflight`, from `sink.kafka.max_pipelined_sends`.
    max_pipelined_sends: usize,
    /// Preflight-check fields: stored at construction for AdminClient use.
    preflight_brokers: String,
    preflight_client_id: String,
    preflight_security: KafkaSecurityConfig,
    preflight_transport: krafka::network::TransportConfig,
    preflight_timeout: Duration,
}

impl KafkaSink {
    pub async fn new(config: &KafkaSinkConfig) -> rustcdc::core::Result<Self> {
        let brokers = config.normalized_brokers();
        if brokers.is_empty() {
            return Err(RtError::ConfigError(
                "sink.kafka.brokers must contain at least one broker".to_string(),
            ));
        }

        // The same parser the config loader ran. Reached again here because a sink can
        // be built from a `KafkaSinkConfig` that never passed through `config::load` —
        // the replay command and the Kafka state backend both do — and a template that
        // is only checked in one of the two paths is a template that is not checked.
        let template = TopicTemplate::parse(&config.topic)
            .map_err(|e| RtError::ConfigError(format!("sink.kafka.{e}")))?;
        config
            .topic_naming
            .validate("sink.kafka.topic_naming")
            .map_err(RtError::ConfigError)?;

        if config.client_id.trim().is_empty() {
            return Err(RtError::ConfigError(
                "sink.kafka.client_id must not be empty".to_string(),
            ));
        }

        if config.ack_timeout_ms == 0 {
            return Err(RtError::ConfigError(
                "sink.kafka.ack_timeout_ms must be > 0".to_string(),
            ));
        }

        if config.retry_backoff_ms == 0 {
            return Err(RtError::ConfigError(
                "sink.kafka.retry_backoff_ms must be > 0".to_string(),
            ));
        }

        if config.retry_max_attempts == 0 {
            return Err(RtError::ConfigError(
                "sink.kafka.retry_max_attempts must be > 0".to_string(),
            ));
        }

        config.security.validate().map_err(RtError::ConfigError)?;

        let auth = config
            .security
            .to_auth_config()
            .map_err(RtError::ConfigError)?;

        // Carries the SOCKS5 proxy and the per-connection in-flight ceiling too — one
        // accessor for every transport setting, so no builder below can be given some of
        // them and silently miss the rest.
        let transport = config.transport.to_krafka().map_err(RtError::ConfigError)?;

        let (producer, delivery_guarantee) = match config.delivery_mode {
            KafkaDeliveryMode::AtLeastOnceIdempotent => {
                let builder = Producer::builder()
                    .bootstrap_servers(brokers.join(","))
                    .client_id(config.client_id.clone())
                    .acks(Acks::All)
                    .idempotent(true)
                    .compression(
                        config
                            .compression
                            .to_krafka()
                            .map_err(RtError::ConfigError)?,
                    )
                    .compression_level(config.compression_level)
                    .batch_size(config.batch_size)
                    .linger(Duration::from_millis(config.linger_ms))
                    .retries(config.retry_max_attempts)
                    .retry_backoff(Duration::from_millis(config.retry_backoff_ms))
                    .request_timeout(Duration::from_millis(config.ack_timeout_ms))
                    .connect_timeout(kafka_connect_timeout(Duration::from_millis(
                        config.ack_timeout_ms,
                    )))
                    .delivery_timeout(Self::delivery_timeout(config))
                    .transport(transport.clone())
                    .auth(auth);
                let producer = builder.build().await.map_err(|e| {
                    RtError::ConfigError(format!("failed to build idempotent krafka producer: {e}"))
                })?;
                (
                    KafkaProducerClient::Idempotent(Arc::new(producer)),
                    SinkDeliveryGuarantee::AtLeastOnceIdempotent,
                )
            }
            KafkaDeliveryMode::Transactional => {
                let transactional_id = config
                    .transactional_id
                    .as_deref()
                    .ok_or_else(|| {
                        RtError::ConfigError(
                            "sink.kafka.transactional_id is required when sink.kafka.delivery_mode=\"transactional\""
                                .to_string(),
                        )
                    })?
                    .trim();

                if transactional_id.is_empty() {
                    return Err(RtError::ConfigError(
                        "sink.kafka.transactional_id must not be empty when sink.kafka.delivery_mode=\"transactional\""
                            .to_string(),
                    ));
                }

                let builder = TransactionalProducer::builder()
                    .bootstrap_servers(brokers.join(","))
                    .client_id(config.client_id.clone())
                    .transactional_id(transactional_id)
                    .transaction_timeout(Duration::from_millis(config.transaction_timeout_ms))
                    .request_timeout(Duration::from_millis(config.ack_timeout_ms))
                    .connect_timeout(kafka_connect_timeout(Duration::from_millis(
                        config.ack_timeout_ms,
                    )))
                    .compression(
                        config
                            .compression
                            .to_krafka()
                            .map_err(RtError::ConfigError)?,
                    )
                    .compression_level(config.compression_level)
                    .batch_size(config.batch_size)
                    .linger(Duration::from_millis(config.linger_ms))
                    .retries(config.retry_max_attempts)
                    .retry_backoff(Duration::from_millis(config.retry_backoff_ms))
                    // Bounds how long a batch may sit in flight. It matters more here
                    // than on the idempotent producer: a stuck batch holds the
                    // transaction open and blocks the checkpoint barrier behind it.
                    .delivery_timeout(Self::delivery_timeout(config))
                    .transport(transport.clone())
                    .auth(auth);
                let producer = builder.build().await.map_err(|e| {
                    RtError::ConfigError(format!(
                        "failed to build transactional krafka producer: {e}"
                    ))
                })?;

                producer.init_transactions().await.map_err(|e| {
                    RtError::SourceError(format!(
                        "failed to initialize transactional producer state: {e}"
                    ))
                })?;

                (
                    KafkaProducerClient::Transactional(Arc::new(producer)),
                    SinkDeliveryGuarantee::EffectivelyOnce,
                )
            }
        };

        // One line at construction saying what the topic layout is. An operator reading
        // a startup log should not have to infer "one topic" versus "one per table" from
        // the absence of a message, and the character policy is the setting most likely
        // to be wondered about after the first dead-lettered event.
        if template.is_templated() {
            tracing::info!(
                topic_template = %config.topic,
                invalid_characters = ?config.topic_naming.invalid_characters,
                "kafka sink: the topic is resolved per event, one topic per distinct \
                 rendering of the template"
            );
        }

        Ok(Self {
            producer,
            topics: TopicResolver::new(template, config.topic_naming.clone()),
            preflight_tables: Vec::new(),
            tombstones_on_delete: config.tombstones_on_delete,
            headers: config.record_headers,
            tombstones_total: 0,
            unkeyed_deletes_total: 0,
            unkeyed_delete_warned: std::collections::HashSet::new(),
            delivery_guarantee,
            barrier: Arc::new(Mutex::new(BarrierStateMachine::new())),
            closed: false,
            inflight: SendWindow::default(),
            max_pipelined_sends: config.max_pipelined_sends.max(1),
            preflight_brokers: config.normalized_brokers().join(","),
            preflight_client_id: format!("{}-preflight", config.client_id),
            preflight_security: config.security.clone(),
            preflight_transport: transport,
            preflight_timeout: Duration::from_millis(config.ack_timeout_ms.max(5_000)),
        })
    }

    /// Name the concrete tables preflight should render this sink's topic template
    /// against.
    ///
    /// Separate from [`new`](Self::new) because the table set is a property of the
    /// *pipeline* — `snapshot_tables`, `incremental_snapshot.tables` and the non-glob
    /// entries of the source's `table_include_list` — not of the sink. A sink built
    /// without it still preflights the broker connection; it just has no per-table
    /// topics to check.
    pub fn with_preflight_tables(mut self, tables: Vec<QualifiedTable>) -> Self {
        self.preflight_tables = tables;
        self
    }

    /// The topic this event belongs on.
    ///
    /// A render failure is a property of the event's own identifiers — a schema the
    /// event does not carry, a table name Kafka cannot spell — so it surfaces as
    /// `ValidationError`, which [`AppError::is_dead_letterable`] treats as
    /// quarantine-and-continue. A *collision* is not: two tables are equally
    /// responsible, quarantining one of them would drain a healthy table into the DLQ,
    /// and the merge it prevents is silent data corruption. That surfaces as
    /// `ConfigError`, which halts.
    pub fn topic_for(&mut self, event: &rustcdc::Event) -> rustcdc::core::Result<Arc<str>> {
        self.topics
            .resolve(event.schema.as_deref(), &event.table)
            .map_err(|error| match error {
                TopicResolveError::Render(_) => RtError::ValidationError(vec![error.to_string()]),
                TopicResolveError::Collision { .. } => RtError::ConfigError(error.to_string()),
            })
    }

    /// Follow a delivered delete with a tombstone, when this event calls for one.
    ///
    /// `row_key` is the key the **codec** produced, before `SinkBinding::send_event`
    /// substitutes its qualified-table-name fallback. That distinction is the whole rule: a
    /// tombstone is a statement about a *row*, and the fallback key names a *table*.
    ///
    /// `row_key.is_some()` therefore excludes three cases, each of which would destroy data
    /// rather than prune it: `truncate` and `schema_change` (keyed by table name — a
    /// tombstone would compact away the marker itself), and a table with no primary key
    /// (every event shares one key, so a tombstone would erase its whole history). Testing
    /// `event.op` and `primary_key_values()` separately reaches the same answer today and
    /// drifts the moment a codec derives keys some other way.
    ///
    /// Durability, transactions and the DLQ are settled by *where this sits*. The tombstone
    /// enters the same send window as its delete, so the batch's flush covers both and the
    /// checkpoint cannot advance past a delete whose tombstone is missing; under
    /// `effectively_once` the barrier's transaction spans the batch, so both join it. And a
    /// tombstone cannot exceed `runtime.max_event_bytes` when the delete did not — same key,
    /// no value.
    pub async fn send_delete_tombstone(
        &mut self,
        event: &rustcdc::Event,
        row_key: Option<&Bytes>,
    ) -> rustcdc::core::Result<()> {
        if !self.tombstones_on_delete || event.op != rustcdc::Operation::Delete {
            return Ok(());
        }

        let Some(key) = row_key else {
            self.unkeyed_deletes_total = self.unkeyed_deletes_total.saturating_add(1);
            self.warn_unkeyed_delete(event);
            return Ok(());
        };

        // Resolved again rather than passed in: the resolver caches per table, so this is
        // a hash lookup, and asking it twice is what guarantees the tombstone lands on the
        // same topic as the delete even if the template is ever made richer.
        let topic = self.topic_for(event)?;
        self.send_tombstone(event, &topic, key.clone())
            .await
            .map_err(escalate_tombstone_failure)?;
        self.tombstones_total = self.tombstones_total.saturating_add(1);
        Ok(())
    }

    /// Say once per table that its deletes cannot be tombstoned.
    fn warn_unkeyed_delete(&mut self, event: &rustcdc::Event) {
        let table = event.qualified_table_name();
        if !self.unkeyed_delete_warned.insert(table.clone()) {
            return;
        }
        tracing::warn!(
            table = %table,
            "table has no primary key, so its deletes carry no row key and cannot be \
             tombstoned; note that such a table cannot be consumed from a compacted \
             topic at all — every one of its events shares the qualified-table-name key, \
             so compaction would retain only the newest one"
        );
    }

    /// Tombstones published by this sink.
    pub fn tombstones_total(&self) -> u64 {
        self.tombstones_total
    }

    /// Deletes this sink could not tombstone because the event carried no row key.
    pub fn unkeyed_deletes_total(&self) -> u64 {
        self.unkeyed_deletes_total
    }

    /// Connection-level counters from the underlying krafka producer.
    ///
    /// Both producer flavours expose the same handle, so the OAUTHBEARER token
    /// counters are available regardless of delivery mode.
    pub fn connection_metrics(&self) -> krafka::metrics::ConnectionMetricsSnapshot {
        match &self.producer {
            KafkaProducerClient::Idempotent(producer) => producer.connection_metrics().snapshot(),
            KafkaProducerClient::Transactional(producer) => {
                producer.connection_metrics().snapshot()
            }
        }
    }

    fn delivery_timeout(config: &KafkaSinkConfig) -> Duration {
        let retry_budget = config
            .retry_backoff_ms
            .saturating_mul(config.retry_max_attempts as u64);
        Duration::from_millis(
            config
                .ack_timeout_ms
                .saturating_add(retry_budget)
                .saturating_add(5_000),
        )
    }

    /// Verify that the Kafka topics this sink will write to exist and are reachable, using
    /// a short-lived AdminClient so the main producer is not involved.
    ///
    /// A literal topic is described directly. A template has no finite topic set, so it is
    /// rendered against the tables the configuration already names
    /// ([`with_preflight_tables`](Self::with_preflight_tables)) and those are described —
    /// a partial check, and the log line says so. It still catches an unreachable broker
    /// and a topic-per-table layout whose topics were never created; it cannot catch a
    /// table that first appears at runtime, short of auto-creating topics.
    ///
    /// A named table that cannot be *rendered* is a startup warning rather than a failure:
    /// it will dead-letter if it ever produces an event, and failing the pipeline for it
    /// would take out every healthy table alongside it.
    pub async fn preflight_check(&mut self) -> Result<(), AppError> {
        let auth = self
            .preflight_security
            .to_auth_config()
            .map_err(AppError::Other)?;

        let admin = AdminClient::builder()
            .bootstrap_servers(self.preflight_brokers.clone())
            .client_id(self.preflight_client_id.clone())
            .request_timeout(self.preflight_timeout)
            .connect_timeout(kafka_connect_timeout(self.preflight_timeout))
            .transport(self.preflight_transport.clone())
            .auth(auth)
            .build()
            .await
            .map_err(|e| {
                AppError::Other(format!(
                    "sink preflight: failed to connect to Kafka brokers '{}': {e}",
                    self.preflight_brokers
                ))
            })?;

        let template = self.topics.template().source().to_string();
        let templated = self.topics.template().is_templated();

        let expected: Vec<String> = match self.topics.template().as_literal() {
            Some(literal) => vec![literal.to_string()],
            None => {
                // Moved out and put back rather than cloned: `warm` needs `&mut
                // self.topics` and the table list is `&self.preflight_tables`, which the
                // borrow checker will not grant together. Restored rather than consumed
                // so a second preflight — a reconnect, a test — checks the same set.
                let tables = std::mem::take(&mut self.preflight_tables);
                let (resolved, skipped) = self.topics.warm(&tables);
                self.preflight_tables = tables;

                for reason in &skipped {
                    tracing::warn!(
                        topic_template = %template,
                        reason = %reason,
                        "sink preflight: a configured table has no representable Kafka \
                         topic; events from it will be dead-lettered when they arrive"
                    );
                }

                if resolved.is_empty() {
                    // Not a failure: a stream-only pipeline names no tables up front,
                    // and a glob include-list names patterns rather than tables. Saying
                    // so is the whole point — a silent no-op check reads exactly like a
                    // check that passed.
                    tracing::info!(
                        topic_template = %template,
                        brokers = %self.preflight_brokers,
                        "sink preflight: Kafka brokers reachable; the topic template \
                         names no topics that can be checked up front, so per-topic \
                         existence is verified on first use by the broker itself"
                    );
                    return Ok(());
                }
                resolved
            }
        };

        let found = admin.describe_topics(&expected).await.map_err(|e| {
            AppError::Other(format!(
                "sink preflight: failed to describe Kafka topic(s) {}: {e}",
                quoted_list(&expected)
            ))
        })?;

        let missing: Vec<String> = expected
            .iter()
            .filter(|topic| !found.iter().any(|t| t.0 == topic.as_str()))
            .cloned()
            .collect();

        if !missing.is_empty() {
            return Err(AppError::Other(if templated {
                format!(
                    "sink preflight: Kafka topic(s) {} not found on brokers '{}'. They \
                     are the topics sink.kafka.topic = \"{template}\" renders for the \
                     tables this configuration names; create them, or narrow the \
                     template.",
                    quoted_list(&missing),
                    self.preflight_brokers
                )
            } else {
                format!(
                    "sink preflight: Kafka topic {} not found on brokers '{}'",
                    quoted_list(&missing),
                    self.preflight_brokers
                )
            }));
        }

        if templated {
            tracing::info!(
                topic_template = %template,
                topics = %quoted_list(&expected),
                checked = expected.len(),
                brokers = %self.preflight_brokers,
                "sink preflight: every Kafka topic this configuration names up front is \
                 reachable; tables discovered at runtime are not covered by this check"
            );
        } else {
            tracing::info!(
                topic = %expected[0],
                brokers = %self.preflight_brokers,
                "sink preflight: Kafka topic reachable"
            );
        }
        Ok(())
    }

    /// Hand one record to the producer and return a future for its acknowledgement.
    ///
    /// `Bytes` is refcounted, so neither the key nor the value is copied here. The old
    /// `send(&topic, Some(&key), &value)` path took byte slices and krafka's convenience
    /// wrapper did a `Bytes::copy_from_slice` of each — a full duplicate of every payload,
    /// per record, discarded immediately after the accumulator took ownership.
    ///
    /// # The two arms differ, and the difference is the ordering guarantee
    ///
    /// **Idempotent:** `enqueue` performs the accumulator append *before it returns*, so
    /// produce order is call order — settled here, at submission, and not dependent on how
    /// the returned handles are polled afterwards. This path does not need [`SendWindow`]'s
    /// poll sweep.
    ///
    /// **Transactional:** `TransactionalProducer::enqueue` returns a handle that borrows
    /// the producer, so it cannot be stored in the same struct that owns the producer
    /// without a self-reference — and `unsafe_code = "forbid"` rules out the usual
    /// escapes. This arm therefore still boxes a future that appends when first polled,
    /// and still depends on the ordered sweep. Raised upstream as F-5; when an owned
    /// transactional handle exists, this arm collapses into the one above and
    /// [`SendWindow`] loses its reason to exist.
    ///
    /// Takes its inputs by value rather than through `&self`: an `InFlightSend` is
    /// `Send` but not `Sync`, so holding `&KafkaSink` across the enqueue await would make
    /// the whole `SinkAdapter::send` future non-`Send` and fail the trait's bound.
    async fn build_send(
        producer: KafkaProducerClient,
        topic: String,
        key: Bytes,
        value: Option<Bytes>,
        headers: Vec<(String, Option<Bytes>)>,
    ) -> krafka::error::Result<InFlightSend> {
        // `with_key` unconditionally, including for an empty key. An empty key and an
        // absent key partition differently — murmur2("") pins one partition, absent
        // round-robins — and the original path always passed `Some(key)`. Changing that
        // here would silently repartition an existing topic.
        //
        // `None` is a **tombstone**: Kafka's null value, a `-1` length prefix on the
        // wire, which marks the key for deletion on a `cleanup.policy=compact` topic.
        // `Some(Bytes::new())` would be a zero-length value — an ordinary record that
        // compaction preserves — which is why this is `Option` rather than an empty
        // `Bytes` by convention.
        let mut record = match value {
            Some(value) => ProducerRecord::new(topic, value).with_key(key),
            None => ProducerRecord::tombstone(topic, key),
        };
        record.headers = headers;
        match producer {
            KafkaProducerClient::Idempotent(producer) => {
                let handle = producer.enqueue(record).await?;
                Ok(Box::pin(handle))
            }
            KafkaProducerClient::Transactional(producer) => {
                Ok(Box::pin(
                    async move { producer.enqueue(record).await?.await },
                ))
            }
        }
    }

    /// Convert one collected acknowledgement into this layer's error taxonomy.
    fn settle(result: krafka::error::Result<RecordMetadata>) -> rustcdc::core::Result<()> {
        match result {
            Ok(metadata) => enforce_durable_confirmation(&metadata, "sink"),
            Err(error) => Err(RtError::from(classify_krafka_error(
                &error,
                "sink delivery failed",
            ))),
        }
    }

    /// Collect the oldest outstanding acknowledgement, driving every other outstanding
    /// send forward in the process.
    async fn await_oldest_send(&mut self) -> rustcdc::core::Result<()> {
        let Some(result) = self.inflight.next_completed().await else {
            return Ok(());
        };
        if result.is_err() {
            // Abandon the rest of the window. The dropped futures are cancelled, but a
            // record they already appended may still be delivered — under
            // `at_least_once_idempotent` that is a duplicate the producer's sequence
            // numbers collapse, and under `effectively_once` the aborted barrier hides it.
            // Retaining them instead would report this batch's failure against the *next*
            // batch's send.
            self.inflight.abandon();
        }
        Self::settle(result)
    }

    /// Collect every outstanding acknowledgement, in submission order.
    ///
    /// This is the durability boundary. `process_batch_events` calls `flush` before the
    /// batch returns, and only a returned `Ok` lets `run_loop_batch` commit the
    /// checkpoint — so no checkpoint can advance past a send whose acknowledgement was
    /// never collected.
    async fn drain_inflight(&mut self) -> rustcdc::core::Result<()> {
        let mut first_error = None;
        while let Some(result) = self.inflight.next_completed().await {
            // Keep draining after the first failure: the remaining records are already in
            // flight, and collecting them is what stops one racing a subsequent
            // transaction commit. Only the first error — the earliest record to fail —
            // is reported.
            if let Err(err) = Self::settle(result) {
                first_error.get_or_insert(err);
            }
        }
        match first_error {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }

    /// Accept an encoded record for delivery, without waiting for its acknowledgement.
    ///
    /// `topic` is a per-event value once `sink.kafka.topic` carries placeholders; callers
    /// get it from [`topic_for`](Self::topic_for), which renders and caches.
    ///
    /// `event` is required even though the bytes are already encoded, because the record's
    /// *headers* come from it. There is deliberately no header-free variant: a second entry
    /// point into the producer would be a second set of headers to keep in step, and the one
    /// that got forgotten would publish records a consumer cannot filter — silently, since a
    /// missing header reads exactly like a consumer bug.
    ///
    /// Ordering and the send window are [`enqueue`](Self::enqueue)'s contract.
    pub async fn send_encoded(
        &mut self,
        event: &rustcdc::Event,
        topic: &str,
        key: Bytes,
        value: Bytes,
    ) -> rustcdc::core::Result<()> {
        let headers = self.headers_for(event);
        self.enqueue(topic, key, Some(value), headers).await
    }

    /// The configured headers for one event, or nothing when `headers = "none"`.
    fn headers_for(&self, event: &rustcdc::Event) -> Vec<(String, Option<Bytes>)> {
        match self.headers {
            KafkaRecordHeaders::Cdc => cdc_headers(event),
            KafkaRecordHeaders::None => Vec::new(),
        }
    }

    /// Publish a **tombstone**: `key` with Kafka's null value.
    ///
    /// On a `cleanup.policy=compact` topic this marks the key for deletion, so log
    /// compaction eventually removes every earlier record for that row and then the
    /// tombstone itself. Without one, a deleted row's last record is retained forever and
    /// a consumer rebuilding state from the topic sees the delete event but never sees
    /// the key disappear.
    ///
    /// Goes through the same window as every other record — same ordering guarantee, so a
    /// tombstone can never overtake the delete it follows — which is the whole reason it
    /// is not a separate producer call.
    pub async fn send_tombstone(
        &mut self,
        event: &rustcdc::Event,
        topic: &str,
        key: Bytes,
    ) -> rustcdc::core::Result<()> {
        // The headers matter more here than anywhere else. A tombstone has a key and a
        // null value, so without them nothing on the record says which table it came
        // from, which operation produced it, or where in the source log that happened.
        // On a topic-per-table layout the topic name recovers the table; on a single
        // topic nothing does, and a Debezium tombstone is opaque for exactly this reason.
        let headers = self.headers_for(event);
        self.enqueue(topic, key, None, headers).await
    }

    /// The one path into the send window. `None` is a tombstone; see [`build_send`].
    ///
    /// Records and tombstones share it deliberately. Two entry points into the producer
    /// would be two orderings, and the invariant that a tombstone follows its delete
    /// depends on there being exactly one.
    ///
    /// # Ordering
    ///
    /// Kafka's per-partition ordering is what CDC consumers depend on, and pipelining is
    /// only sound if records reach krafka's accumulator in submission order. That holds
    /// here by construction:
    ///
    /// 1. Each future is created and polled **once, in submission order**, before the
    ///    next one exists. That first poll is what carries a record into the accumulator,
    ///    or — if the accumulator is saturated — what claims its place in the queue.
    /// 2. Every suspension point on that path is a tokio primitive with documented FIFO
    ///    fairness: the buffer-memory semaphore in `Accumulator::append_routed_with_guard`,
    ///    the accumulator's mpsc channel, and the in-flight semaphore on the direct path.
    ///    Record *i* takes its ticket before record *i + 1* exists, so it is served first
    ///    however the two are polled afterwards.
    ///
    /// That second point is what makes it safe for `inflight` to poll every outstanding
    /// send concurrently: once the tickets are drawn in order, poll order cannot change
    /// the outcome. The ordered *first* poll is the entire guarantee, and it is why this
    /// polls manually instead of pushing an unpolled future onto the queue.
    ///
    /// The clean fix belongs upstream — krafka should expose Java's
    /// `send(record) -> Future<RecordMetadata>`, which separates *enqueue* from *await*
    /// explicitly instead of leaving both inside one opaque future, which would make this
    /// invariant structural rather than argued.
    async fn enqueue(
        &mut self,
        topic: &str,
        key: Bytes,
        value: Option<Bytes>,
        headers: Vec<(String, Option<Bytes>)>,
    ) -> rustcdc::core::Result<()> {
        if self.closed {
            return Err(RtError::StateError("sink is closed".to_string()));
        }

        // Bound the window *before* widening it, so `max_pipelined_sends` is a ceiling on
        // outstanding records rather than on records-plus-one.
        while self.inflight.len() >= self.max_pipelined_sends {
            self.await_oldest_send().await?;
        }

        // An enqueue failure is the record never reaching the accumulator at all —
        // validation, an unknown topic, or the bounded wait for buffer memory expiring.
        // It is this record's outcome and belongs to this call, not to the window.
        let mut send = match Self::build_send(
            self.producer.clone(),
            topic.to_string(),
            key,
            value,
            headers,
        )
        .await
        {
            Ok(send) => send,
            Err(error) => return Self::settle(Err(error)),
        };
        match futures::poll!(send.as_mut()) {
            // Already settled — a small record into a warm accumulator with memory
            // available can complete without ever suspending.
            Poll::Ready(result) if self.inflight.is_empty() => Self::settle(result),
            // Settled, but out of turn: reporting it now would attribute this record's
            // outcome ahead of records submitted before it. Park it so results are
            // reported in the same order the records were accepted.
            Poll::Ready(result) => {
                self.inflight.push_done(result);
                Ok(())
            }
            Poll::Pending => {
                self.inflight.push(send);
                Ok(())
            }
        }
    }

    pub async fn flush(&mut self) -> rustcdc::core::Result<()> {
        if self.closed {
            return Err(RtError::StateError("sink is closed".to_string()));
        }
        // Acknowledgements first: krafka's `flush` drains its own accumulator, but a
        // record still pending on *our* side has not reached the accumulator yet, so
        // flushing before draining would return success with records still unsent.
        self.drain_inflight().await?;
        match &mut self.producer {
            KafkaProducerClient::Idempotent(producer) => producer
                .flush()
                .await
                .map_err(|e| RtError::SourceError(format!("Kafka sink flush failed: {e}"))),
            // Drains the accumulator and waits for the in-flight batches, but does
            // **not** make the records visible to a `read_committed` consumer — only
            // `commit_checkpoint_barrier` does that. Draining here is still what makes
            // the runtime's flush timeout bound anything on this path: before krafka
            // 0.16 exposed a transactional `flush`, this arm returned immediately while
            // the records sat in the accumulator, so a stalled broker looked like a
            // completed flush.
            KafkaProducerClient::Transactional(producer) => producer.flush().await.map_err(|e| {
                RtError::SourceError(format!("Kafka transactional sink flush failed: {e}"))
            }),
        }
    }

    pub async fn close(&mut self) -> rustcdc::core::Result<()> {
        if self.closed {
            return Ok(());
        }
        // Collect what is still outstanding before tearing the producer down, so a
        // shutdown cannot silently drop an accepted record. A failure here is reported,
        // but must not skip the close: leaking the producer's connections and its
        // background accumulator task would be the worse outcome.
        let drain = self.drain_inflight().await;
        match &self.producer {
            KafkaProducerClient::Idempotent(producer) => {
                producer
                    .close_with_timeout(Duration::from_secs(5))
                    .await
                    .map_err(|e| RtError::SourceError(format!("Kafka sink close failed: {e}")))?;
            }
            KafkaProducerClient::Transactional(producer) => {
                producer
                    .close_with_timeout(Duration::from_secs(5))
                    .await
                    .map_err(|e| {
                        RtError::SourceError(format!("Kafka transactional sink close failed: {e}"))
                    })?;
            }
        }
        self.barrier.lock().await.mark_closed();
        self.closed = true;
        drain
    }

    pub fn is_closed(&self) -> bool {
        self.closed
    }

    pub fn delivery_guarantee(&self) -> SinkDeliveryGuarantee {
        self.delivery_guarantee
    }

    pub fn transactional_checkpoint_barrier_capable(&self) -> bool {
        matches!(self.producer, KafkaProducerClient::Transactional(_))
    }

    /// A share in this sink's transaction, for the Kafka state backend.
    ///
    /// `None` for an idempotent producer: there is no transaction to join, and
    /// `effectively_once` is rejected at load for that delivery mode anyway.
    pub fn transaction_handle(&self) -> Option<KafkaTransactionHandle> {
        match &self.producer {
            KafkaProducerClient::Idempotent(_) => None,
            KafkaProducerClient::Transactional(producer) => Some(KafkaTransactionHandle {
                producer: Arc::clone(producer),
                barrier: Arc::clone(&self.barrier),
            }),
        }
    }

    pub async fn begin_checkpoint_barrier(&mut self) -> rustcdc::core::Result<()> {
        let mut barrier = self.barrier.lock().await;

        if self.closed {
            return Err(RtError::StateError("sink is closed".to_string()));
        }

        match &self.producer {
            KafkaProducerClient::Idempotent(_) => Ok(()),
            KafkaProducerClient::Transactional(producer) => {
                barrier.ensure_can_begin()?;

                producer.begin_transaction().map_err(|e| {
                    RtError::SourceError(format!(
                        "failed to begin transactional checkpoint barrier: {e}"
                    ))
                })?;
                // Transition: NotActive → Active
                barrier.mark_active();
                Ok(())
            }
        }
    }

    pub async fn commit_checkpoint_barrier(&mut self) -> rustcdc::core::Result<()> {
        // Before the lock, because draining needs `&mut self`.
        //
        // Load-bearing for `effectively_once`: a pipelined record that has not yet
        // reached the accumulator would otherwise be appended *after* `EndTxn` and land
        // in the following transaction, while the checkpoint this commit unblocks claims
        // the batch is complete. `commit_transaction` flushes krafka's accumulator, but
        // it cannot flush a record that has not arrived there yet.
        if self.transactional_checkpoint_barrier_capable() {
            self.drain_inflight().await?;
        }

        let mut barrier = self.barrier.lock().await;

        if self.closed {
            return Err(RtError::StateError("sink is closed".to_string()));
        }

        match &self.producer {
            KafkaProducerClient::Idempotent(_) => Ok(()),
            KafkaProducerClient::Transactional(producer) => {
                barrier.ensure_can_commit()?;

                producer.commit_transaction().await.map_err(|e| {
                    RtError::SourceError(format!(
                        "failed to commit transactional checkpoint barrier: {e}"
                    ))
                })?;
                // Transition: Active → NotActive
                barrier.mark_not_active();
                Ok(())
            }
        }
    }

    pub async fn abort_checkpoint_barrier(&mut self) -> rustcdc::core::Result<()> {
        // Drop rather than drain: the transaction these records belong to is being
        // discarded, so their acknowledgements carry no information and awaiting them
        // would block the abort behind the very broker that is probably why we are
        // aborting. Cancelling the futures does not un-append a record already in the
        // accumulator — `abort_transaction` is what makes those invisible.
        self.inflight.abandon();

        let mut barrier = self.barrier.lock().await;

        if self.closed {
            return Err(RtError::StateError("sink is closed".to_string()));
        }

        match &self.producer {
            KafkaProducerClient::Idempotent(_) => Ok(()),
            KafkaProducerClient::Transactional(producer) => {
                // Only abort if we're actually Active; idempotent is safe to call redundantly
                if !barrier.is_active() {
                    // Already NotActive, no-op (safe to call multiple times)
                    return Ok(());
                }

                producer.abort_transaction().await.map_err(|e| {
                    RtError::SourceError(format!(
                        "failed to abort transactional checkpoint barrier: {e}"
                    ))
                })?;
                // Transition: Active → NotActive
                barrier.mark_not_active();
                Ok(())
            }
        }
    }
}

// ── Schema registry helpers ───────────────────────────────────────────────────

/// Cap the TCP connect timeout at the request timeout.
///
/// krafka ≥ 0.13 validates `request_timeout >= connect_timeout` at build time
/// (default connect timeout: 10 s). This server deliberately runs short request
/// budgets for local state and preflight operations; a connection that cannot
/// be established within the request budget is useless to that request anyway,
/// so the connect timeout follows the request timeout downward.
pub(crate) fn kafka_connect_timeout(request_timeout: Duration) -> Duration {
    request_timeout.min(Duration::from_secs(10))
}

/// Classify a krafka failure into the three responses the pipeline can actually take.
///
/// Every Kafka failure used to become `RtError::SourceError`, which upstream classifies
/// as `Transient`. That single mapping produced two distinct defects:
///
/// * A **poison record** — one the broker will reject identically forever, such as
///   `MessageTooLarge` or `InvalidRecord` — was retried until the circuit breaker gave
///   up, then killed the process, then failed again on the same record after restart.
///   The dead-letter queue added in Round 4 was unreachable from this sink, because its
///   branch is only taken for a non-recoverable error and this sink never produced one.
/// * A **fatal misconfiguration** — bad SASL credentials, a revoked topic ACL — was also
///   retried forever, so `TopicAuthorizationFailed` looked exactly like a broker restart.
///
/// The three-way split maps onto [`AppError::is_recoverable`] and
/// [`AppError::is_dead_letterable`]:
///
/// | Class | Example | Response |
/// |---|---|---|
/// | Retriable | `LeaderNotAvailable`, `RequestTimedOut`, network | back off, retry the batch |
/// | Poison record | `MessageTooLarge`, `InvalidRecord` | quarantine the event, advance |
/// | Fatal | `TopicAuthorizationFailed`, `Auth`, `Config` | halt and page |
///
/// The unclassified default is **fatal**, not poison: halting is loud and recoverable by
/// a human, whereas guessing "poison" silently drains the change stream into the DLQ.
pub(crate) fn classify_krafka_error(error: &krafka::error::KrafkaError, context: &str) -> AppError {
    use krafka::error::{ErrorCode, KrafkaError};

    let message = format!("Kafka {context}: {error}");

    if error.is_retriable() {
        // `TimeoutError` is rustcdc's Transient variant — the batch loop backs off and
        // retries the same events.
        return AppError::SinkTimeout(message);
    }

    match error {
        // Properties of the payload. The same bytes fail identically on every attempt,
        // so the only way forward is to quarantine this event and advance past it.
        KrafkaError::Broker {
            code:
                ErrorCode::MessageTooLarge
                | ErrorCode::RecordListTooLarge
                | ErrorCode::InvalidRecord
                | ErrorCode::InvalidTimestamp,
            ..
        } => AppError::SinkPoisonRecord(message),
        // A record we could not even encode is equally the record's fault.
        KrafkaError::Serialization { .. } | KrafkaError::Compression { .. } => {
            AppError::SinkPoisonRecord(message)
        }
        // Everything else that is not retriable is environmental: credentials, ACLs,
        // topic configuration, protocol mismatch. Retrying will not help and neither
        // will skipping the event, because the next one fails the same way.
        _ => AppError::SinkFatal(message),
    }
}

/// Durability tripwire on the send acknowledgement (krafka ≥ 0.13).
///
/// `RecordMetadata::delivery` states what the broker actually confirmed —
/// `offset == -1` alone cannot distinguish "idempotent-deduplicated (durable)"
/// from "acks = 0 (no guarantee at all)". Every producer in this server is
/// built with `acks = All`, so an `Unacknowledged` confirmation can only mean a
/// misconfiguration slipped through — fail the send instead of silently
/// downgrading the delivery contract.
pub(crate) fn enforce_durable_confirmation(
    metadata: &krafka::producer::RecordMetadata,
    context: &str,
) -> rustcdc::core::Result<()> {
    if metadata.is_unacknowledged() {
        return Err(RtError::StateError(format!(
            "Kafka {context} send returned an unacknowledged delivery confirmation \
             (acks = 0 semantics); refusing to treat it as durable"
        )));
    }
    if metadata.is_deduplicated() {
        tracing::debug!(
            topic = %metadata.topic,
            partition = metadata.partition,
            "Kafka {context} send deduplicated by idempotent producer — data already durable"
        );
    }
    Ok(())
}

// `KafkaSink` deliberately does **not** implement `SinkAdapter`.
//
// It did, and the impl was dead — nothing ever built a `BoxedSink` from a bare
// `KafkaSink`, because the router holds `SinkBinding`s. What it would have done if anyone
// had reached for it was wrong three times over: it serialised the event as JSON directly,
// ignoring the sink's configured codec; it passed an **empty** message key, which Kafka
// partitioners hash like any other key, pinning every event across every table to one
// murmur2("") partition — the exact defect `SinkBinding::send_event`'s key fallback exists
// to prevent; and it published no tombstone after a delete.
//
// A correct Kafka send needs the codec, and the codec lives in `SinkBinding`. Driving the
// sink through the binding is the only way to get one, so the trait impl that suggested
// otherwise is gone rather than repaired. Compile errors are better documentation than a
// comment on a footgun.

#[cfg(test)]
#[path = "kafka_tests.rs"]
mod tests;
