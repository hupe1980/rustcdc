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

use crate::config::schema::{KafkaDeliveryMode, KafkaSecurityConfig, KafkaSinkConfig};
use crate::error::AppError;

use super::SinkDeliveryGuarantee;

/// An accepted-but-unconfirmed send.
///
/// `'static` — hence the `Arc` around each producer — so it can outlive the
/// `send_encoded` call that created it and sit in [`KafkaSink::inflight`].
type InFlightSend = Pin<Box<dyn Future<Output = krafka::error::Result<RecordMetadata>> + Send>>;

/// Producers are held behind `Arc` so a send future can be detached from the
/// `&mut self` that created it. krafka's `send` takes `&self`, so this costs nothing
/// but the refcount and is what makes pipelining expressible at all.
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
/// This is not theoretical. `pipelined_sends_reach_the_partition_in_submission_order`
/// caught it with `FuturesOrdered` in place: 200 records arrived as `0..62`, then
/// `199..136` reversed, then `63..126`, then `135..127` reversed. Reversed runs of exactly
/// the records that had queued behind accumulator capacity. For a CDC stream that is
/// silent replica corruption — `balance=50` applied after `balance=100`.
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
                if let SendSlot::Pending(send) = slot {
                    if let Poll::Ready(result) = send.as_mut().poll(cx) {
                        *slot = SendSlot::Done(result);
                    }
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

        let result = self.producer.send(topic, Some(key), payload).await;

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

pub struct KafkaSink {
    producer: KafkaProducerClient,
    topic: String,
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

        if config.topic.trim().is_empty() {
            return Err(RtError::ConfigError(
                "sink.kafka.topic must not be empty".to_string(),
            ));
        }

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

        let transport = config.transport.to_krafka().map_err(RtError::ConfigError)?;

        let (producer, delivery_guarantee) = match config.delivery_mode {
            KafkaDeliveryMode::AtLeastOnceIdempotent => {
                let producer = Producer::builder()
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
                    .max_in_flight(config.max_in_flight)
                    .transport(transport.clone())
                    .auth(auth)
                    .build()
                    .await
                    .map_err(|e| {
                        RtError::ConfigError(format!(
                            "failed to build idempotent krafka producer: {e}"
                        ))
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

                let producer = TransactionalProducer::builder()
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
                    .max_in_flight(config.max_in_flight)
                    .retries(config.retry_max_attempts)
                    .retry_backoff(Duration::from_millis(config.retry_backoff_ms))
                    // Bounds how long a batch may sit in flight. It matters more here
                    // than on the idempotent producer: a stuck batch holds the
                    // transaction open and blocks the checkpoint barrier behind it.
                    .delivery_timeout(Self::delivery_timeout(config))
                    .transport(transport.clone())
                    .auth(auth)
                    .build()
                    .await
                    .map_err(|e| {
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

        Ok(Self {
            producer,
            topic: config.topic.clone(),
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

    /// Verify that the configured Kafka topic exists and is reachable before
    /// the pipeline starts processing events.  Uses a short-lived AdminClient
    /// so the main producer is not involved.
    /// `&mut self` rather than `&self` so the returned future needs only
    /// `KafkaSink: Send`. Holding `&self` across an await would demand `Sync`, which the
    /// in-flight send futures cannot provide — and preflight is exclusive anyway: it runs
    /// once, before the pipeline is marked ready.
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

        let topics = admin
            .describe_topics(std::slice::from_ref(&self.topic))
            .await
            .map_err(|e| {
                AppError::Other(format!(
                    "sink preflight: failed to describe Kafka topic '{}': {e}",
                    self.topic
                ))
            })?;

        if !topics.iter().any(|t| t.0 == self.topic.as_str()) {
            return Err(AppError::Other(format!(
                "sink preflight: Kafka topic '{}' not found on brokers '{}'",
                self.topic, self.preflight_brokers
            )));
        }

        tracing::info!(
            topic = %self.topic,
            brokers = %self.preflight_brokers,
            "sink preflight: Kafka topic reachable"
        );
        Ok(())
    }

    /// Build the send future without polling it.
    ///
    /// `Bytes` is refcounted, so neither the key nor the value is copied here. The old
    /// `send(&topic, Some(&key), &value)` path took byte slices and krafka's convenience
    /// wrapper did a `Bytes::copy_from_slice` of each — a full duplicate of every payload,
    /// per record, discarded immediately after the accumulator took ownership.
    fn build_send(&self, key: Bytes, value: Bytes) -> InFlightSend {
        let topic = self.topic.clone();
        // `with_key` unconditionally, including for an empty key. An empty key and an
        // absent key partition differently — murmur2("") pins one partition, absent
        // round-robins — and the original path always passed `Some(key)`. Changing that
        // here would silently repartition an existing topic.
        match &self.producer {
            KafkaProducerClient::Idempotent(producer) => {
                let producer = Arc::clone(producer);
                Box::pin(async move {
                    producer
                        .send_record(ProducerRecord::new(topic, value).with_key(key))
                        .await
                })
            }
            KafkaProducerClient::Transactional(producer) => {
                let producer = Arc::clone(producer);
                Box::pin(async move {
                    producer
                        .send_record(ProducerRecord::new(topic, value).with_key(key))
                        .await
                })
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

    /// Accept a record for delivery, without waiting for its acknowledgement.
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
    pub async fn send_encoded(&mut self, key: Bytes, value: Bytes) -> rustcdc::core::Result<()> {
        if self.closed {
            return Err(RtError::StateError("sink is closed".to_string()));
        }

        // Bound the window *before* widening it, so `max_pipelined_sends` is a ceiling on
        // outstanding records rather than on records-plus-one.
        while self.inflight.len() >= self.max_pipelined_sends {
            self.await_oldest_send().await?;
        }

        let mut send = self.build_send(key, value);
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

impl rustcdc::sink::SinkAdapter for KafkaSink {
    fn name(&self) -> &str {
        "kafka"
    }

    async fn send(&mut self, event: &rustcdc::Event) -> rustcdc::core::Result<()> {
        let value =
            serde_json::to_vec(event).map_err(|e| RtError::SerializationError(e.to_string()))?;
        self.send_encoded(Bytes::new(), Bytes::from(value)).await
    }

    async fn flush(&mut self) -> rustcdc::core::Result<()> {
        self.flush().await
    }

    async fn close(&mut self) -> rustcdc::core::Result<()> {
        self.close().await
    }

    fn delivery_guarantee(&self) -> rustcdc::sink::SinkDeliveryGuarantee {
        match self.delivery_guarantee() {
            SinkDeliveryGuarantee::EffectivelyOnce => {
                rustcdc::sink::SinkDeliveryGuarantee::EffectivelyOnce
            }
            _ => rustcdc::sink::SinkDeliveryGuarantee::AtLeastOnce,
        }
    }

    fn transactional_checkpoint_barrier_capable(&self) -> bool {
        self.transactional_checkpoint_barrier_capable()
    }

    async fn begin_checkpoint_barrier(&mut self) -> rustcdc::core::Result<()> {
        self.begin_checkpoint_barrier().await
    }

    async fn commit_checkpoint_barrier(&mut self) -> rustcdc::core::Result<()> {
        self.commit_checkpoint_barrier().await
    }

    async fn abort_checkpoint_barrier(&mut self) -> rustcdc::core::Result<()> {
        self.abort_checkpoint_barrier().await
    }
}

/// Derive a stable Avro record name from the event's table identifier.
/// Falls back to `"CdcEvent"` when no table name is present.
#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;

    use super::{
        classify_krafka_error, kafka_connect_timeout, BarrierState, BarrierStateMachine, KafkaSink,
        RtError,
    };
    use crate::config::schema::{
        KafkaCompression, KafkaSecurityConfig, KafkaSecurityProtocol, KafkaSinkConfig,
    };
    use crate::error::AppError;
    use krafka::consumer::{AutoOffsetReset, Consumer};
    use rustcdc::{fingerprint_event_stable, Event, Operation, SourceMetadata};
    use serde_json::json;
    use tokio::process::Command;
    use tokio::sync::{Barrier, Mutex};
    use tokio::time::{sleep, Duration};

    #[test]
    fn barrier_state_machine_rejects_invalid_transition_order() {
        let mut machine = BarrierStateMachine::new();

        let commit_without_begin = machine
            .ensure_can_commit()
            .expect_err("commit must fail when no checkpoint barrier is active");
        assert!(
            commit_without_begin.to_string().contains("expected Active"),
            "unexpected commit error: {commit_without_begin}"
        );

        machine.ensure_can_begin().expect("begin should be allowed");
        machine.mark_active();
        assert_eq!(machine.state(), BarrierState::Active);

        let begin_twice = machine
            .ensure_can_begin()
            .expect_err("second begin must fail while barrier is active");
        assert!(
            begin_twice.to_string().contains("expected NotActive"),
            "unexpected begin error: {begin_twice}"
        );

        machine
            .ensure_can_commit()
            .expect("commit should be allowed");
        machine.mark_not_active();
        assert_eq!(machine.state(), BarrierState::NotActive);
    }

    #[tokio::test]
    async fn barrier_state_machine_contention_yields_single_begin_winner() {
        let machine = Arc::new(Mutex::new(BarrierStateMachine::new()));
        let start = Arc::new(Barrier::new(3));

        let mut joins = Vec::new();
        for _ in 0..2 {
            let machine = Arc::clone(&machine);
            let start = Arc::clone(&start);
            joins.push(tokio::spawn(async move {
                start.wait().await;
                let mut guard = machine.lock().await;
                match guard.ensure_can_begin() {
                    Ok(()) => {
                        guard.mark_active();
                        Ok::<(), String>(())
                    }
                    Err(err) => Err(err.to_string()),
                }
            }));
        }

        start.wait().await;

        let mut success = 0_usize;
        let mut expected_error = 0_usize;
        for join in joins {
            match join.await.expect("join must succeed") {
                Ok(()) => success += 1,
                Err(err) => {
                    if err.contains("expected NotActive") {
                        expected_error += 1;
                    }
                }
            }
        }

        assert_eq!(success, 1, "exactly one contender should begin barrier");
        assert_eq!(
            expected_error, 1,
            "exactly one contender should be rejected by state machine"
        );
        assert_eq!(machine.lock().await.state(), BarrierState::Active);
    }

    fn sample_event() -> Event {
        Event::builder("users", Operation::Insert)
            .after(json!({"id": 42, "name": "bob"}))
            .source(SourceMetadata::new("postgres", "0/16B6A71", 1))
            .ts(1)
            .schema("public")
            .primary_key(["id"])
            .build()
    }

    fn sample_kafka_config(brokers: &str, topic: &str) -> KafkaSinkConfig {
        KafkaSinkConfig {
            brokers: brokers.to_string(),
            topic: topic.to_string(),
            client_id: "cdc-kafka-test".to_string(),
            ack_timeout_ms: 1_000,
            retry_backoff_ms: 100,
            retry_max_attempts: 3,
            compression: KafkaCompression::None,
            compression_level: None,
            batch_size: 16 * 1024,
            linger_ms: 0,
            max_pipelined_sends: 128,
            max_in_flight: 5,
            transport: Default::default(),
            delivery_mode: crate::config::schema::KafkaDeliveryMode::AtLeastOnceIdempotent,
            transactional_id: None,
            transaction_timeout_ms: 60_000,
            security: KafkaSecurityConfig::default(),
            codec: None,
        }
    }

    fn live_kafka_config_from_env(brokers: &str, topic: &str) -> KafkaSinkConfig {
        let mut cfg = sample_kafka_config(brokers, topic);
        cfg.security.protocol = match std::env::var("CDC_TEST_KAFKA_PROTOCOL") {
            Ok(protocol) if protocol.eq_ignore_ascii_case("tls") => KafkaSecurityProtocol::Tls,
            _ => KafkaSecurityProtocol::Plaintext,
        };
        cfg.security.ssl_ca_location = std::env::var("CDC_TEST_KAFKA_CA")
            .ok()
            .map(std::path::PathBuf::from);
        cfg
    }

    fn sample_live_event(timestamp: u64, offset_prefix: &str) -> Event {
        static NEXT_SUFFIX: AtomicU64 = AtomicU64::new(1);

        let mut event = sample_event();
        let suffix = NEXT_SUFFIX.fetch_add(1, Ordering::Relaxed);
        event.source.offset = format!("{offset_prefix}{suffix}");
        event.source.timestamp = timestamp;
        event.ts = timestamp;
        event
    }

    fn live_kafka_target_from_env(test_name: &str) -> Option<(String, String)> {
        let Ok(brokers) = std::env::var("CDC_TEST_KAFKA_BROKERS") else {
            eprintln!("skipping {test_name} (CDC_TEST_KAFKA_BROKERS is not set)");
            return None;
        };

        let Ok(topic) = std::env::var("CDC_TEST_KAFKA_TOPIC") else {
            eprintln!("skipping {test_name} (CDC_TEST_KAFKA_TOPIC is not set)");
            return None;
        };

        Some((brokers, topic))
    }

    fn decode_event_offset(record_value: &[u8]) -> String {
        let event: Event =
            serde_json::from_slice(record_value).expect("kafka record value must decode as Event");
        event.source.offset
    }

    async fn consume_until_offsets_seen(
        consumer: &Consumer,
        expected_offsets: &BTreeSet<String>,
        attempts: usize,
    ) -> BTreeSet<String> {
        let mut seen_offsets = BTreeSet::new();

        for _ in 0..attempts {
            let records = consumer
                .poll(Duration::from_millis(250))
                .await
                .expect("consumer poll should succeed");

            for record in records {
                if let Some(value) = &record.value {
                    seen_offsets.insert(decode_event_offset(value.as_ref()));
                }
            }

            if expected_offsets.is_subset(&seen_offsets) {
                break;
            }

            sleep(Duration::from_millis(100)).await;
        }

        seen_offsets
    }

    async fn consume_offset_sequence_until_seen(
        consumer: &Consumer,
        expected_offsets: &BTreeSet<String>,
        attempts: usize,
    ) -> Vec<String> {
        let mut seen_offsets = BTreeSet::new();
        let mut sequence = Vec::new();

        for _ in 0..attempts {
            let records = consumer
                .poll(Duration::from_millis(250))
                .await
                .expect("consumer poll should succeed");

            for record in records {
                if let Some(value) = &record.value {
                    let offset = decode_event_offset(value.as_ref());
                    sequence.push(offset.clone());
                    seen_offsets.insert(offset);
                }
            }

            if expected_offsets.is_subset(&seen_offsets) {
                break;
            }

            sleep(Duration::from_millis(100)).await;
        }

        sequence
    }

    #[test]
    fn fingerprint_bytes_are_stable() {
        let event = sample_event();
        let expected = fingerprint_event_stable(&event)
            .expect("fingerprint")
            .into_bytes();
        let actual = fingerprint_event_stable(&event)
            .expect("fingerprint")
            .into_bytes();
        assert_eq!(actual, expected);
    }

    #[test]
    fn fingerprint_changes_when_source_offset_changes() {
        let first = sample_event();
        let mut second = sample_event();
        second.source.offset = "0/16B6A72".to_string();

        let first_fp = fingerprint_event_stable(&first).expect("fingerprint");
        let second_fp = fingerprint_event_stable(&second).expect("fingerprint");

        assert_ne!(first_fp, second_fp);
    }

    #[tokio::test]
    async fn broker_degradation_fails_closed() {
        let mut cfg = sample_kafka_config("127.0.0.1:1", "cdc-events-degraded");
        cfg.ack_timeout_ms = 200;
        cfg.retry_backoff_ms = 25;
        cfg.retry_max_attempts = 1;

        match KafkaSink::new(&cfg).await {
            Ok(mut sink) => {
                let event = sample_event();
                let json = serde_json::to_vec(&event).expect("serialize");
                let err = sink
                    .send_encoded(bytes::Bytes::new(), bytes::Bytes::from(json))
                    .await
                    .expect_err("send should fail when broker is unavailable");
                assert!(err.to_string().contains("Kafka sink delivery failed"));
            }
            Err(err) => {
                assert!(
                    err.to_string().contains("failed to build")
                        && err.to_string().contains("krafka producer"),
                    "expected krafka producer build error, got: {err}"
                );
            }
        }
    }

    #[tokio::test]
    async fn new_rejects_blank_topic_before_build() {
        let mut cfg = sample_kafka_config("localhost:9092", "cdc-events");
        cfg.topic = "   ".to_string();

        match KafkaSink::new(&cfg).await {
            Ok(_) => panic!("expected blank topic to be rejected"),
            Err(err) => assert!(err.to_string().contains("sink.kafka.topic")),
        }
    }

    #[tokio::test]
    async fn new_rejects_blank_client_id_before_build() {
        let mut cfg = sample_kafka_config("localhost:9092", "cdc-events");
        cfg.client_id = "   ".to_string();

        match KafkaSink::new(&cfg).await {
            Ok(_) => panic!("expected blank client_id to be rejected"),
            Err(err) => assert!(err.to_string().contains("sink.kafka.client_id")),
        }
    }

    #[tokio::test]
    async fn new_rejects_blank_brokers_before_build() {
        let cfg = sample_kafka_config(" , , ", "cdc-events");

        match KafkaSink::new(&cfg).await {
            Ok(_) => panic!("expected blank broker list to be rejected"),
            Err(err) => assert!(err.to_string().contains("sink.kafka.brokers")),
        }
    }

    #[tokio::test]
    async fn new_rejects_zero_ack_timeout_before_build() {
        let mut cfg = sample_kafka_config("localhost:9092", "cdc-events");
        cfg.ack_timeout_ms = 0;

        match KafkaSink::new(&cfg).await {
            Ok(_) => panic!("expected zero ack_timeout_ms to be rejected"),
            Err(err) => assert!(err.to_string().contains("sink.kafka.ack_timeout_ms")),
        }
    }

    #[tokio::test]
    async fn new_rejects_zero_retry_backoff_before_build() {
        let mut cfg = sample_kafka_config("localhost:9092", "cdc-events");
        cfg.retry_backoff_ms = 0;

        match KafkaSink::new(&cfg).await {
            Ok(_) => panic!("expected zero retry_backoff_ms to be rejected"),
            Err(err) => assert!(err.to_string().contains("sink.kafka.retry_backoff_ms")),
        }
    }

    #[tokio::test]
    async fn new_rejects_zero_retry_attempts_before_build() {
        let mut cfg = sample_kafka_config("localhost:9092", "cdc-events");
        cfg.retry_max_attempts = 0;

        match KafkaSink::new(&cfg).await {
            Ok(_) => panic!("expected zero retry_max_attempts to be rejected"),
            Err(err) => assert!(err.to_string().contains("sink.kafka.retry_max_attempts")),
        }
    }

    #[tokio::test]
    async fn new_rejects_tls_with_verify_peer_disabled_before_build() {
        let mut cfg = sample_kafka_config("localhost:9092", "cdc-events");
        cfg.security.protocol = KafkaSecurityProtocol::Tls;
        cfg.security.verify_peer = false;

        match KafkaSink::new(&cfg).await {
            Ok(_) => panic!("expected insecure tls verify_peer=false to be rejected"),
            Err(err) => assert!(err.to_string().contains("sink.kafka.security.verify_peer")),
        }
    }

    #[tokio::test]
    async fn new_rejects_tls_with_missing_ca_file_before_build() {
        let mut cfg = sample_kafka_config("localhost:9092", "cdc-events");
        cfg.security.protocol = KafkaSecurityProtocol::Tls;
        cfg.security.verify_peer = true;
        cfg.security.ssl_ca_location =
            Some(std::path::PathBuf::from("/tmp/no-such-kafka-ca-cert.pem"));

        match KafkaSink::new(&cfg).await {
            Ok(_) => panic!("expected missing tls CA file to be rejected"),
            Err(err) => assert!(err
                .to_string()
                .contains("sink.kafka.security.ssl_ca_location")),
        }
    }

    #[tokio::test]
    async fn live_kafka_send_and_flush_when_env_is_set() {
        let Some((brokers, topic)) = live_kafka_target_from_env("live kafka sink test") else {
            return;
        };

        let cfg = live_kafka_config_from_env(&brokers, &topic);
        let first = sample_live_event(11, "0/16B6A7");
        let second = sample_live_event(12, "0/16B6A8");

        let mut sink = KafkaSink::new(&cfg)
            .await
            .expect("live kafka producer must build");
        let send = |event: &Event| {
            let json = serde_json::to_vec(event).expect("serialize");
            (bytes::Bytes::new(), bytes::Bytes::from(json))
        };
        let (k, v) = send(&first);
        sink.send_encoded(k, v)
            .await
            .expect("first send should succeed");
        let (k, v) = send(&second);
        sink.send_encoded(k, v)
            .await
            .expect("second send should succeed");
        sink.flush().await.expect("flush should succeed");
        sink.close().await.expect("close should succeed");
        assert!(sink.is_closed());
    }

    #[tokio::test]
    async fn live_kafka_recovers_after_external_churn_when_env_is_set() {
        let Some((brokers, topic)) = live_kafka_target_from_env("kafka churn test") else {
            return;
        };
        let Ok(churn_cmd) = std::env::var("CDC_TEST_KAFKA_CHURN_COMMAND") else {
            eprintln!("skipping kafka churn test (CDC_TEST_KAFKA_CHURN_COMMAND is not set)");
            return;
        };

        let cfg = live_kafka_config_from_env(&brokers, &topic);

        let mut sink = KafkaSink::new(&cfg)
            .await
            .expect("live kafka producer must build");

        // Establish baseline health before injecting churn.
        let baseline = sample_live_event(10, "0/16B6C");
        let bl_json = serde_json::to_vec(&baseline).expect("serialize");
        sink.send_encoded(bytes::Bytes::new(), bytes::Bytes::from(bl_json))
            .await
            .expect("baseline send should succeed");
        sink.flush().await.expect("baseline flush should succeed");

        let churn = Command::new("sh")
            .arg("-c")
            .arg(churn_cmd.as_str())
            .output()
            .await
            .expect("failed to execute churn command");
        assert!(
            churn.status.success(),
            "churn command failed: {}",
            String::from_utf8_lossy(&churn.stderr)
        );

        // After churn, give the broker/client path a bounded recovery window.
        let mut recovered = false;
        for attempt in 0..40_u64 {
            let event = sample_live_event(20 + attempt, &format!("0/16B6D{}", attempt));
            let ev_json = serde_json::to_vec(&event).expect("serialize");
            let delivered = sink
                .send_encoded(bytes::Bytes::new(), bytes::Bytes::from(ev_json))
                .await
                .is_ok();
            let flushed = sink.flush().await.is_ok();
            if delivered && flushed {
                recovered = true;
                break;
            }
            sleep(Duration::from_millis(250)).await;
        }

        assert!(
            recovered,
            "sink did not recover delivery after external churn command"
        );
        sink.close().await.expect("close should succeed");
    }

    #[tokio::test]
    async fn live_kafka_consumer_group_commit_survives_rebalance_and_restart() {
        let Some((brokers, topic)) =
            live_kafka_target_from_env("kafka consumer-group commit/rebalance test")
        else {
            return;
        };

        static NEXT_GROUP_SUFFIX: AtomicU64 = AtomicU64::new(1);
        let suffix = NEXT_GROUP_SUFFIX.fetch_add(1, Ordering::Relaxed);

        let cfg = live_kafka_config_from_env(&brokers, &topic);
        let auth = cfg
            .security
            .to_auth_config()
            .expect("kafka auth config should be valid");

        let first_batch = vec![
            sample_live_event(31, &format!("0/16B6E1-{suffix}-")),
            sample_live_event(32, &format!("0/16B6E2-{suffix}-")),
        ];
        let first_offsets = first_batch
            .iter()
            .map(|event| event.source.offset.clone())
            .collect::<BTreeSet<_>>();

        let mut sink = KafkaSink::new(&cfg)
            .await
            .expect("live kafka producer must build");
        for event in &first_batch {
            let json = serde_json::to_vec(event).expect("serialize");
            sink.send_encoded(bytes::Bytes::new(), bytes::Bytes::from(json))
                .await
                .expect("first-batch send should succeed");
        }
        sink.flush()
            .await
            .expect("first-batch flush should succeed");

        let group_id = format!("cdc-kafka-commit-e2e-{suffix}");
        let consumer_a = Consumer::builder()
            .bootstrap_servers(brokers.clone())
            .group_id(group_id.clone())
            .client_id(format!("cdc-kafka-commit-e2e-a-{suffix}"))
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .enable_auto_commit(false)
            .request_timeout(Duration::from_millis(cfg.ack_timeout_ms))
            .connect_timeout(kafka_connect_timeout(Duration::from_millis(
                cfg.ack_timeout_ms,
            )))
            .auth(auth.clone())
            .build()
            .await
            .expect("consumer A should build");
        consumer_a
            .subscribe(&[topic.as_str()])
            .await
            .expect("consumer A should subscribe");

        let seen_by_a = consume_until_offsets_seen(&consumer_a, &first_offsets, 40).await;
        assert!(
            first_offsets.is_subset(&seen_by_a),
            "consumer A did not receive all first-batch offsets; expected={first_offsets:?}, seen={seen_by_a:?}"
        );
        consumer_a
            .commit()
            .await
            .expect("consumer A commit should succeed");

        let consumer_b = Consumer::builder()
            .bootstrap_servers(brokers.clone())
            .group_id(group_id)
            .client_id(format!("cdc-kafka-commit-e2e-b-{suffix}"))
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .enable_auto_commit(false)
            .request_timeout(Duration::from_millis(cfg.ack_timeout_ms))
            .connect_timeout(kafka_connect_timeout(Duration::from_millis(
                cfg.ack_timeout_ms,
            )))
            .auth(auth)
            .build()
            .await
            .expect("consumer B should build");
        consumer_b
            .subscribe(&[topic.as_str()])
            .await
            .expect("consumer B should subscribe");

        // Drive a short overlap window so group membership churn triggers rebalance.
        for _ in 0..8 {
            let _ = consumer_a
                .poll(Duration::from_millis(150))
                .await
                .expect("consumer A overlap poll should succeed");
            let _ = consumer_b
                .poll(Duration::from_millis(150))
                .await
                .expect("consumer B overlap poll should succeed");
            sleep(Duration::from_millis(75)).await;
        }

        consumer_a
            .close()
            .await
            .expect("consumer A close should succeed");

        let second_batch = vec![
            sample_live_event(41, &format!("0/16B6F1-{suffix}-")),
            sample_live_event(42, &format!("0/16B6F2-{suffix}-")),
        ];
        let expected_second_order = second_batch
            .iter()
            .map(|event| event.source.offset.clone())
            .collect::<Vec<_>>();
        let second_offsets = second_batch
            .iter()
            .map(|event| event.source.offset.clone())
            .collect::<BTreeSet<_>>();
        for event in &second_batch {
            let json = serde_json::to_vec(event).expect("serialize");
            sink.send_encoded(bytes::Bytes::new(), bytes::Bytes::from(json))
                .await
                .expect("second-batch send should succeed");
        }
        sink.flush()
            .await
            .expect("second-batch flush should succeed");

        let seen_by_b_sequence =
            consume_offset_sequence_until_seen(&consumer_b, &second_offsets, 40).await;
        let seen_by_b = seen_by_b_sequence.iter().cloned().collect::<BTreeSet<_>>();
        assert!(
            second_offsets.is_subset(&seen_by_b),
            "consumer B did not receive all second-batch offsets; expected={second_offsets:?}, seen={seen_by_b:?}"
        );

        let replayed = seen_by_b
            .iter()
            .filter(|offset| first_offsets.contains(*offset))
            .cloned()
            .collect::<Vec<_>>();
        assert!(
            replayed.is_empty(),
            "consumer B replayed committed first-batch offsets after rebalance/restart: {replayed:?}"
        );

        let seen_second_in_order = seen_by_b_sequence
            .iter()
            .filter(|offset| second_offsets.contains(*offset))
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(
            seen_second_in_order, expected_second_order,
            "consumer B violated second-batch ordering after rebalance/restart"
        );

        let mut second_counts = BTreeMap::<String, usize>::new();
        for offset in &seen_second_in_order {
            *second_counts.entry(offset.clone()).or_default() += 1;
        }
        for expected in &expected_second_order {
            assert_eq!(
                second_counts.get(expected),
                Some(&1),
                "consumer B observed duplicate/missing second-batch offset {expected} after rebalance/restart"
            );
        }

        consumer_b
            .commit()
            .await
            .expect("consumer B commit should succeed");
        consumer_b
            .close()
            .await
            .expect("consumer B close should succeed");
        sink.close().await.expect("sink close should succeed");
    }

    // ── In-process fake-broker tests (krafka `test-broker`) ──────────────────
    //
    // These run the real Kafka wire protocol against krafka's in-process fake
    // broker — no Docker, no env gating, always on in CI.

    #[tokio::test]
    async fn fake_broker_sink_delivers_all_records_durably() {
        let broker = krafka::testing::FakeBroker::start()
            .await
            .expect("fake broker");
        broker.create_topic("cdc.fake.delivery", 3);

        let cfg = sample_kafka_config(&broker.bootstrap_servers(), "cdc.fake.delivery");
        let mut sink = KafkaSink::new(&cfg).await.expect("kafka sink");

        for i in 0..5u32 {
            sink.send_encoded(
                bytes::Bytes::from(format!("key-{i}")),
                bytes::Bytes::from(format!("value-{i}")),
            )
            .await
            .expect("send must succeed and confirm durably");
        }
        sink.flush().await.expect("flush");

        // Every record must be in the broker log — the sum of next_offset over
        // all partitions is the total number of durably appended records.
        let total: i64 = broker.with_state(|state| {
            (0..3)
                .filter_map(|partition| state.partition("cdc.fake.delivery", partition))
                .map(|p| p.next_offset)
                .sum()
        });
        assert_eq!(
            total, 5,
            "all sends must be appended to the fake broker log"
        );

        sink.close().await.expect("close");
    }

    // ── Pipelined sends ──────────────────────────────────────────────────────

    /// Read every record of a single-partition topic back off the fake broker, in log
    /// order, as a `read_uncommitted` consumer sees it.
    async fn drain_partition_values(brokers: &str, topic: &str, expected: usize) -> Vec<String> {
        let consumer = Consumer::builder()
            .bootstrap_servers(brokers.to_string())
            .group_id(format!("{topic}-verify"))
            .client_id(format!("{topic}-verify"))
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .enable_auto_commit(false)
            .build()
            .await
            .expect("verification consumer");
        consumer.subscribe(&[topic]).await.expect("subscribe");

        let mut values = Vec::new();
        for _ in 0..40 {
            for record in consumer
                .poll(Duration::from_millis(250))
                .await
                .expect("poll")
            {
                if let Some(value) = &record.value {
                    values.push(String::from_utf8_lossy(value.as_ref()).into_owned());
                }
            }
            if values.len() >= expected {
                break;
            }
        }
        consumer.close().await.expect("close verification consumer");
        values
    }

    /// Read a topic as a `read_committed` consumer does — aborted records excluded.
    async fn read_committed_values(brokers: &str, topic: &str) -> Vec<String> {
        let consumer = Consumer::builder()
            .bootstrap_servers(brokers.to_string())
            .group_id(format!("{topic}-committed"))
            .client_id(format!("{topic}-committed"))
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .isolation_level(krafka::consumer::IsolationLevel::ReadCommitted)
            .enable_auto_commit(false)
            .build()
            .await
            .expect("read_committed consumer");
        consumer.subscribe(&[topic]).await.expect("subscribe");

        let mut values = Vec::new();
        for _ in 0..8 {
            for record in consumer
                .poll(Duration::from_millis(250))
                .await
                .expect("poll")
            {
                if let Some(value) = &record.value {
                    values.push(String::from_utf8_lossy(value.as_ref()).into_owned());
                }
            }
        }
        consumer.close().await.expect("close");
        values
    }

    /// **The property pipelining must not break.** Per-partition ordering is what every
    /// downstream CDC consumer depends on: replaying `UPDATE balance=100` before
    /// `UPDATE balance=50` silently corrupts the replica.
    ///
    /// One partition, a window far wider than the record count, so every send is
    /// outstanding at once and any reordering in the accumulator would show up here.
    #[tokio::test]
    async fn pipelined_sends_reach_the_partition_in_submission_order() {
        let broker = krafka::testing::FakeBroker::start()
            .await
            .expect("fake broker");
        let topic = "cdc.fake.pipeline.order";
        broker.create_topic(topic, 1);

        let mut cfg = sample_kafka_config(&broker.bootstrap_servers(), topic);
        cfg.max_pipelined_sends = 256;
        // A non-zero linger is the case that used to be unusable: it forces records to
        // coalesce in the accumulator, which is exactly where a reordering would happen.
        cfg.linger_ms = 5;
        let mut sink = KafkaSink::new(&cfg).await.expect("kafka sink");

        let expected: Vec<String> = (0..200).map(|i| format!("value-{i:04}")).collect();
        for value in &expected {
            sink.send_encoded(
                bytes::Bytes::from_static(b"same-key"),
                bytes::Bytes::from(value.clone()),
            )
            .await
            .expect("send accepted");
        }
        sink.flush().await.expect("flush");

        let observed =
            drain_partition_values(&broker.bootstrap_servers(), topic, expected.len()).await;
        assert_eq!(
            observed, expected,
            "pipelined sends must reach the partition in submission order"
        );

        sink.close().await.expect("close");
    }

    /// `flush` is the durability boundary the checkpoint depends on: `run_loop_batch`
    /// commits a checkpoint only after `process_batch_events` returns, and that calls
    /// `flush`. If `flush` returned before collecting outstanding acknowledgements, the
    /// checkpoint would advance past records that were never confirmed.
    #[tokio::test]
    async fn flush_collects_every_outstanding_acknowledgement() {
        let broker = krafka::testing::FakeBroker::start()
            .await
            .expect("fake broker");
        let topic = "cdc.fake.pipeline.flush";
        broker.create_topic(topic, 1);

        let mut cfg = sample_kafka_config(&broker.bootstrap_servers(), topic);
        cfg.max_pipelined_sends = 512;
        cfg.linger_ms = 20;
        let mut sink = KafkaSink::new(&cfg).await.expect("kafka sink");

        for i in 0..100u32 {
            sink.send_encoded(
                bytes::Bytes::from(format!("k{i}")),
                bytes::Bytes::from(format!("v{i}")),
            )
            .await
            .expect("send accepted");
        }

        sink.flush().await.expect("flush");
        assert!(
            sink.inflight.is_empty(),
            "flush must leave no unconfirmed sends behind"
        );

        let durable = broker.next_offset(topic, 0).expect("partition exists");
        assert_eq!(
            durable, 100,
            "every accepted record must be durable once flush returns"
        );

        sink.close().await.expect("close");
    }

    /// `max_pipelined_sends` is a ceiling, not a hint. An inert tuning knob is worse than
    /// no knob, so this asserts the window is bounded
    /// while sends are outstanding, not merely that the field is read.
    #[tokio::test]
    async fn the_pipeline_window_is_bounded_by_its_configured_depth() {
        let broker = krafka::testing::FakeBroker::start()
            .await
            .expect("fake broker");
        let topic = "cdc.fake.pipeline.window";
        broker.create_topic(topic, 1);

        let mut cfg = sample_kafka_config(&broker.bootstrap_servers(), topic);
        cfg.max_pipelined_sends = 8;
        cfg.linger_ms = 20;
        let mut sink = KafkaSink::new(&cfg).await.expect("kafka sink");

        for i in 0..50u32 {
            sink.send_encoded(
                bytes::Bytes::from(format!("k{i}")),
                bytes::Bytes::from(format!("v{i}")),
            )
            .await
            .expect("send accepted");
            assert!(
                sink.inflight.len() <= 8,
                "window grew to {} with max_pipelined_sends = 8",
                sink.inflight.len()
            );
        }

        sink.flush().await.expect("flush");
        sink.close().await.expect("close");
    }

    /// The throughput claim, measured rather than asserted in prose.
    ///
    /// A depth-1 window is precisely the original sink: one broker round-trip per
    /// record. Both runs go through the same code against the same in-process broker, so
    /// the ratio isolates pipelining from everything else. The threshold is deliberately
    /// loose — this runs on shared CI hardware, and the point is to catch the window
    /// silently reverting to serial, not to publish a number.
    #[tokio::test]
    async fn pipelining_outperforms_one_round_trip_per_record() {
        async fn run(broker_servers: &str, topic: &str, depth: usize) -> Duration {
            let mut cfg = sample_kafka_config(broker_servers, topic);
            cfg.max_pipelined_sends = depth;
            cfg.linger_ms = 2;
            let mut sink = KafkaSink::new(&cfg).await.expect("kafka sink");

            let started = std::time::Instant::now();
            for i in 0..300u32 {
                sink.send_encoded(
                    bytes::Bytes::from(format!("k{i}")),
                    bytes::Bytes::from(format!("v{i}")),
                )
                .await
                .expect("send accepted");
            }
            sink.flush().await.expect("flush");
            let elapsed = started.elapsed();
            sink.close().await.expect("close");
            elapsed
        }

        let broker = krafka::testing::FakeBroker::start()
            .await
            .expect("fake broker");
        broker.create_topic("cdc.fake.perf.serial", 1);
        broker.create_topic("cdc.fake.perf.pipelined", 1);

        broker.clear_requests();
        let serial = run(&broker.bootstrap_servers(), "cdc.fake.perf.serial", 1).await;
        let serial_produces = broker.request_count(krafka::protocol::ApiKey::Produce);

        broker.clear_requests();
        let pipelined = run(&broker.bootstrap_servers(), "cdc.fake.perf.pipelined", 256).await;
        let pipelined_produces = broker.request_count(krafka::protocol::ApiKey::Produce);

        eprintln!(
            "pipelining: serial {serial:?} / {serial_produces} produce requests \
             vs pipelined {pipelined:?} / {pipelined_produces} produce requests"
        );

        // The round-trip count is the deterministic signal and the one that actually
        // governs throughput on a real network; wall-clock on shared CI hardware is not.
        assert_eq!(
            serial_produces, 300,
            "a depth-1 window is one broker round-trip per record, by definition"
        );
        assert!(
            pipelined_produces * 10 < serial_produces,
            "pipelining must coalesce records into batches (serial {serial_produces} \
             produce requests, pipelined {pipelined_produces})"
        );
        assert!(
            pipelined < serial,
            "pipelining must not be slower (serial {serial:?}, pipelined {pipelined:?})"
        );
    }

    // ── Transactional (effectively-once) coverage ────────────────────────────
    //
    // krafka 0.16's fake broker serves the full transaction protocol — commit and
    // abort markers, `read_committed` isolation and the last-stable-offset — so the
    // checkpoint-barrier path finally has broker-level evidence rather than only the
    // in-memory state-machine tests above.

    fn transactional_kafka_config(brokers: &str, topic: &str, txn_id: &str) -> KafkaSinkConfig {
        let mut cfg = sample_kafka_config(brokers, topic);
        cfg.delivery_mode = crate::config::schema::KafkaDeliveryMode::Transactional;
        cfg.transactional_id = Some(txn_id.to_string());
        cfg
    }

    /// A committed barrier must advance the last stable offset — that is what makes
    /// the records visible to a `read_committed` consumer, and it is the property
    /// `effectively_once` actually sells.
    #[tokio::test]
    async fn fake_broker_committed_barrier_advances_last_stable_offset() {
        let broker = krafka::testing::FakeBroker::start()
            .await
            .expect("fake broker");
        broker.create_topic("cdc.fake.txn.commit", 1);

        let cfg = transactional_kafka_config(
            &broker.bootstrap_servers(),
            "cdc.fake.txn.commit",
            "cdc-txn-commit",
        );
        let mut sink = KafkaSink::new(&cfg).await.expect("transactional sink");
        assert_eq!(
            sink.delivery_guarantee(),
            super::SinkDeliveryGuarantee::EffectivelyOnce
        );

        sink.begin_checkpoint_barrier()
            .await
            .expect("begin barrier");
        for i in 0..3u32 {
            sink.send_encoded(
                bytes::Bytes::from(format!("key-{i}")),
                bytes::Bytes::from(format!("value-{i}")),
            )
            .await
            .expect("send inside transaction");
        }

        // Uncommitted: the records are appended but not yet stable, so a
        // `read_committed` consumer must not be able to see them.
        let lso_before = broker
            .last_stable_offset("cdc.fake.txn.commit", 0)
            .expect("partition exists");
        assert_eq!(
            lso_before, 0,
            "records must not be stable before the barrier commits"
        );

        sink.commit_checkpoint_barrier()
            .await
            .expect("commit barrier");

        let lso_after = broker
            .last_stable_offset("cdc.fake.txn.commit", 0)
            .expect("partition exists");
        assert!(
            lso_after > lso_before,
            "committing the barrier must advance the last stable offset \
             ({lso_before} -> {lso_after})"
        );
        assert!(
            broker
                .aborted_transactions("cdc.fake.txn.commit", 0)
                .is_empty(),
            "a committed barrier must leave no abort marker"
        );

        sink.close().await.expect("close");
    }

    /// An aborted barrier must leave an abort marker, so a `read_committed`
    /// consumer skips the records rather than reading a partially-applied batch.
    /// This is the failure path the delivery contract exists for.
    #[tokio::test]
    async fn fake_broker_aborted_barrier_leaves_an_abort_marker() {
        let broker = krafka::testing::FakeBroker::start()
            .await
            .expect("fake broker");
        broker.create_topic("cdc.fake.txn.abort", 1);

        let cfg = transactional_kafka_config(
            &broker.bootstrap_servers(),
            "cdc.fake.txn.abort",
            "cdc-txn-abort",
        );
        let mut sink = KafkaSink::new(&cfg).await.expect("transactional sink");

        sink.begin_checkpoint_barrier()
            .await
            .expect("begin barrier");
        sink.send_encoded(
            bytes::Bytes::from("key-doomed"),
            bytes::Bytes::from("value-doomed"),
        )
        .await
        .expect("send inside transaction");
        // Collect the acknowledgement so the record is genuinely in the open transaction
        // on the broker. Without this the abort has nothing to mark: a pipelined send may
        // still be outstanding, and `abort_checkpoint_barrier` cancels it rather than
        // waiting — see `SendWindow::abandon`. Both routes discard the batch, and the
        // *unflushed* one is covered below; this half needs a record that reached the log.
        sink.flush().await.expect("flush into the open transaction");

        sink.abort_checkpoint_barrier()
            .await
            .expect("abort barrier");

        assert!(
            !broker
                .aborted_transactions("cdc.fake.txn.abort", 0)
                .is_empty(),
            "an aborted barrier must record an abort marker so read_committed skips it"
        );

        // The state machine must be back to NotActive, so the next barrier can begin.
        sink.begin_checkpoint_barrier()
            .await
            .expect("a new barrier must be startable after an abort");
        sink.commit_checkpoint_barrier()
            .await
            .expect("commit the recovery barrier");

        sink.close().await.expect("close");
    }

    /// The other half of the abort contract: a record accepted into a pipelined window
    /// but aborted before it drains must never become visible either.
    ///
    /// This is the path `run_loop_batch` takes when delivery fails mid-batch — it aborts
    /// without flushing. Cancelling an outstanding send is sound precisely because the
    /// transaction it belonged to is being discarded; the assertion is that the record
    /// does not survive by some other route.
    #[tokio::test]
    async fn aborting_before_the_window_drains_publishes_nothing() {
        let broker = krafka::testing::FakeBroker::start()
            .await
            .expect("fake broker");
        let topic = "cdc.fake.txn.abort.undrained";
        broker.create_topic(topic, 1);

        let mut cfg =
            transactional_kafka_config(&broker.bootstrap_servers(), topic, "cdc-txn-undrained");
        cfg.max_pipelined_sends = 128;
        cfg.linger_ms = 50;
        let mut sink = KafkaSink::new(&cfg).await.expect("transactional sink");

        sink.begin_checkpoint_barrier()
            .await
            .expect("begin barrier");
        for i in 0..20u32 {
            sink.send_encoded(
                bytes::Bytes::from(format!("k{i}")),
                bytes::Bytes::from(format!("doomed-{i}")),
            )
            .await
            .expect("send accepted");
        }
        sink.abort_checkpoint_barrier()
            .await
            .expect("abort barrier");

        // Whatever reached the log is covered by an abort marker; whatever did not was
        // cancelled. Either way a read_committed consumer sees no *data*. Asserting on
        // the last stable offset would be wrong: the abort marker is itself a control
        // record and takes an offset, so the LSO advances past it on a correct abort.
        let visible = read_committed_values(&broker.bootstrap_servers(), topic).await;
        assert!(
            visible.is_empty(),
            "no aborted record may become visible to a read_committed consumer, saw {visible:?}"
        );

        sink.close().await.expect("close");
    }

    // ── Sink error classification ────────────────────────────────────────────

    /// Every Kafka failure used to map to `SourceError`, which classifies as Transient.
    /// A record the broker rejects permanently was therefore retried until the circuit
    /// breaker killed the process — and then failed identically after the restart, on the
    /// same record, forever. The dead-letter queue could not help, because its branch
    /// only runs for a non-recoverable error and this sink never produced one.
    #[test]
    fn a_poison_record_is_quarantinable_not_retriable() {
        let error = krafka::error::KrafkaError::Broker {
            code: krafka::error::ErrorCode::MessageTooLarge,
            message: "record exceeds max.message.bytes".to_string(),
        };
        let classified = classify_krafka_error(&error, "sink delivery failed");

        assert!(
            !classified.is_recoverable(),
            "a record the broker will reject identically forever is not retriable"
        );
        assert!(
            classified.is_dead_letterable(),
            "an oversized record is the record's fault, so quarantine is the way forward"
        );
    }

    /// The mirror image, and the reason `is_dead_letterable` exists as a separate
    /// question. Bad credentials are permanent *and* not the record's fault: quarantining
    /// them would drain the entire change stream into the DLQ one event at a time while
    /// every health check still reported the pipeline as running.
    #[test]
    fn an_authorization_failure_is_neither_retriable_nor_quarantinable() {
        let error = krafka::error::KrafkaError::Broker {
            code: krafka::error::ErrorCode::TopicAuthorizationFailed,
            message: "not authorized".to_string(),
        };
        let classified = classify_krafka_error(&error, "sink delivery failed");

        assert!(
            !classified.is_recoverable(),
            "an ACL will not change on retry"
        );
        assert!(
            !classified.is_dead_letterable(),
            "dead-lettering an ACL failure quarantines every event in the stream"
        );
    }

    /// A leader election is the common case and must stay retriable, or a routine broker
    /// restart becomes a process exit and a full replay from the last checkpoint.
    #[test]
    fn a_transient_broker_condition_stays_retriable() {
        let error = krafka::error::KrafkaError::Broker {
            code: krafka::error::ErrorCode::LeaderNotAvailable,
            message: "leader election in progress".to_string(),
        };
        assert!(
            classify_krafka_error(&error, "sink delivery failed").is_recoverable(),
            "a leader election must be retried, not escalated"
        );
    }

    /// The classification has to survive the trip through `rustcdc::core::Error` and back,
    /// because that is the boundary the sink's result actually crosses on its way to the
    /// batch loop's dead-letter decision.
    #[test]
    fn classification_survives_the_round_trip_through_rustcdc() {
        let poison = classify_krafka_error(
            &krafka::error::KrafkaError::Broker {
                code: krafka::error::ErrorCode::InvalidRecord,
                message: "malformed".to_string(),
            },
            "sink delivery failed",
        );
        let round_tripped = AppError::Runtime(RtError::from(poison));
        assert!(!round_tripped.is_recoverable());
        assert!(
            round_tripped.is_dead_letterable(),
            "a poison record must still be quarantinable after crossing the sink boundary"
        );

        let fatal = classify_krafka_error(
            &krafka::error::KrafkaError::Auth {
                message: "bad credentials".to_string(),
            },
            "sink delivery failed",
        );
        let round_tripped = AppError::Runtime(RtError::from(fatal));
        assert!(!round_tripped.is_recoverable());
        assert!(
            !round_tripped.is_dead_letterable(),
            "an auth failure must not become quarantinable by crossing the sink boundary"
        );
    }

    /// `init_transactions` must fence the previous producer for the same
    /// transactional id by bumping the epoch — that is what stops a zombie writer
    /// from committing after this instance took over (KIP-447).
    #[tokio::test]
    async fn fake_broker_reinit_fences_the_previous_producer_epoch() {
        let broker = krafka::testing::FakeBroker::start()
            .await
            .expect("fake broker");
        broker.create_topic("cdc.fake.txn.fence", 1);

        let cfg = transactional_kafka_config(
            &broker.bootstrap_servers(),
            "cdc.fake.txn.fence",
            "cdc-txn-fence",
        );

        let first = KafkaSink::new(&cfg).await.expect("first sink");
        let (id_a, epoch_a) = broker
            .transactional_producer("cdc-txn-fence")
            .expect("producer registered");

        // A second instance claiming the same transactional id — the restart case.
        let mut second = KafkaSink::new(&cfg).await.expect("second sink");
        let (id_b, epoch_b) = broker
            .transactional_producer("cdc-txn-fence")
            .expect("producer still registered");

        assert_eq!(
            id_a, id_b,
            "re-initialising the same transactional id must keep the producer id"
        );
        assert!(
            epoch_b > epoch_a,
            "re-initialising must bump the epoch to fence the previous producer \
             ({epoch_a} -> {epoch_b})"
        );

        drop(first);
        second.close().await.expect("close");
    }

    /// `linger_ms` must default to 0.
    ///
    /// This sink awaits each record's broker confirmation before returning from
    /// `send_encoded` — that per-record durability check is the point of the Kafka
    /// sink. It also means a batch never accumulates across calls: every send waits
    /// out the full linger on its own, so a non-zero default silently caps throughput
    /// at `1000 / linger_ms` events per second per sink. Measured against the fake
    /// broker, a 50 ms linger takes ~50 ms *per record*.
    #[tokio::test]
    async fn linger_is_not_charged_per_record_at_the_default() {
        let broker = krafka::testing::FakeBroker::start()
            .await
            .expect("fake broker");
        broker.create_topic("cdc.fake.linger", 1);

        let cfg = sample_kafka_config(&broker.bootstrap_servers(), "cdc.fake.linger");
        assert_eq!(
            cfg.linger_ms, 0,
            "the default linger must be 0; anything higher is charged once per record \
             because send_encoded awaits each confirmation"
        );

        let mut sink = KafkaSink::new(&cfg).await.expect("kafka sink");
        let started = std::time::Instant::now();
        for i in 0..20u32 {
            sink.send_encoded(bytes::Bytes::from(format!("k{i}")), bytes::Bytes::from("v"))
                .await
                .expect("send");
        }
        let elapsed = started.elapsed();
        sink.close().await.expect("close");

        // 20 sequential confirmed sends against an in-process broker. With a 5 ms
        // linger this took >100 ms; with 0 it is bounded by the round-trips alone.
        assert!(
            elapsed < std::time::Duration::from_millis(500),
            "20 confirmed sends took {elapsed:?}; the default linger is charging \
             per-record latency"
        );
    }

    /// End-to-end check of the message-key contract through `SinkBinding`:
    /// events with a primary key are keyed by the PK JSON (per-row ordering);
    /// keyless events fall back to the qualified table name instead of an
    /// empty key (which Kafka would hash — pinning every keyless event of
    /// every table to one partition).
    #[tokio::test]
    async fn fake_broker_message_keys_use_pk_json_with_table_name_fallback() {
        use krafka::consumer::CompactedTopicConsumer;

        let broker = krafka::testing::FakeBroker::start()
            .await
            .expect("fake broker");
        broker.create_topic("cdc.fake.keys", 1);

        let cfg = sample_kafka_config(&broker.bootstrap_servers(), "cdc.fake.keys");
        let mut binding =
            crate::sink::build_binding(&crate::config::schema::SinkConfig::Kafka(cfg), usize::MAX)
                .await
                .expect("sink binding");

        let keyed = sample_event(); // primary_key = ["id"], after.id = 42
        let mut keyless = sample_event();
        keyless.primary_key = None;

        binding.send_event(&keyed).await.expect("send keyed");
        binding.send_event(&keyless).await.expect("send keyless");
        use rustcdc::sink::SinkAdapter as _;
        binding.flush().await.expect("flush");

        // No connect_timeout on this builder — use a request budget >= the
        // 10 s connect default (see load_records for the same workaround).
        let mut consumer = CompactedTopicConsumer::builder()
            .bootstrap_servers(broker.bootstrap_servers())
            .topic("cdc.fake.keys".to_string())
            .client_id("fake-key-check".to_string())
            .request_timeout(Duration::from_secs(10))
            .build()
            .await
            .expect("compacted consumer");
        consumer
            .scan(Duration::from_millis(1_000))
            .await
            .expect("scan");
        let table = consumer.table();

        assert!(
            table.contains_key(br#"{"id":42}"#.as_slice()),
            "keyed event must use the primary-key JSON as message key"
        );
        assert!(
            table.contains_key(b"public.users".as_slice()),
            "keyless event must fall back to the qualified table name key"
        );
        consumer.close().await.expect("consumer close");
        binding.close().await.expect("binding close");
    }
}
