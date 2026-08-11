//! The Zerobus sink's contract, driven against an in-process fake of the gRPC service.
//!
//! # Why a fake, and why this one is trustworthy
//!
//! A workspace cannot be reached from a test, and an account-gated suite that never runs
//! reports success. The usual objection to faking a gRPC service — that you test your own
//! restatement of someone else's contract — does not apply: the SDK's `build.rs` generates
//! **both** halves from the canonical `zerobus_service.proto`, and the fake below
//! implements the public server trait `databricks::zerobus::zerobus_server::Zerobus`. If
//! Databricks changes the contract, this stops compiling.
//!
//! # What it asserts
//!
//! Each property needs the server to misbehave on demand, which a real workspace cannot be
//! asked to do:
//!
//! * `flush()` does not return until the durability acknowledgement covers the batch;
//! * an acknowledgement that never arrives **fails** the flush, so the checkpoint cannot
//!   advance past records that are not durable;
//! * records arrive as JSON, in order, exactly once per send;
//! * the sink reports `at_least_once` — Zerobus streams are ephemeral, so there is no
//!   durable offset to resume from and `effectively_once` would be a lie.

use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use databricks_zerobus_ingest_sdk::databricks::zerobus::{
    CreateIngestStreamResponse, EphemeralStreamRequest, EphemeralStreamResponse,
    IngestRecordResponse, ephemeral_stream_request, ephemeral_stream_response,
    zerobus_server::{Zerobus, ZerobusServer},
};
use futures::StreamExt as _;
use rustcdc::core::{Event, Operation, SourceMetadata};
use rustcdc::sink::SinkAdapter as _;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

#[derive(Default)]
struct FakeState {
    /// JSON payloads received, in arrival order.
    records: Vec<String>,
    /// Highest offset the fake has acknowledged as durable.
    acked_up_to: Option<i64>,
    /// Table the client asked for.
    table: Option<String>,
    /// Withhold every acknowledgement, so the sink's wait must fail rather than hang.
    never_ack: bool,
    /// Acknowledge only after this many records have arrived — makes the wait a real wait.
    ack_after_records: usize,
}

type Shared = Arc<Mutex<FakeState>>;

fn lock(state: &Shared) -> std::sync::MutexGuard<'_, FakeState> {
    state
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

struct FakeZerobus {
    state: Shared,
}

#[tonic::async_trait]
impl Zerobus for FakeZerobus {
    type EphemeralStreamStream =
        Pin<Box<dyn futures::Stream<Item = Result<EphemeralStreamResponse, Status>> + Send>>;

    async fn ephemeral_stream(
        &self,
        request: Request<Streaming<EphemeralStreamRequest>>,
    ) -> Result<Response<Self::EphemeralStreamStream>, Status> {
        let state = Arc::clone(&self.state);
        let mut inbound = request.into_inner();
        let (tx, rx) = tokio::sync::mpsc::channel(1024);

        tokio::spawn(async move {
            while let Some(message) = inbound.next().await {
                let Ok(message) = message else { break };
                match message.payload {
                    Some(ephemeral_stream_request::Payload::CreateStream(create)) => {
                        lock(&state).table = create.table_name.clone();
                        let response = EphemeralStreamResponse {
                            payload: Some(
                                ephemeral_stream_response::Payload::CreateStreamResponse(
                                    CreateIngestStreamResponse {
                                        stream_id: Some("fake-stream".to_string()),
                                    },
                                ),
                            ),
                        };
                        if tx.send(Ok(response)).await.is_err() {
                            break;
                        }
                    }
                    Some(ephemeral_stream_request::Payload::IngestRecord(record)) => {
                        let offset = record.offset_id.unwrap_or_default();
                        let ack = {
                            let mut fake = lock(&state);
                            if let Some(
                                databricks_zerobus_ingest_sdk::databricks::zerobus::ingest_record_request::Record::JsonRecord(json),
                            ) = record.record
                            {
                                fake.records.push(json);
                            }
                            if fake.never_ack || fake.records.len() < fake.ack_after_records {
                                None
                            } else {
                                fake.acked_up_to = Some(offset);
                                Some(offset)
                            }
                        };
                        if let Some(offset) = ack {
                            let response = EphemeralStreamResponse {
                                payload: Some(
                                    ephemeral_stream_response::Payload::IngestRecordResponse(
                                        IngestRecordResponse {
                                            durability_ack_up_to_offset: Some(offset),
                                        },
                                    ),
                                ),
                            };
                            if tx.send(Ok(response)).await.is_err() {
                                break;
                            }
                        }
                    }
                    _ => {}
                }
            }
        });

        Ok(Response::new(
            Box::pin(ReceiverStream::new(rx)) as Self::EphemeralStreamStream
        ))
    }
}

async fn start_fake(state: Shared) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind the fake");
    let addr = listener.local_addr().expect("local addr");
    tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(ZerobusServer::new(FakeZerobus { state }))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await;
    });
    addr
}

fn sink_config(addr: SocketAddr) -> rustcdc_server::config::schema::SinkConfig {
    serde_json::from_value(serde_json::json!({
        "type": "zerobus",
        "endpoint": format!("http://127.0.0.1:{}", addr.port()),
        "unity_catalog_url": format!("http://127.0.0.1:{}", addr.port()),
        "table": "main.cdc.events",
        "auth": { "type": "no_auth" },
        "ack_timeout_ms": 5000,
        "flush_interval_ms": 1000,
    }))
    .expect("zerobus sink config")
}

fn event(id: u64) -> Event {
    Event::builder("orders", Operation::Insert)
        .after(serde_json::json!({ "id": id }))
        .source(SourceMetadata::new("postgres", format!("0/{id:04X}"), id))
        .ts(1_700_000_000_000 + id)
        .schema("public")
        .primary_key(["id"])
        .build()
}

async fn build_router(addr: SocketAddr) -> rustcdc_server::pipeline::router::TableRouter {
    let binding = rustcdc_server::sink::build_binding(&sink_config(addr), 1 << 20)
        .await
        .expect("zerobus binding");
    rustcdc_server::pipeline::router::single(binding)
}

/// **The durability property.**
///
/// `ingest_record_offset` returning means the SDK has the record, not that Databricks does.
/// `flush()` must wait for `durability_ack_up_to_offset` to cover the batch, or the pipeline
/// checkpoints past records a process exit would lose.
#[tokio::test]
async fn flush_waits_for_the_durability_acknowledgement() {
    let state: Shared = Arc::new(Mutex::new(FakeState {
        // Acknowledge nothing until the whole batch has arrived.
        ack_after_records: 5,
        ..Default::default()
    }));
    let addr = start_fake(Arc::clone(&state)).await;
    let mut router = build_router(addr).await;
    router.preflight_check().await.expect("preflight");

    for id in 0..5 {
        router.send(&event(id)).await.expect("send");
    }
    router.flush().await.expect("flush must succeed");

    let fake = lock(&state);
    assert_eq!(fake.records.len(), 5, "every record must reach the service");
    assert!(
        fake.acked_up_to.is_some(),
        "the flush must not have returned before an acknowledgement arrived"
    );
    assert_eq!(fake.table.as_deref(), Some("main.cdc.events"));
}

/// An acknowledgement that never arrives must **fail** the flush.
///
/// Returning success would advance the checkpoint past records that are not durable — the
/// silent-loss shape the durability wait exists to refuse.
#[tokio::test]
async fn an_acknowledgement_that_never_arrives_fails_the_flush() {
    let state: Shared = Arc::new(Mutex::new(FakeState {
        never_ack: true,
        ..Default::default()
    }));
    let addr = start_fake(Arc::clone(&state)).await;
    let mut router = build_router(addr).await;
    router.preflight_check().await.expect("preflight");

    router.send(&event(1)).await.expect("send");
    let started = Instant::now();
    let error = router
        .flush()
        .await
        .expect_err("an unacknowledged batch must fail the flush");

    assert!(
        error.to_string().contains("acknowledge"),
        "the error must name the durability wait: {error}"
    );
    assert!(
        started.elapsed() >= Duration::from_millis(200),
        "the flush must actually have waited"
    );
}

/// Records go on the wire as JSON, in order, once each.
#[tokio::test]
async fn records_arrive_as_json_in_submission_order() {
    let state: Shared = Arc::new(Mutex::new(FakeState::default()));
    let addr = start_fake(Arc::clone(&state)).await;
    let mut router = build_router(addr).await;
    router.preflight_check().await.expect("preflight");

    for id in 0..20 {
        router.send(&event(id)).await.expect("send");
    }
    router.flush().await.expect("flush");

    let fake = lock(&state);
    assert_eq!(fake.records.len(), 20, "once each, not twice");
    let ids: Vec<u64> = fake
        .records
        .iter()
        .map(|json| {
            serde_json::from_str::<serde_json::Value>(json)
                .expect("each record must be one complete JSON text")
                .pointer("/after/id")
                .and_then(serde_json::Value::as_u64)
                .expect("the event carries its id")
        })
        .collect();
    assert_eq!(ids, (0..20).collect::<Vec<_>>(), "order is per-stream");
}

/// The sink must advertise the contract it actually provides.
///
/// Zerobus streams are ephemeral — `zerobus_service.proto` reserves `last_offset_id` and
/// documents reopening by `stream_id` as `NOT SUPPORTED` — so there is no durable
/// destination-side record to resume from. The acknowledgement rules out loss, not
/// duplicates, and `effectively_once` here would be a claim the service cannot support.
#[tokio::test]
async fn the_sink_advertises_at_least_once_not_effectively_once() {
    let state: Shared = Arc::new(Mutex::new(FakeState::default()));
    let addr = start_fake(Arc::clone(&state)).await;

    let binding = rustcdc_server::sink::build_binding(&sink_config(addr), 1 << 20)
        .await
        .expect("binding");
    assert_eq!(binding.name(), "zerobus");
    assert_eq!(
        binding.delivery_guarantee(),
        rustcdc::sink::SinkDeliveryGuarantee::AtLeastOnce
    );
    assert!(
        !binding.idempotent_delivery_capable(),
        "there is no durable offset to deduplicate against"
    );
    assert!(!binding.transactional_checkpoint_barrier_capable());
}
