//! The Snowflake sink's contract, driven against a fake of the Snowpipe Streaming API.
//!
//! Deliberately **not** named `integration_*`: it needs no container, no account and no
//! environment variable, and it runs on every `cargo test`. The `integration_*` suites are
//! the ones that manage a real dependency and are pinned to the CI matrix by
//! `tests/architecture.rs`; calling this one of them would misdescribe what it proves.
//!
//! # Why a fake rather than an account-gated suite
//!
//! A Snowflake sink cannot be exercised against the real service without an account, and
//! this project has just spent a round establishing that an env-gated test nobody runs is
//! worse than no test — it reports success. So the properties that make this sink *correct*
//! are asserted against a local fake instead, and they run on every `cargo test`.
//!
//! That is not a consolation prize. Snowpipe Streaming is a plain HTTP API with a documented
//! error vocabulary, and every claim this sink makes is a claim about **how it reacts to
//! that API**:
//!
//! * a `200` from `Append Rows` is *not* durability, so `flush` must keep waiting;
//! * a commit that never lands must fail the flush, or the pipeline checkpoints past rows a
//!   channel reopen would discard;
//! * `STALE_CONTINUATION_TOKEN_SEQUENCER` must reopen the channel rather than fail;
//! * a channel reopened with a committed offset token must skip the replayed rows.
//!
//! A fake can be made to do all four on demand. A real account cannot — you cannot ask
//! Snowflake to withhold a commit, and the stale-sequencer path needs a second writer.
//!
//! What the fake cannot prove is that the request shapes match the real service. That is
//! covered by pinning them here against the published reference, and it is the gap a live
//! suite would close — recorded in the coverage table rather than glossed over.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post, put};
use axum::{Json, Router};
use rustcdc::core::{Event, Operation, SourceMetadata};
use rustcdc::sink::SinkAdapter as _;

/// Everything the fake remembers, and the knobs a test uses to steer it.
#[derive(Default)]
struct FakeState {
    /// NDJSON bodies received, in order.
    appends: Vec<String>,
    /// End offset token of each append.
    append_end_offsets: Vec<String>,
    /// What `Bulk Get Channel Status` reports as committed.
    committed_offset_token: Option<String>,
    /// The continuation token the fake currently accepts.
    continuation: String,
    /// Rejects the next N appends with `STALE_CONTINUATION_TOKEN_SEQUENCER`.
    stale_appends_remaining: u32,
    /// Committed-status polls to serve before the commit "lands".
    commit_after_polls: u32,
    polls: u32,
    /// Never advance the committed token, whatever happens.
    never_commit: bool,
    /// Reported by the next status poll.
    rows_error_count: u64,
    open_calls: u32,
    token_exchanges: u32,
    /// What the sink put in `Authorization` when exchanging its credential.
    exchange_authorization: Option<String>,
    exchange_token_type: Option<String>,
    exchange_body: String,
}

type Shared = Arc<Mutex<FakeState>>;

fn lock(state: &Shared) -> std::sync::MutexGuard<'_, FakeState> {
    state
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

async fn oauth_token(
    State(state): State<Shared>,
    headers: axum::http::HeaderMap,
    body: String,
) -> impl IntoResponse {
    let mut fake = lock(&state);
    fake.token_exchanges += 1;
    fake.exchange_authorization = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    fake.exchange_token_type = headers
        .get("x-snowflake-authorization-token-type")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    fake.exchange_body = body;
    // The real endpoint returns a bare token string, not JSON.
    (StatusCode::OK, "fake-scoped-token")
}

async fn hostname(State(_): State<Shared>) -> impl IntoResponse {
    // Deliberately echoes nothing useful: the sink must keep using the configured host when
    // the response does not name a different one.
    (StatusCode::OK, Json(serde_json::json!({ "hostname": "" })))
}

async fn open_channel(
    State(state): State<Shared>,
    Path((_db, _schema, _pipe, channel)): Path<(String, String, String, String)>,
) -> impl IntoResponse {
    let mut fake = lock(&state);
    fake.open_calls += 1;
    // A reopen mints a fresh continuation token — that is what makes the old one stale.
    fake.continuation = format!("continuation-{}", fake.open_calls);
    let committed = fake.committed_offset_token.clone();
    let continuation = fake.continuation.clone();
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "next_continuation_token": continuation,
            "channel_status": {
                "database_name": "CDC",
                "schema_name": "PUBLIC",
                "pipe_name": "EVENTS_PIPE",
                "channel_name": channel,
                "channel_status_code": "ACTIVE",
                "last_committed_offset_token": committed,
                "rows_inserted": 0,
                "rows_parsed": 0,
                "rows_error_count": 0,
            }
        })),
    )
}

#[derive(serde::Deserialize)]
struct AppendQuery {
    #[serde(rename = "continuationToken")]
    continuation_token: String,
    #[serde(rename = "endOffsetToken")]
    end_offset_token: Option<String>,
}

async fn append_rows(
    State(state): State<Shared>,
    Query(query): Query<AppendQuery>,
    body: String,
) -> impl IntoResponse {
    let mut fake = lock(&state);

    if fake.stale_appends_remaining > 0 {
        fake.stale_appends_remaining -= 1;
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "code": "STALE_CONTINUATION_TOKEN_SEQUENCER",
                "message": "the channel was reopened by another writer",
            })),
        );
    }

    if query.continuation_token != fake.continuation {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "code": "STALE_CONTINUATION_TOKEN_SEQUENCER",
                "message": "continuation token does not match",
            })),
        );
    }

    fake.appends.push(body);
    if let Some(end) = query.end_offset_token.clone() {
        fake.append_end_offsets.push(end);
    }
    fake.polls = 0;

    // A fresh continuation token per append, exactly as the real API does.
    fake.continuation = format!("{}-next", fake.continuation);
    let continuation = fake.continuation.clone();
    (
        StatusCode::OK,
        Json(serde_json::json!({ "next_continuation_token": continuation })),
    )
}

/// The real path is `…/pipes/{pipe}:bulk-channel-status` — a parameter and a literal in one
/// segment, which axum's router cannot express. The whole segment is captured instead and
/// the action split off here, so the sink still sends the exact documented URL.
async fn bulk_channel_status(
    State(state): State<Shared>,
    Path((_db, _schema, pipe_and_action)): Path<(String, String, String)>,
    Json(body): Json<serde_json::Value>,
) -> impl IntoResponse {
    assert!(
        pipe_and_action.ends_with(":bulk-channel-status"),
        "the sink must call the documented bulk-channel-status action, got {pipe_and_action:?}"
    );
    let mut fake = lock(&state);
    fake.polls += 1;

    // The commit lands only after the configured number of polls — this is what makes the
    // durability wait a real wait rather than an assertion about an already-true value.
    if !fake.never_commit
        && fake.polls >= fake.commit_after_polls
        && let Some(end) = fake.append_end_offsets.last().cloned()
    {
        fake.committed_offset_token = Some(end);
    }

    let channel = body["channel_names"][0]
        .as_str()
        .unwrap_or("rustcdc")
        .to_string();
    let committed = fake.committed_offset_token.clone();
    let rows_error_count = fake.rows_error_count;

    (
        StatusCode::OK,
        Json(serde_json::json!({
            "channel_statuses": {
                channel: {
                    "channel_status_code": "ACTIVE",
                    "last_committed_offset_token": committed,
                    "rows_inserted": 0,
                    "rows_parsed": 0,
                    "rows_error_count": rows_error_count,
                    "last_error_message": if rows_error_count > 0 {
                        serde_json::Value::String("column TS is not a TIMESTAMP".to_string())
                    } else {
                        serde_json::Value::Null
                    },
                }
            }
        })),
    )
}

/// Start the fake on an ephemeral loopback port.
async fn start_fake(state: Shared) -> SocketAddr {
    let app = Router::new()
        .route("/oauth/token", post(oauth_token))
        .route("/v2/streaming/hostname", get(hostname))
        .route(
            "/v2/streaming/databases/{db}/schemas/{schema}/pipes/{pipe}/channels/{channel}",
            put(open_channel),
        )
        .route(
            "/v2/streaming/data/databases/{db}/schemas/{schema}/pipes/{pipe}/channels/{channel}/rows",
            post(append_rows),
        )
        .route(
            "/v2/streaming/databases/{db}/schemas/{schema}/pipes/{pipe_and_action}",
            post(bulk_channel_status),
        )
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind the fake");
    let addr = listener.local_addr().expect("local addr");
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    addr
}

fn sink_config(addr: SocketAddr) -> rustcdc_server::config::schema::SinkConfig {
    sink_config_with_auth(
        addr,
        serde_json::json!({
            "type": "key_pair",
            "private_key": include_str!("fixtures/snowflake_test_key.pem"),
        }),
    )
}

fn sink_config_with_auth(
    addr: SocketAddr,
    auth: serde_json::Value,
) -> rustcdc_server::config::schema::SinkConfig {
    serde_json::from_value(serde_json::json!({
        "type": "snowflake",
        "account_url": format!("http://127.0.0.1:{}", addr.port()),
        "user": "cdc_svc",
        "account": "myorg-myaccount",
        "auth": auth,
        "database": "CDC",
        "schema": "PUBLIC",
        "pipe": "EVENTS_PIPE",
        "channel": "rustcdc",
        "batch_max_rows": 1000,
        "commit_timeout_ms": 5000,
        "commit_poll_ms": 5,
        "request_timeout_ms": 5000,
        "max_retries": 3,
    }))
    .expect("snowflake sink config")
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
        .expect("snowflake binding");
    rustcdc_server::pipeline::router::single(binding)
}

/// **The core durability property.**
///
/// `Append Rows` returning `200` means Snowflake buffered the rows. Reopening a channel
/// discards uncommitted buffered rows, so a `flush()` that returned on the append would let
/// the pipeline checkpoint past rows that vanish on the next restart.
///
/// The fake withholds the commit for several status polls; `flush()` must not return until
/// the committed offset token has actually reached the batch.
#[tokio::test]
async fn flush_waits_for_the_committed_offset_token_to_reach_the_batch() {
    let state: Shared = Arc::new(Mutex::new(FakeState {
        commit_after_polls: 4,
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
    assert_eq!(fake.appends.len(), 1, "one batch, one append");
    assert!(
        fake.polls >= 4,
        "flush must have polled the channel status until the commit landed, got {} poll(s)",
        fake.polls
    );
    assert_eq!(
        fake.committed_offset_token.as_deref(),
        Some("0/0004"),
        "the committed token must be the batch's last event offset"
    );
}

/// A commit that never lands must **fail** the flush.
///
/// Returning success would advance the checkpoint past rows that are not durable — the
/// silent-data-loss shape this sink's whole design exists to refuse.
#[tokio::test]
async fn a_commit_that_never_lands_fails_the_flush_rather_than_advancing() {
    let state: Shared = Arc::new(Mutex::new(FakeState {
        never_commit: true,
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
        .expect_err("a flush whose rows never commit must fail");

    assert!(
        error.to_string().contains("did not commit"),
        "the error must name the durability wait, not something incidental: {error}"
    );
    assert!(
        started.elapsed() >= Duration::from_millis(100),
        "the flush must actually have waited"
    );
}

/// Rows Snowflake rejected server-side are not in the table, so the flush is a failure.
#[tokio::test]
async fn server_side_row_errors_fail_the_flush() {
    let state: Shared = Arc::new(Mutex::new(FakeState {
        never_commit: true,
        rows_error_count: 2,
        ..Default::default()
    }));
    let addr = start_fake(Arc::clone(&state)).await;
    let mut router = build_router(addr).await;
    router.preflight_check().await.expect("preflight");

    router.send(&event(1)).await.expect("send");
    let error = router.flush().await.expect_err("row errors must fail");
    assert!(
        error.to_string().contains("row error"),
        "the error must name the rejected rows: {error}"
    );
}

/// A stale continuation token means another writer opened the channel. Reopen and retry —
/// failing here would stop a pipeline for something entirely recoverable.
#[tokio::test]
async fn a_stale_continuation_token_reopens_the_channel_and_retries() {
    let state: Shared = Arc::new(Mutex::new(FakeState {
        stale_appends_remaining: 1,
        ..Default::default()
    }));
    let addr = start_fake(Arc::clone(&state)).await;
    let mut router = build_router(addr).await;
    router.preflight_check().await.expect("preflight");

    router.send(&event(7)).await.expect("send");
    router
        .flush()
        .await
        .expect("a stale sequencer must be recovered, not surfaced");

    let fake = lock(&state);
    assert_eq!(
        fake.open_calls, 2,
        "the channel must have been reopened exactly once after the stale response"
    );
    assert_eq!(fake.appends.len(), 1, "the batch must land on the retry");
}

/// **The exactly-once property.**
///
/// A crash between a committed flush and the checkpoint write leaves Snowflake ahead. The
/// runtime replays that batch; the sink must recognise the committed offset token on
/// reopen and skip everything up to and including it.
#[tokio::test]
async fn a_replayed_batch_is_skipped_up_to_the_committed_offset_token() {
    let state: Shared = Arc::new(Mutex::new(FakeState {
        // Snowflake already has everything through 0/0002.
        committed_offset_token: Some("0/0002".to_string()),
        ..Default::default()
    }));
    let addr = start_fake(Arc::clone(&state)).await;
    let mut router = build_router(addr).await;
    router.preflight_check().await.expect("preflight");

    // The runtime replays from the checkpoint, which is at or before the committed token.
    for id in 0..5 {
        router.send(&event(id)).await.expect("send");
    }
    router.flush().await.expect("flush");

    let fake = lock(&state);
    assert_eq!(fake.appends.len(), 1, "one append");
    let body = &fake.appends[0];
    let lines: Vec<&str> = body.lines().filter(|l| !l.is_empty()).collect();
    assert_eq!(
        lines.len(),
        2,
        "only 0/0003 and 0/0004 are new; the first three were already committed:\n{body}"
    );
    assert!(
        lines[0].contains("\"0/0003\"") && lines[1].contains("\"0/0004\""),
        "the surviving rows must be the ones after the committed token:\n{body}"
    );
}

/// **The duplicate this sink very nearly shipped.**
///
/// `runtime.sink_flush_timeout_ms` defaults to 60 s and `commit_timeout_ms` defaulted to the
/// same, so the runtime's `tokio::time::timeout` would usually win the race and **drop the
/// `flush` future mid-wait**. The buffer had already been taken and appended; the run loop
/// then retried the whole batch; the resume filter was long since disarmed — and the rows
/// went to Snowflake twice. In the one sink that advertises `effectively_once`.
///
/// This drives that sequence exactly: append, abandon the wait, then re-send the same
/// events as the run loop does. The rows must not be appended a second time.
#[tokio::test]
async fn a_retry_after_an_abandoned_commit_wait_does_not_append_twice() {
    let state: Shared = Arc::new(Mutex::new(FakeState {
        // Withhold the commit long enough that the caller gives up on the wait…
        commit_after_polls: u32::MAX,
        ..Default::default()
    }));
    let addr = start_fake(Arc::clone(&state)).await;
    let mut router = build_router(addr).await;
    router.preflight_check().await.expect("preflight");

    for id in 0..3 {
        router.send(&event(id)).await.expect("send");
    }
    // …and abandon the flush the way the runtime's own timeout does: by dropping it.
    let abandoned = tokio::time::timeout(Duration::from_millis(200), router.flush()).await;
    assert!(abandoned.is_err(), "the flush must still have been waiting");

    assert_eq!(lock(&state).appends.len(), 1, "one append so far");

    // Snowflake did commit after all — the wait was simply abandoned too early.
    {
        let mut fake = lock(&state);
        fake.committed_offset_token = Some("0/0002".to_string());
        fake.commit_after_polls = 0;
    }

    // The run loop retries the whole batch.
    for id in 0..3 {
        router.send(&event(id)).await.expect("send");
    }
    router.flush().await.expect("the retry must succeed");

    let fake = lock(&state);
    assert_eq!(
        fake.appends.len(),
        1,
        "the retry must append nothing: all three rows were already committed, and \
         re-appending them is the duplicate this reconciliation exists to prevent.\n{:?}",
        fake.appends
    );
}

/// The body must be NDJSON: one JSON text per line, each terminated by `\n`.
///
/// The API rejects anything else, and a trailing-newline mistake is the classic way to lose
/// the last row of every batch.
#[tokio::test]
async fn the_request_body_is_newline_terminated_ndjson() {
    let state: Shared = Arc::new(Mutex::new(FakeState::default()));
    let addr = start_fake(Arc::clone(&state)).await;
    let mut router = build_router(addr).await;
    router.preflight_check().await.expect("preflight");

    for id in 0..3 {
        router.send(&event(id)).await.expect("send");
    }
    router.flush().await.expect("flush");

    let fake = lock(&state);
    let body = &fake.appends[0];
    assert!(
        body.ends_with('\n'),
        "every JSON text must be followed by a newline, including the last:\n{body:?}"
    );
    let lines: Vec<&str> = body.split_terminator('\n').collect();
    assert_eq!(lines.len(), 3);
    for line in lines {
        serde_json::from_str::<serde_json::Value>(line)
            .unwrap_or_else(|e| panic!("each line must be one complete JSON text: {e}\n{line}"));
    }
}

/// Closing must not drop the channel: the offset token *is* the durable record of what this
/// pipeline delivered, and dropping the channel deletes it — a restart would then re-deliver
/// everything the checkpoint had not yet covered.
#[tokio::test]
async fn closing_flushes_but_leaves_the_channel_intact() {
    let state: Shared = Arc::new(Mutex::new(FakeState::default()));
    let addr = start_fake(Arc::clone(&state)).await;
    let mut router = build_router(addr).await;
    router.preflight_check().await.expect("preflight");

    router.send(&event(1)).await.expect("send");
    router.close().await.expect("close must flush");

    let fake = lock(&state);
    assert_eq!(fake.appends.len(), 1, "close must flush what was buffered");
    assert_eq!(
        fake.committed_offset_token.as_deref(),
        Some("0/0001"),
        "and must wait for it to commit"
    );
}

/// **The credential must travel in `Authorization`, labelled.**
///
/// This one was very nearly shipped wrong. The Snowpipe Streaming reference lists only
/// `grant_type` and `scope` in the token-exchange *body*, and an earlier draft put the JWT
/// there as an `assertion=` parameter — where Snowflake ignores it. The credential goes in
/// the `Authorization` header, and the `X-Snowflake-Authorization-Token-Type` header names
/// what kind it is; omit that and Snowflake assumes `OAUTH` and rejects a key-pair JWT with
/// an error about the wrong thing.
#[tokio::test]
async fn the_credential_is_sent_as_a_labelled_authorization_header() {
    let state: Shared = Arc::new(Mutex::new(FakeState::default()));
    let addr = start_fake(Arc::clone(&state)).await;
    let mut router = build_router(addr).await;
    router.preflight_check().await.expect("preflight");

    let fake = lock(&state);
    let authorization = fake
        .exchange_authorization
        .as_deref()
        .expect("the exchange must carry an Authorization header");
    assert!(
        authorization.starts_with("Bearer eyJ"),
        "a key-pair credential is a JWT in the Authorization header, not a form field: \
         {authorization}"
    );
    assert_eq!(
        fake.exchange_token_type.as_deref(),
        Some("KEYPAIR_JWT"),
        "an unlabelled credential is assumed to be OAUTH"
    );
    assert!(
        fake.exchange_body.contains("grant_type=") && fake.exchange_body.contains("scope="),
        "the body carries the grant and scope: {:?}",
        fake.exchange_body
    );
    assert!(
        !fake.exchange_body.contains("assertion="),
        "the credential must not also be in the body: {:?}",
        fake.exchange_body
    );
}

/// A programmatic access token is sent verbatim, labelled as one.
#[tokio::test]
async fn a_programmatic_access_token_is_sent_verbatim() {
    let state: Shared = Arc::new(Mutex::new(FakeState::default()));
    let addr = start_fake(Arc::clone(&state)).await;
    let config = sink_config_with_auth(
        addr,
        serde_json::json!({ "type": "programmatic_access_token", "token": "pat-secret" }),
    );
    let binding = rustcdc_server::sink::build_binding(&config, 1 << 20)
        .await
        .expect("binding");
    let mut router = rustcdc_server::pipeline::router::single(binding);
    router.preflight_check().await.expect("preflight");

    let fake = lock(&state);
    assert_eq!(
        fake.exchange_authorization.as_deref(),
        Some("Bearer pat-secret")
    );
    assert_eq!(
        fake.exchange_token_type.as_deref(),
        Some("PROGRAMMATIC_ACCESS_TOKEN")
    );
}

/// Workload identity federation: the attestation is prefixed `WIF.<PROVIDER>.` and re-read
/// from disk on every exchange, because the platform rewrites it in place.
#[tokio::test]
async fn a_workload_identity_attestation_is_prefixed_and_re_read_each_time() {
    let dir = tempfile::tempdir().expect("tempdir");
    let token_file = dir.path().join("token");
    std::fs::write(
        &token_file,
        "first-attestation
",
    )
    .expect("write token");

    let state: Shared = Arc::new(Mutex::new(FakeState::default()));
    let addr = start_fake(Arc::clone(&state)).await;
    let config = sink_config_with_auth(
        addr,
        serde_json::json!({
            "type": "workload_identity",
            "provider": "oidc",
            "token_file": token_file,
        }),
    );
    let binding = rustcdc_server::sink::build_binding(&config, 1 << 20)
        .await
        .expect("binding");
    let mut router = rustcdc_server::pipeline::router::single(binding);
    router.preflight_check().await.expect("preflight");

    {
        let fake = lock(&state);
        assert_eq!(
            fake.exchange_authorization.as_deref(),
            Some("Bearer WIF.OIDC.first-attestation"),
            "the attestation must be prefixed and trimmed"
        );
        assert_eq!(
            fake.exchange_token_type.as_deref(),
            Some("WORKLOAD_IDENTITY_FEDERATION")
        );
    }

    // Kubernetes rewrites a projected service-account token in place at 80 % of its
    // lifetime. A token cached at startup stops working within the hour, and the failure
    // looks like an outage rather than a stale read — so the file is re-read every time.
    std::fs::write(
        &token_file,
        "rotated-attestation
",
    )
    .expect("rotate token");
    let binding = rustcdc_server::sink::build_binding(&config, 1 << 20)
        .await
        .expect("binding");
    let mut router = rustcdc_server::pipeline::router::single(binding);
    router.preflight_check().await.expect("preflight");

    let fake = lock(&state);
    assert_eq!(
        fake.exchange_authorization.as_deref(),
        Some("Bearer WIF.OIDC.rotated-attestation"),
        "the rotated attestation must be picked up without a restart"
    );
}

/// The sink reports itself as idempotent and effectively-once, which is what lets the
/// loader accept `delivery_contract = "effectively_once"` without a Kafka transaction.
#[tokio::test]
async fn the_sink_advertises_the_contract_its_design_actually_provides() {
    let state: Shared = Arc::new(Mutex::new(FakeState::default()));
    let addr = start_fake(Arc::clone(&state)).await;

    // On the binding, not the router: `TableRouter::name()` is the router's own label.
    let binding = rustcdc_server::sink::build_binding(&sink_config(addr), 1 << 20)
        .await
        .expect("snowflake binding");
    assert_eq!(binding.name(), "snowflake");

    let router = build_router(addr).await;
    assert!(
        router.idempotent_delivery_capable(),
        "resume filtering against the committed offset token is what this flag means"
    );
    assert!(
        !router.transactional_checkpoint_barrier_capable(),
        "there is no barrier here — durability comes from the commit wait, not a transaction"
    );
}
