//! A replication slot PostgreSQL invalidates is named, on both WAL transports, on
//! PostgreSQL 16 and 18.
//!
//! The slot is lost the way production loses it: `max_slot_wal_keep_size = 1MB`, WAL the
//! pipeline does not consume, and a checkpoint. Before the loss the slot must be reported
//! `unreserved` with a negative safe size; after it, the next poll fails with
//! `SourceErrorKind::SlotInvalidated` and the runtime's verdict is
//! `Stalled { SourcePositionLost }`. A slot invalidated by PostgreSQL 18's
//! `idle_replication_slot_timeout` while nothing ran is refused at startup the same way.
#![cfg(feature = "postgres")]

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rustcdc::{
    CdcRuntime, ConnectionRetryPolicy, Error, MetricsCollector, Operation, PostgresSourceConfig,
    RuntimeConfig, RuntimeSourceConfig, SlotWalStatus, SourceErrorKind, StallCause, WalTransport,
    checkpoint::FileCheckpoint, schema_history::InMemorySchemaHistory,
};
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};

fn docker_enabled(name: &str) -> bool {
    let enabled = std::env::var("CDC_RS_RUN_DOCKER_TESTS").as_deref() == Ok("1");
    if !enabled {
        eprintln!("skipping {name} (set CDC_RS_RUN_DOCKER_TESTS=1)");
    }
    enabled
}

fn source_error(error: impl std::error::Error) -> Error {
    Error::SourceError(rustcdc::render_error_chain(&error))
}

async fn start_postgres(
    tag: &str,
    extra: &[&str],
) -> rustcdc::Result<ContainerAsync<GenericImage>> {
    let mut cmd = vec![
        "postgres",
        "-c",
        "wal_level=logical",
        "-c",
        "max_replication_slots=8",
        "-c",
        "max_wal_senders=8",
    ];
    cmd.extend_from_slice(extra);
    GenericImage::new("postgres", tag)
        .with_exposed_port(5432.tcp())
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_USER", "postgres")
        .with_env_var("POSTGRES_PASSWORD", "postgres")
        .with_env_var("POSTGRES_DB", "cdc")
        .with_cmd(cmd)
        .start()
        .await
        .map_err(source_error)
}

async fn admin_client(
    container: &ContainerAsync<GenericImage>,
) -> rustcdc::Result<(tokio_postgres::Client, String, u16)> {
    let host = container
        .get_host()
        .await
        .map_err(source_error)?
        .to_string();
    let port = container
        .get_host_port_ipv4(5432.tcp())
        .await
        .map_err(source_error)?;
    let dsn = format!(
        "host={host} port={port} user=postgres password=postgres dbname=cdc connect_timeout=30"
    );
    let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
        .await
        .map_err(source_error)?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    Ok((client, host, port))
}

/// A captured table, a filler table outside the publication, and the slot.
async fn prepare(admin: &tokio_postgres::Client, slot: &str) -> rustcdc::Result<()> {
    admin
        .batch_execute(
            "CREATE TABLE public.captured (id BIGINT PRIMARY KEY, name TEXT);
             CREATE TABLE public.filler (id BIGINT, payload TEXT);
             CREATE PUBLICATION slot_pub FOR TABLE public.captured;",
        )
        .await
        .map_err(source_error)?;
    admin
        .execute(
            "SELECT pg_create_logical_replication_slot($1, 'pgoutput')",
            &[&slot],
        )
        .await
        .map_err(source_error)?;
    Ok(())
}

/// About 100 MB of WAL in a table the publication does not cover, so nothing reaches the
/// pipeline and nothing advances the slot.
async fn burst_wal(admin: &tokio_postgres::Client) -> rustcdc::Result<()> {
    for _ in 0..2 {
        admin
            .batch_execute(
                "INSERT INTO public.filler \
                 SELECT g, repeat(md5(g::text), 40) FROM generate_series(1, 40000) g",
            )
            .await
            .map_err(source_error)?;
    }
    admin
        .batch_execute("SELECT pg_switch_wal()")
        .await
        .map_err(source_error)?;
    Ok(())
}

/// Records the context of every error the runtime reports.
#[derive(Default)]
struct ErrorContexts(Mutex<Vec<String>>);

impl MetricsCollector for ErrorContexts {
    fn record_event_processed(&self, _op: Operation, _latency_ms: u64) {}
    fn record_checkpoint_committed(&self, _event_count: u64, _latency_ms: u64) {}
    fn record_replication_lag_ms(&self, _lag_ms: u64, _lag_events: u64) {}
    fn record_replication_slot_lag_bytes(&self, _lag_bytes: u64) {}
    fn record_error(&self, _error: &Error, context: &str) {
        self.0.lock().unwrap().push(context.to_string());
    }
}

fn runtime_for(
    host: &str,
    port: u16,
    slot: &str,
    transport: WalTransport,
    checkpoint_dir: &std::path::Path,
    contexts: Arc<ErrorContexts>,
) -> rustcdc::Result<CdcRuntime> {
    let source = PostgresSourceConfig {
        host: host.to_string(),
        port,
        user: "postgres".into(),
        password: "postgres".to_string().into(),
        database: "cdc".into(),
        replication_slot_name: slot.into(),
        publication_name: "slot_pub".into(),
        transport: rustcdc::TransportConfig::plaintext(),
        conn_timeout_secs: 30,
        stream_poll_interval_ms: 50,
        wal_transport: transport,
        // Idle advance would confirm the server's position and rescue the slot.
        slot_idle_advance_interval_ms: 0,
        slot_state_sample_interval_ms: Some(100),
        ..PostgresSourceConfig::default()
    };
    let mut config = RuntimeConfig::new(
        RuntimeSourceConfig::Postgres(source),
        FileCheckpoint::new(checkpoint_dir),
        InMemorySchemaHistory::default(),
    )
    .with_max_poll_wait_ms(200)
    .with_metrics(contexts);
    config.options.connection_retry = Some(ConnectionRetryPolicy {
        max_retries: Some(5),
        initial_delay_ms: 50,
        max_delay_ms: 200,
    });
    CdcRuntime::new(config)
}

async fn a_slot_invalidated_mid_run_is_named(
    tag: &str,
    transport: WalTransport,
) -> rustcdc::Result<()> {
    let container = start_postgres(tag, &["-c", "max_slot_wal_keep_size=1MB"]).await?;
    let (admin, host, port) = admin_client(&container).await?;
    let slot = "lost_slot";
    prepare(&admin, slot).await?;

    let checkpoint_dir = tempfile::tempdir().map_err(source_error)?;
    let contexts = Arc::new(ErrorContexts::default());
    let mut runtime = runtime_for(
        &host,
        port,
        slot,
        transport,
        checkpoint_dir.path(),
        Arc::clone(&contexts),
    )?;
    runtime.start().await?;
    let first = runtime.poll_event_batch().await?;
    assert!(first.is_empty());

    // Before the loss: the slot is past its keep size and reports so.
    burst_wal(&admin).await?;
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let batch = runtime.poll_event_batch().await?;
        assert!(batch.is_empty(), "the filler table is not published");
        let snapshot = runtime.admin_snapshot();
        if snapshot.replication_slot_wal_status == Some(SlotWalStatus::Unreserved) {
            let safe = snapshot.replication_slot_safe_wal_size_bytes;
            assert!(
                safe.is_some_and(|bytes| bytes < 0),
                "safe_wal_size {safe:?}"
            );
            break;
        }
        assert!(
            Instant::now() < deadline,
            "[{tag} {transport:?}] the slot never reported unreserved: {:?}",
            snapshot.replication_slot_wal_status
        );
    }

    // The loss: a checkpoint removes the WAL the slot needs.
    admin
        .batch_execute("CHECKPOINT")
        .await
        .map_err(source_error)?;
    let errors_before = contexts.0.lock().unwrap().len();
    let deadline = Instant::now() + Duration::from_secs(60);
    let error = loop {
        match runtime.poll_event_batch().await {
            Ok(batch) => assert!(batch.is_empty()),
            Err(error) => break error,
        }
        assert!(
            Instant::now() < deadline,
            "[{tag} {transport:?}] no poll failed after the slot was invalidated"
        );
    };

    assert_eq!(
        error.source_kind(),
        Some(SourceErrorKind::SlotInvalidated),
        "[{tag} {transport:?}] {}",
        error.report()
    );
    let report = error.report().to_string();
    assert!(report.contains(slot), "{report}");
    if tag.starts_with("18") {
        assert!(report.contains("wal_removed"), "{report}");
    }
    if transport == WalTransport::SqlPeek {
        // The first failing poll is already the verdict: no reconnect was attempted.
        let contexts = contexts.0.lock().unwrap();
        assert!(
            !contexts[errors_before..]
                .iter()
                .any(|context| context == "runtime.poll.stream_retry"),
            "[{tag}] the poll path must classify on its own: {contexts:?}"
        );
    }
    let snapshot = runtime.admin_snapshot();
    assert_eq!(
        snapshot.health.stall_cause(),
        Some(StallCause::SourcePositionLost),
        "[{tag} {transport:?}] {:?}",
        snapshot.health
    );
    assert_eq!(
        snapshot.replication_slot_wal_status,
        Some(SlotWalStatus::Lost)
    );
    let _ = runtime.force_stop().await;
    Ok(())
}

#[tokio::test]
async fn a_slot_invalidated_mid_run_is_named_on_both_transports() -> rustcdc::Result<()> {
    if !docker_enabled("postgres slot invalidation test") {
        return Ok(());
    }
    for tag in ["16-alpine", "18-alpine"] {
        for transport in [WalTransport::SqlPeek, WalTransport::StreamingReplication] {
            a_slot_invalidated_mid_run_is_named(tag, transport).await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn an_invalidated_slot_is_refused_at_startup() -> rustcdc::Result<()> {
    if !docker_enabled("postgres idle slot startup test") {
        return Ok(());
    }
    // Seconds on PostgreSQL 18; the invalidation happens at the next checkpoint.
    let container = start_postgres("18-alpine", &["-c", "idle_replication_slot_timeout=1"]).await?;
    let (admin, host, port) = admin_client(&container).await?;
    let slot = "idle_slot";
    prepare(&admin, slot).await?;
    tokio::time::sleep(Duration::from_secs(3)).await;
    admin
        .batch_execute("CHECKPOINT")
        .await
        .map_err(source_error)?;

    let checkpoint_dir = tempfile::tempdir().map_err(source_error)?;
    let mut runtime = runtime_for(
        &host,
        port,
        slot,
        WalTransport::StreamingReplication,
        checkpoint_dir.path(),
        Arc::new(ErrorContexts::default()),
    )?;
    let error = runtime
        .start()
        .await
        .expect_err("an invalidated slot must be refused at startup");
    assert_eq!(
        error.source_kind(),
        Some(SourceErrorKind::SlotInvalidated),
        "{}",
        error.report()
    );
    assert!(
        error.report().to_string().contains("idle_timeout"),
        "{}",
        error.report()
    );
    assert_eq!(
        runtime.admin_snapshot().health.stall_cause(),
        Some(StallCause::SourcePositionLost)
    );
    Ok(())
}

/// The `START_REPLICATION` refusal is classified on its own path. `connect()` validates
/// the slot while it is still intact; it is then lost before the stream opens, so only the
/// refusal path in `start_stream` can name it.
#[tokio::test]
async fn a_refused_start_replication_is_classified_as_an_invalidated_slot() -> rustcdc::Result<()> {
    if !docker_enabled("postgres refused START_REPLICATION test") {
        return Ok(());
    }
    let container = start_postgres("16-alpine", &["-c", "max_slot_wal_keep_size=1MB"]).await?;
    let (admin, host, port) = admin_client(&container).await?;
    let slot = "refused_slot";
    prepare(&admin, slot).await?;

    let mut connection = rustcdc::PostgresConnection::new(PostgresSourceConfig {
        host,
        port,
        user: "postgres".into(),
        password: "postgres".to_string().into(),
        database: "cdc".into(),
        replication_slot_name: slot.into(),
        publication_name: "slot_pub".into(),
        transport: rustcdc::TransportConfig::plaintext(),
        conn_timeout_secs: 30,
        wal_transport: WalTransport::StreamingReplication,
        slot_idle_advance_interval_ms: 0,
        ..PostgresSourceConfig::default()
    });
    rustcdc::source::Source::connect(&connection).await?;

    burst_wal(&admin).await?;
    admin
        .batch_execute("CHECKPOINT")
        .await
        .map_err(source_error)?;

    let error = match rustcdc::source::Source::start_stream(&mut connection, None).await {
        Ok(_) => panic!("a lost slot must refuse START_REPLICATION"),
        Err(error) => error,
    };
    assert_eq!(
        error.source_kind(),
        Some(SourceErrorKind::SlotInvalidated),
        "{}",
        error.report()
    );
    Ok(())
}
