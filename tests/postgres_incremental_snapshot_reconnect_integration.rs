//! An incremental snapshot must survive a mid-flight reconnect.
//!
//! An incremental snapshot is delivered by a driver that **wraps** the log stream: it owns the
//! per-table chunk cursors and reports them through `StreamHandle::incremental_snapshot_state`,
//! which is what puts those cursors into every checkpoint record.
//!
//! The runtime's reconnect path used to rebuild the stream with a plain `start_stream`, losing
//! the driver. That did two damaging things at once, neither of them visible:
//!
//! 1. The snapshot **stopped progressing** — no further chunk was ever read, so it never
//!    completed.
//! 2. A plain stream reports no snapshot state, so every checkpoint written after the reconnect
//!    **erased the progress record**. A later restart then found no snapshot in flight at all,
//!    and the un-read tables were neither resumed nor reported missing.
//!
//! Any transient network error during a snapshot reached that path, and a snapshot of a large
//! table is a long window. This test provokes the disconnect the way production does — the
//! walsender goes away — and asserts the snapshot still finishes.
//!
//! The second test covers the on-demand path: `CdcRuntime::request_incremental_snapshot`
//! backfilling a table on a pipeline that is already running.

#![cfg(feature = "postgres")]

use std::collections::BTreeSet;

use rustcdc::{
    checkpoint::FileCheckpoint, schema_history::InMemorySchemaHistory, ConnectionRetryPolicy,
    IncrementalSnapshotConfig, Operation, PostgresSourceConfig, RuntimeConfig, RuntimeSourceConfig,
};
use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    GenericImage, ImageExt,
};

const SLOT: &str = "incremental_reconnect_slot";
const ROWS: i64 = 400;
/// Small enough that the snapshot spans many chunks, so the disconnect lands mid-snapshot
/// rather than after it.
const CHUNK_SIZE: usize = 25;

#[tokio::test]
async fn an_incremental_snapshot_survives_a_reconnect_and_still_completes() -> rustcdc::Result<()> {
    if std::env::var("CDC_RS_RUN_DOCKER_TESTS").as_deref() != Ok("1") {
        eprintln!(
            "skipping postgres incremental-snapshot reconnect test (set CDC_RS_RUN_DOCKER_TESTS=1)"
        );
        return Ok(());
    }

    let container = GenericImage::new("postgres", "16-alpine")
        .with_exposed_port(5432.tcp())
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_USER", "postgres")
        .with_env_var("POSTGRES_PASSWORD", "postgres")
        .with_env_var("POSTGRES_DB", "cdc")
        .with_cmd(vec![
            "postgres",
            "-c",
            "wal_level=logical",
            "-c",
            "max_replication_slots=8",
            "-c",
            "max_wal_senders=8",
        ])
        .start()
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

    let host = container
        .get_host()
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?
        .to_string();
    let port = container
        .get_host_port_ipv4(5432.tcp())
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

    let dsn = format!(
        "host={host} port={port} user=postgres password=postgres dbname=cdc connect_timeout=30"
    );
    let (admin, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;
    tokio::spawn(async move {
        let _ = connection.await;
    });

    admin
        .batch_execute(
            "
            CREATE TABLE public.reconnect_snapshot (id BIGINT PRIMARY KEY, payload TEXT NOT NULL);
            ALTER TABLE public.reconnect_snapshot REPLICA IDENTITY FULL;
            CREATE PUBLICATION reconnect_snapshot_pub FOR TABLE public.reconnect_snapshot;
            ",
        )
        .await
        .map_err(|error| rustcdc::Error::SourceError(rustcdc::render_error_chain(&error)))?;

    // Seed before the slot exists: these rows are the snapshot's job, not the stream's.
    for id in 1..=ROWS {
        admin
            .execute(
                "INSERT INTO public.reconnect_snapshot (id, payload) VALUES ($1, $2)",
                &[&id, &format!("row-{id}")],
            )
            .await
            .map_err(|error| rustcdc::Error::SourceError(rustcdc::render_error_chain(&error)))?;
    }

    admin
        .execute(
            "SELECT pg_create_logical_replication_slot($1, 'pgoutput')",
            &[&SLOT],
        )
        .await
        .map_err(|error| rustcdc::Error::SourceError(rustcdc::render_error_chain(&error)))?;

    let checkpoint_dir = tempfile::tempdir().map_err(rustcdc::Error::IoError)?;
    let source_cfg = PostgresSourceConfig {
        host: host.clone(),
        port,
        user: "postgres".into(),
        password: "postgres".to_string().into(),
        database: "cdc".into(),
        replication_slot_name: SLOT.into(),
        publication_name: "reconnect_snapshot_pub".into(),
        // The container runs with `ssl = off`.
        transport: rustcdc::TransportConfig::plaintext(),
        stream_poll_interval_ms: 50,
        max_events_per_poll: 200,
        ..PostgresSourceConfig::default()
    };

    let mut runtime = rustcdc::CdcRuntime::new(
        RuntimeConfig::new(
            RuntimeSourceConfig::Postgres(source_cfg),
            FileCheckpoint::new(checkpoint_dir.path()),
            InMemorySchemaHistory::default(),
        )
        .with_incremental_snapshot(
            IncrementalSnapshotConfig::new(vec!["public.reconnect_snapshot".to_string()])
                .with_chunk_size(CHUNK_SIZE),
        )
        // Retry is on by default; the tighter delays here just keep the test quick. Without
        // a retry policy a dropped connection surfaces to the caller instead of reconnecting,
        // and the path under test is never taken.
        .with_options(
            rustcdc::RuntimeOptions::new().with_connection_retry(
                ConnectionRetryPolicy::new()
                    .with_max_retries(Some(20))
                    .with_initial_delay_ms(100)
                    .with_max_delay_ms(500),
            ),
        )
        .with_max_buffer_size(500)
        .with_max_poll_wait_ms(500),
    )?;

    runtime.start().await?;

    let mut seen: BTreeSet<i64> = BTreeSet::new();
    let mut terminated_walsender = false;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(180);

    while seen.len() < ROWS as usize && std::time::Instant::now() < deadline {
        let batch = runtime.poll_event_batch().await?;

        for event in batch.events() {
            if event.op != Operation::Read {
                continue;
            }
            let id = event
                .after
                .as_ref()
                .and_then(|after| after.get("id"))
                .and_then(|value| {
                    value
                        .as_i64()
                        .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
                })
                .ok_or_else(|| {
                    rustcdc::Error::SourceError("snapshot row is missing its id".into())
                })?;
            assert!(
                seen.insert(id),
                "the snapshot delivered row {id} twice; the reconnect must resume at a chunk \
                 boundary, not replay rows already emitted"
            );
        }

        runtime.commit_ack(batch.ack_mode()).await?;

        // Drop the replication connection exactly once, after real progress but well before
        // the snapshot can finish. Terminating the walsender is what a network blip, a
        // failover, or an idle-timeout proxy looks like to the client.
        if !terminated_walsender && seen.len() >= CHUNK_SIZE && seen.len() < ROWS as usize / 2 {
            let killed = admin
                .execute(
                    "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots \
                     WHERE slot_name = $1 AND active_pid IS NOT NULL",
                    &[&SLOT],
                )
                .await
                .map_err(|error| {
                    rustcdc::Error::SourceError(rustcdc::render_error_chain(&error))
                })?;
            if killed > 0 {
                terminated_walsender = true;
                eprintln!(
                    "terminated the walsender after {} of {ROWS} snapshot rows",
                    seen.len()
                );
            }
        }
    }

    assert!(
        terminated_walsender,
        "the test never managed to drop the replication connection, so it proved nothing"
    );
    assert_eq!(
        seen.len(),
        ROWS as usize,
        "the incremental snapshot must still complete after a reconnect. Missing rows mean the \
         reconnect rebuilt a plain stream and dropped the snapshot driver, which also erases \
         the cursor from every later checkpoint. Highest row seen: {:?}",
        seen.iter().next_back()
    );

    Ok(())
}

#[tokio::test]
async fn a_table_requested_at_runtime_is_backfilled_without_a_restart() -> rustcdc::Result<()> {
    if std::env::var("CDC_RS_RUN_DOCKER_TESTS").as_deref() != Ok("1") {
        eprintln!("skipping postgres on-demand snapshot test (set CDC_RS_RUN_DOCKER_TESTS=1)");
        return Ok(());
    }

    const SLOT_ONDEMAND: &str = "incremental_ondemand_slot";
    const LATE_ROWS: i64 = 120;

    let container = GenericImage::new("postgres", "16-alpine")
        .with_exposed_port(5432.tcp())
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_USER", "postgres")
        .with_env_var("POSTGRES_PASSWORD", "postgres")
        .with_env_var("POSTGRES_DB", "cdc")
        .with_cmd(vec![
            "postgres",
            "-c",
            "wal_level=logical",
            "-c",
            "max_replication_slots=8",
            "-c",
            "max_wal_senders=8",
        ])
        .start()
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

    let host = container
        .get_host()
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?
        .to_string();
    let port = container
        .get_host_port_ipv4(5432.tcp())
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

    let dsn = format!(
        "host={host} port={port} user=postgres password=postgres dbname=cdc connect_timeout=30"
    );
    let (admin, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;
    tokio::spawn(async move {
        let _ = connection.await;
    });

    // `configured` is snapshotted from the start; `late_arrival` is the table an operator
    // decides to backfill after the pipeline is already running.
    admin
        .batch_execute(
            "
            CREATE TABLE public.configured (id BIGINT PRIMARY KEY, payload TEXT NOT NULL);
            CREATE TABLE public.late_arrival (id BIGINT PRIMARY KEY, payload TEXT NOT NULL);
            ALTER TABLE public.configured REPLICA IDENTITY FULL;
            ALTER TABLE public.late_arrival REPLICA IDENTITY FULL;
            CREATE PUBLICATION ondemand_pub FOR TABLE public.configured, public.late_arrival;
            INSERT INTO public.configured (id, payload) VALUES (1, 'seed');
            ",
        )
        .await
        .map_err(|error| rustcdc::Error::SourceError(rustcdc::render_error_chain(&error)))?;

    for id in 1..=LATE_ROWS {
        admin
            .execute(
                "INSERT INTO public.late_arrival (id, payload) VALUES ($1, $2)",
                &[&id, &format!("late-{id}")],
            )
            .await
            .map_err(|error| rustcdc::Error::SourceError(rustcdc::render_error_chain(&error)))?;
    }

    admin
        .execute(
            "SELECT pg_create_logical_replication_slot($1, 'pgoutput')",
            &[&SLOT_ONDEMAND],
        )
        .await
        .map_err(|error| rustcdc::Error::SourceError(rustcdc::render_error_chain(&error)))?;

    let checkpoint_dir = tempfile::tempdir().map_err(rustcdc::Error::IoError)?;
    let source_cfg = PostgresSourceConfig {
        host: host.clone(),
        port,
        user: "postgres".into(),
        password: "postgres".to_string().into(),
        database: "cdc".into(),
        replication_slot_name: SLOT_ONDEMAND.into(),
        publication_name: "ondemand_pub".into(),
        transport: rustcdc::TransportConfig::plaintext(),
        stream_poll_interval_ms: 50,
        max_events_per_poll: 200,
        ..PostgresSourceConfig::default()
    };

    let mut runtime = rustcdc::CdcRuntime::new(
        RuntimeConfig::new(
            RuntimeSourceConfig::Postgres(source_cfg),
            FileCheckpoint::new(checkpoint_dir.path()),
            InMemorySchemaHistory::default(),
        )
        // Only `configured` is in the static list; `late_arrival` arrives by request.
        .with_incremental_snapshot(
            IncrementalSnapshotConfig::new(vec!["public.configured".to_string()])
                .with_chunk_size(CHUNK_SIZE),
        )
        .with_max_buffer_size(500)
        .with_max_poll_wait_ms(300),
    )?;

    runtime.start().await?;

    // Drain the configured table first so the driver reaches `Done` — the state an on-demand
    // request must be able to bring back to life.
    for _ in 0..20 {
        let batch = runtime.poll_event_batch().await?;
        let empty = batch.is_empty();
        runtime.commit_ack(batch.ack_mode()).await?;
        if empty {
            break;
        }
    }

    let enqueued = runtime
        .request_incremental_snapshot(vec!["public.late_arrival".to_string()])
        .await?;
    assert_eq!(enqueued, 1, "the request must enqueue exactly one table");

    // A second, identical request must be a safe no-op rather than a rewind.
    assert_eq!(
        runtime
            .request_incremental_snapshot(vec!["public.late_arrival".to_string()])
            .await?,
        0,
        "re-requesting a table already in progress must not restart it"
    );

    let mut late_ids: BTreeSet<i64> = BTreeSet::new();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(120);
    while late_ids.len() < LATE_ROWS as usize && std::time::Instant::now() < deadline {
        let batch = runtime.poll_event_batch().await?;
        for event in batch.events() {
            if event.op != Operation::Read || event.table != "late_arrival" {
                continue;
            }
            let id = event
                .after
                .as_ref()
                .and_then(|after| after.get("id"))
                .and_then(|value| {
                    value
                        .as_i64()
                        .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
                })
                .ok_or_else(|| {
                    rustcdc::Error::SourceError("snapshot row is missing its id".into())
                })?;
            assert!(late_ids.insert(id), "row {id} was delivered twice");
        }
        runtime.commit_ack(batch.ack_mode()).await?;
    }

    assert_eq!(
        late_ids.len(),
        LATE_ROWS as usize,
        "the on-demand snapshot must read every row of a table that was never in the static \
         config; got {} of {LATE_ROWS}",
        late_ids.len()
    );

    // The request must be durable: the table is absent from `config.tables`, so its progress
    // survives only if the driver adopts it from the checkpoint.
    let persisted = std::fs::read_to_string(checkpoint_dir.path().join("checkpoint_postgres.json"))
        .map_err(rustcdc::Error::IoError)?;
    assert!(
        persisted.contains("late_arrival"),
        "the requested table must appear in the durable checkpoint, or a restart would forget \
         it: {persisted}"
    );

    Ok(())
}
