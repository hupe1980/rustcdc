//! A per-table snapshot row filter must bound the chunk reads and nothing else.
//!
//! `IncrementalSnapshotConfig::with_table_condition` is the equivalent of Debezium's
//! `additional-condition`: backfill one tenant, or only rows past a cutoff, instead of the
//! whole table.
//!
//! The property worth testing is the boundary, not the SQL. A filter restricts **which rows
//! the snapshot reads**; it must never restrict the live stream, or it would quietly become a
//! capture filter and drop change data for rows it excludes — the kind of loss that looks
//! like correct behaviour until someone reconciles row counts months later.

#![cfg(feature = "postgres")]

use std::collections::BTreeSet;

use rustcdc::{
    checkpoint::FileCheckpoint, schema_history::InMemorySchemaHistory, IncrementalSnapshotConfig,
    Operation, PostgresSourceConfig, RuntimeConfig, RuntimeSourceConfig,
};
use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    GenericImage, ImageExt,
};

const SLOT: &str = "snapshot_filter_slot";

#[tokio::test(flavor = "multi_thread")]
async fn a_snapshot_row_filter_bounds_the_chunk_reads_but_not_the_stream() -> rustcdc::Result<()> {
    if std::env::var("CDC_RS_RUN_DOCKER_TESTS").as_deref() != Ok("1") {
        eprintln!("skipping snapshot filter test (set CDC_RS_RUN_DOCKER_TESTS=1)");
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
            "CREATE TABLE public.filtered (id BIGINT PRIMARY KEY, tenant INT NOT NULL);
             ALTER TABLE public.filtered REPLICA IDENTITY FULL;
             CREATE PUBLICATION filtered_pub FOR TABLE public.filtered;",
        )
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

    // Seeded before the slot exists, so these rows are the snapshot's job, not the stream's.
    for id in 1..=40i64 {
        let tenant = if id % 2 == 1 { 1i32 } else { 2i32 };
        admin
            .execute(
                "INSERT INTO public.filtered (id, tenant) VALUES ($1, $2)",
                &[&id, &tenant],
            )
            .await
            .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;
    }

    admin
        .execute(
            "SELECT pg_create_logical_replication_slot($1, 'pgoutput')",
            &[&SLOT],
        )
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

    let checkpoint_dir = tempfile::tempdir().map_err(rustcdc::Error::IoError)?;
    let source_cfg = PostgresSourceConfig {
        host: host.clone(),
        port,
        user: "postgres".into(),
        password: "postgres".to_string().into(),
        database: "cdc".into(),
        replication_slot_name: SLOT.into(),
        publication_name: "filtered_pub".into(),
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
        // Small chunks so the filter is exercised across several reads, not just one.
        .with_incremental_snapshot(
            IncrementalSnapshotConfig::new(vec!["public.filtered".to_string()])
                .with_chunk_size(7)
                .with_table_condition("public.filtered", "tenant = 1"),
        )
        .with_max_buffer_size(500)
        .with_max_poll_wait_ms(300),
    )?;

    runtime.start().await?;

    // A change to a row the filter *excludes*. The stream must still deliver it.
    admin
        .execute(
            "INSERT INTO public.filtered (id, tenant) VALUES (999, 2)",
            &[],
        )
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

    let mut snapshot_ids: BTreeSet<i64> = BTreeSet::new();
    let mut streamed_ids: BTreeSet<i64> = BTreeSet::new();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(90);
    let mut quiet = 0;

    while std::time::Instant::now() < deadline {
        let batch = runtime.poll_event_batch().await?;
        if batch.is_empty() {
            quiet += 1;
            // Both properties have had a chance to show up before we stop.
            if quiet > 15 && !snapshot_ids.is_empty() && !streamed_ids.is_empty() {
                break;
            }
            continue;
        }
        quiet = 0;
        for event in batch.events() {
            // Read representation-agnostically. Snapshot rows arrive from `row_to_json`
            // (typed JSON) while stream rows arrive from pgoutput's text format, so the same
            // column is `1` in one and `"999"` in the other — see the value-representation
            // note in the API guide. This test is about *which rows* are read, not how they
            // are typed, so it must not depend on that.
            let Some(id) = event.after.as_ref().and_then(|row| match row.get("id") {
                Some(serde_json::Value::Number(n)) => n.as_i64(),
                Some(serde_json::Value::String(s)) => s.parse::<i64>().ok(),
                _ => None,
            }) else {
                continue;
            };
            if event.op == Operation::Read {
                snapshot_ids.insert(id);
            } else {
                streamed_ids.insert(id);
            }
        }
        runtime.commit_ack(batch.ack_mode()).await?;
    }

    let expected: BTreeSet<i64> = (1..=40i64).filter(|id| id % 2 == 1).collect();
    assert_eq!(
        snapshot_ids, expected,
        "the snapshot must read exactly the rows the filter selects — no more, and no fewer"
    );
    assert!(
        streamed_ids.contains(&999),
        "the live stream must still deliver a change to a row the filter excludes; a filter \
         that reached the stream would be silently dropping change data. got: {streamed_ids:?}"
    );

    let _ = runtime.force_stop().await;
    Ok(())
}
