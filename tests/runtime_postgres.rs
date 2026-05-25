#![cfg(feature = "postgres")]

use rustcdc::{
    checkpoint::{Checkpoint, FileCheckpoint, PostgresOffset},
    schema_history::InMemorySchemaHistory,
    CdcRuntime, PostgresSourceConfig, RuntimeConfig, RuntimeSourceConfig,
};
use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    GenericImage, ImageExt,
};

#[tokio::test]
async fn runtime_postgres_stream_resume_from_checkpoint() -> rustcdc::Result<()> {
    if std::env::var("CDC_RS_RUN_DOCKER_TESTS").as_deref() != Ok("1") {
        eprintln!("skipping postgres runtime integration test (set CDC_RS_RUN_DOCKER_TESTS=1)");
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
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;
    let port = container
        .get_host_port_ipv4(5432.tcp())
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

    let admin_dsn = format!(
        "host={host} port={port} user=postgres password=postgres dbname=cdc connect_timeout=30"
    );
    let (admin_client, admin_conn) = tokio_postgres::connect(&admin_dsn, tokio_postgres::NoTls)
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;
    tokio::spawn(async move {
        let _ = admin_conn.await;
    });

    admin_client
        .batch_execute(
            "
            CREATE TABLE IF NOT EXISTS public.runtime_users (
              id BIGINT PRIMARY KEY,
              payload TEXT NOT NULL
            );
            ALTER TABLE public.runtime_users REPLICA IDENTITY FULL;
            DROP PUBLICATION IF EXISTS cdc_runtime_pub;
            CREATE PUBLICATION cdc_runtime_pub FOR TABLE public.runtime_users;
            ",
        )
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

    let checkpoint_dir = tempfile::tempdir().map_err(rustcdc::Error::IoError)?;

    let source_cfg = PostgresSourceConfig {
        host: host.to_string(),
        port,
        user: "postgres".to_string(),
        password: "postgres".into(),
        database: "cdc".to_string(),
        replication_slot_name: "cdc_runtime_slot".to_string(),
        publication_name: "cdc_runtime_pub".to_string(),
        conn_timeout_secs: 30,
        stream_poll_interval_ms: 50,
        max_events_per_poll: 1_000,
        ..PostgresSourceConfig::default()
    };

    let mut runtime = CdcRuntime::new(
        RuntimeConfig::new(
            RuntimeSourceConfig::Postgres(source_cfg.clone()),
            FileCheckpoint::new(checkpoint_dir.path()),
            InMemorySchemaHistory::default(),
        )
        .with_max_buffer_size(256)
        .with_max_poll_wait_ms(150),
    )?;

    runtime.start().await?;

    admin_client
        .batch_execute("TRUNCATE TABLE public.runtime_users;")
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

    for id in 1_i64..=100_i64 {
        admin_client
            .execute(
                "INSERT INTO public.runtime_users (id, payload) VALUES ($1, $2)",
                &[&id, &format!("payload-{id}")],
            )
            .await
            .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;
    }

    let first_batch = poll_non_empty_batch(&mut runtime, 40).await?;
    assert!(first_batch.len() >= 100);

    let token = first_batch
        .ack_token()
        .expect("non-empty batch should include ack token");
    let (accepted, _remaining) = token.split_at(50)?;
    runtime.commit_ack(accepted).await?;

    let reader = FileCheckpoint::new(checkpoint_dir.path());
    assert_eq!(reader.get_committed_count().await?, 50);
    let saved = reader
        .load()
        .await?
        .ok_or_else(|| rustcdc::Error::StateError("checkpoint should exist after commit".into()))?;
    let saved_offset = PostgresOffset::from_bytes(&saved.encode()?)?;
    let target_lsn = format_pg_lsn(saved_offset.lsn);
    let advance_sql = format!(
        "SELECT end_lsn::text FROM pg_replication_slot_advance('cdc_runtime_slot', '{target_lsn}'::pg_lsn)"
    );
    admin_client
        .query_one(&advance_sql, &[])
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

    drop(runtime);

    let mut resumed = CdcRuntime::new(
        RuntimeConfig::new(
            RuntimeSourceConfig::Postgres(source_cfg),
            FileCheckpoint::new(checkpoint_dir.path()),
            InMemorySchemaHistory::default(),
        )
        .with_max_buffer_size(256)
        .with_max_poll_wait_ms(150),
    )?;

    resumed.start().await?;

    for id in 101_i64..=150_i64 {
        admin_client
            .execute(
                "INSERT INTO public.runtime_users (id, payload) VALUES ($1, $2)",
                &[&id, &format!("payload-{id}")],
            )
            .await
            .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;
    }

    let second_batch = poll_non_empty_batch(&mut resumed, 40).await?;
    assert!(second_batch.len() >= 50);

    resumed
        .commit_ack(
            second_batch
                .ack_token()
                .expect("non-empty batch should include ack token"),
        )
        .await?;
    let reader_after = FileCheckpoint::new(checkpoint_dir.path());
    assert!(reader_after.get_committed_count().await? >= 100);

    Ok(())
}

async fn poll_non_empty_batch(
    runtime: &mut CdcRuntime<FileCheckpoint, InMemorySchemaHistory>,
    rounds: usize,
) -> rustcdc::Result<rustcdc::EventBatch> {
    for _ in 0..rounds {
        let chunk = runtime.poll_event_batch().await?;
        if !chunk.is_empty() {
            return Ok(chunk);
        }
    }

    Err(rustcdc::Error::TimeoutError(
        "timed out waiting for a non-empty event batch".to_string(),
    ))
}

fn format_pg_lsn(lsn: u64) -> String {
    format!("{:X}/{:08X}", (lsn >> 32), (lsn & 0xFFFF_FFFF))
}
