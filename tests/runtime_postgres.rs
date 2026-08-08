#![cfg(feature = "postgres")]

use rustcdc::{
    checkpoint::{Checkpoint, FileCheckpoint, PostgresOffset},
    schema_history::InMemorySchemaHistory,
    AckMode, CdcRuntime, PostgresSourceConfig, RuntimeConfig, RuntimeSourceConfig,
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
        .map_err(|error| rustcdc::Error::SourceError(rustcdc::render_error_chain(&error)))?;

    let host = container
        .get_host()
        .await
        .map_err(|error| rustcdc::Error::SourceError(rustcdc::render_error_chain(&error)))?;
    let port = container
        .get_host_port_ipv4(5432.tcp())
        .await
        .map_err(|error| rustcdc::Error::SourceError(rustcdc::render_error_chain(&error)))?;

    let admin_dsn = format!(
        "host={host} port={port} user=postgres password=postgres dbname=cdc connect_timeout=30"
    );
    let (admin_client, admin_conn) = tokio_postgres::connect(&admin_dsn, tokio_postgres::NoTls)
        .await
        .map_err(|error| rustcdc::Error::SourceError(rustcdc::render_error_chain(&error)))?;
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
            DROP PUBLICATION IF EXISTS rustcdc_runtime_pub;
            CREATE PUBLICATION rustcdc_runtime_pub FOR TABLE public.runtime_users;
            ",
        )
        .await
        .map_err(|error| rustcdc::Error::SourceError(rustcdc::render_error_chain(&error)))?;

    let checkpoint_dir = tempfile::tempdir().map_err(rustcdc::Error::IoError)?;

    let source_cfg = PostgresSourceConfig {
        host: host.to_string(),
        port,
        user: "postgres".to_string(),
        password: "postgres".into(),
        database: "cdc".to_string(),
        replication_slot_name: "rustcdc_runtime_slot".to_string(),
        publication_name: "rustcdc_runtime_pub".to_string(),
        // Ephemeral test container: the slot legitimately does not exist yet.
        create_replication_slot_if_missing: true,
        conn_timeout_secs: 30,
        stream_poll_interval_ms: 50,
        max_events_per_poll: 1_000,
        // The test container runs with `ssl = off`, so the transport must say so.
        // Left at the default (TLS), `build_connect_config` now sets `sslmode=require`
        // and the connection is refused rather than silently downgraded — which is the
        // point of that change, and the reason this line has to be explicit.
        transport: rustcdc::TransportConfig::plaintext(),
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
        .map_err(|error| rustcdc::Error::SourceError(rustcdc::render_error_chain(&error)))?;

    for id in 1_i64..=100_i64 {
        admin_client
            .execute(
                "INSERT INTO public.runtime_users (id, payload) VALUES ($1, $2)",
                &[&id, &format!("payload-{id}")],
            )
            .await
            .map_err(|error| rustcdc::Error::SourceError(rustcdc::render_error_chain(&error)))?;
    }

    // The property under test is that a **partial** acknowledgement advances the durable
    // position exactly as far as the consumer accepted — not that one poll happens to deliver
    // all 100 rows. Batches are cut on `max_buffer_size`, `max_event_bytes` and free
    // commit-barrier capacity, and a re-poll before acknowledgement redelivers the *same*
    // in-flight batch rather than a larger one, so the split point has to come from what was
    // actually delivered.
    let first_batch = poll_non_empty_batch(&mut runtime, 40).await?;
    let delivered = first_batch.len();
    assert!(
        delivered >= 2,
        "a partial acknowledgement needs at least two delivered events; got {delivered} \
         after writing 100 rows"
    );
    let accepted_count = delivered / 2;

    let AckMode::Required(token) = first_batch.ack_mode() else {
        panic!("non-empty batch should include ack token");
    };
    let (accepted, _remaining) = token.split_at(accepted_count)?;
    runtime.commit_ack(accepted).await?;

    let reader = FileCheckpoint::read_only(checkpoint_dir.path());
    assert_eq!(reader.get_committed_count().await?, accepted_count as u64);
    let saved = reader
        .load()
        .await?
        .ok_or_else(|| rustcdc::Error::StateError("checkpoint should exist after commit".into()))?;
    let saved_offset = PostgresOffset::from_bytes(&saved.encode()?)?;
    let target_lsn = format_pg_lsn(saved_offset.lsn);

    // Release the runtime **before** touching the slot out of band. Under
    // `WalTransport::StreamingReplication` — the default — a walsender holds the slot for
    // the whole life of the stream, and PostgreSQL refuses
    // `pg_replication_slot_advance` on an active slot ("replication slot is active for PID
    // N"). Under the older SQL-peek transport nothing held it, so this ordering did not
    // matter; it does now, and the same constraint applies to any operator script that
    // advances or drops a slot a pipeline is reading.
    drop(runtime);

    let advance_sql = format!(
        "SELECT end_lsn::text FROM pg_replication_slot_advance('rustcdc_runtime_slot', '{target_lsn}'::pg_lsn)"
    );
    // Closing the socket and the server reaping the walsender are not synchronous, so the
    // first attempt can still land while the slot is marked active.
    let mut advanced = None;
    for _ in 0..40 {
        match admin_client.query_one(&advance_sql, &[]).await {
            Ok(row) => {
                advanced = Some(row);
                break;
            }
            Err(error) => {
                if !error.to_string().contains("is active") {
                    return Err(rustcdc::Error::SourceError(rustcdc::render_error_chain(
                        &error,
                    )));
                }
                tokio::time::sleep(std::time::Duration::from_millis(250)).await;
            }
        }
    }
    advanced.ok_or_else(|| {
        rustcdc::Error::StateError(
            "the replication slot stayed active after the runtime was dropped".into(),
        )
    })?;

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
            .map_err(|error| rustcdc::Error::SourceError(rustcdc::render_error_chain(&error)))?;
    }

    // Drain across batches rather than expecting one poll to carry everything: the resumed
    // stream owes the 50 events left unacknowledged above plus the 50 written since, and how
    // those are grouped is not part of the contract.
    let mut acknowledged_after_resume = 0usize;
    for _ in 0..40 {
        let batch = resumed.poll_event_batch().await?;
        if batch.is_empty() {
            continue;
        }
        acknowledged_after_resume += batch.len();
        resumed.commit_ack(batch.ack_mode()).await?;
        if acknowledged_after_resume >= 100 - accepted_count {
            break;
        }
    }

    let reader_after = FileCheckpoint::read_only(checkpoint_dir.path());
    assert!(
        reader_after.get_committed_count().await? >= 100,
        "the resume must redeliver everything left unacknowledged and then the new writes; \
         durable count is {}",
        reader_after.get_committed_count().await?
    );

    Ok(())
}

async fn poll_non_empty_batch(
    runtime: &mut CdcRuntime,
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
