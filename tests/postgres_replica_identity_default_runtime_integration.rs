//! Runtime-level coverage for `REPLICA IDENTITY DEFAULT`.
//!
//! Every other PostgreSQL runtime test forces `REPLICA IDENTITY FULL` in its fixture, so
//! `old_tuple` is always `Some` and the runtime never sees the shape a stock PostgreSQL
//! table actually produces: an UPDATE that does not touch the key sends neither an `O` nor
//! a `K` old tuple, so the event legitimately carries `before: None`.
//!
//! Source-level tests cannot cover this — `Event::validate_or_error` runs at runtime
//! ingress, not in the source — which is why this test drives a `CdcRuntime`.
#![cfg(feature = "postgres")]

use rustcdc::{
    checkpoint::FileCheckpoint, schema_history::InMemorySchemaHistory, CdcRuntime, Operation,
    PostgresSourceConfig, RuntimeConfig, RuntimeSourceConfig,
};
use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    GenericImage, ImageExt,
};

#[tokio::test]
async fn runtime_accepts_update_without_before_image_under_replica_identity_default(
) -> rustcdc::Result<()> {
    if std::env::var("CDC_RS_RUN_DOCKER_TESTS").as_deref() != Ok("1") {
        eprintln!(
            "skipping postgres replica-identity runtime test (set CDC_RS_RUN_DOCKER_TESTS=1)"
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
    let (admin, admin_conn) = tokio_postgres::connect(&admin_dsn, tokio_postgres::NoTls)
        .await
        .map_err(|error| rustcdc::Error::SourceError(rustcdc::render_error_chain(&error)))?;
    tokio::spawn(async move {
        let _ = admin_conn.await;
    });

    admin
        .batch_execute(
            "
            CREATE TABLE public.ri_default_runtime (
              id     BIGINT PRIMARY KEY,
              status TEXT NOT NULL
            );
            -- Deliberately NOT `REPLICA IDENTITY FULL`: DEFAULT is the stock configuration.
            DROP PUBLICATION IF EXISTS ri_default_runtime_pub;
            CREATE PUBLICATION ri_default_runtime_pub FOR TABLE public.ri_default_runtime;
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
        replication_slot_name: "ri_default_runtime_slot".to_string(),
        publication_name: "ri_default_runtime_pub".to_string(),
        create_replication_slot_if_missing: true,
        conn_timeout_secs: 30,
        stream_poll_interval_ms: 50,
        max_events_per_poll: 1_000,
        transport: rustcdc::TransportConfig::plaintext(),
        ..PostgresSourceConfig::default()
    };

    let mut runtime = CdcRuntime::new(
        RuntimeConfig::new(
            RuntimeSourceConfig::Postgres(source_cfg),
            FileCheckpoint::new(checkpoint_dir.path()),
            InMemorySchemaHistory::default(),
        )
        .with_max_buffer_size(256)
        .with_max_poll_wait_ms(150),
    )?;

    runtime.start().await?;

    admin
        .batch_execute(
            "
            INSERT INTO public.ri_default_runtime VALUES (1, 'a');
            -- The key is untouched, so pgoutput emits an `N`-only UPDATE: no before-image.
            UPDATE public.ri_default_runtime SET status = 'b' WHERE id = 1;
            ",
        )
        .await
        .map_err(|error| rustcdc::Error::SourceError(rustcdc::render_error_chain(&error)))?;

    let mut events = Vec::new();
    for _ in 0..60 {
        // The failure mode under test is a hard `Err` out of `poll_event_batch`, so the
        // error is surfaced rather than retried.
        let batch = runtime.poll_event_batch().await?;
        if !batch.is_empty() {
            events.extend(batch.events().iter().cloned());
            runtime.commit_ack(batch.ack_mode()).await?;
        }
        if events.iter().any(|e| e.op == Operation::Update) {
            break;
        }
    }

    let update = events
        .iter()
        .find(|event| event.op == Operation::Update)
        .expect("the runtime must deliver the update event");

    assert!(
        update.before.is_unavailable(),
        "an update that leaves the key untouched has no before-image under DEFAULT: {:?}",
        update.before
    );
    assert!(
        !update.before.is_key_only(),
        "before_is_key_only must stay false when there is no before-image at all"
    );
    assert!(
        !update.has_full_before(),
        "an absent before-image is never a full row"
    );
    assert_eq!(
        update
            .after
            .as_ref()
            .and_then(|a| a.get("status"))
            .and_then(|v| v.as_str()),
        Some("b"),
        "the after-image is always complete"
    );

    runtime.stop().await?;
    Ok(())
}
