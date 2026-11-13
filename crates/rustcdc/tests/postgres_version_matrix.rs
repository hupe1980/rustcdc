#![cfg(feature = "postgres")]

#[path = "support/matrix.rs"]
mod matrix;

use rustcdc::TransportConfig;
use rustcdc::{PostgresConnection, PostgresSourceConfig, source::Source};
use testcontainers::{
    GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};
use tokio_postgres::NoTls;

fn skip_postgres_version_matrix_case() -> bool {
    if std::env::var("CDC_RS_RUN_DOCKER_TESTS").as_deref() == Ok("1") {
        return false;
    }

    eprintln!("skipping postgres version matrix test (set CDC_RS_RUN_DOCKER_TESTS=1)");
    true
}

async fn run_postgres_version_connection_test(
    image_tag: &str,
    slot_name: &str,
    publication_name: &str,
    table_name: &str,
) -> rustcdc::Result<()> {
    let container = GenericImage::new("postgres", image_tag)
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
            // The minimum, so a few thousand rows make the server stream a transaction.
            "-c",
            "logical_decoding_work_mem=64kB",
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
    let (admin_client, admin_conn) = tokio_postgres::connect(&admin_dsn, NoTls)
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;
    tokio::spawn(async move {
        let _ = admin_conn.await;
    });

    let setup_sql = format!(
        "
        CREATE TABLE IF NOT EXISTS public.{table_name} (
          id BIGINT PRIMARY KEY,
          value TEXT
        );
        ALTER TABLE public.{table_name} REPLICA IDENTITY FULL;
        DROP PUBLICATION IF EXISTS {publication_name};
        CREATE PUBLICATION {publication_name} FOR TABLE public.{table_name};
        "
    );
    admin_client
        .batch_execute(&setup_sql)
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

    let config = PostgresSourceConfig {
        host: host.to_string(),
        port,
        user: "postgres".to_string(),
        password: "postgres".to_string().into(),
        database: "cdc".to_string(),
        replication_slot_name: slot_name.to_string(),
        publication_name: publication_name.to_string(),
        // Ephemeral test container: the slot legitimately does not exist yet.
        create_replication_slot_if_missing: true,
        conn_timeout_secs: 30,
        stream_poll_interval_ms: 50,
        max_events_per_poll: 1_000,
        // This matrix checks connector behaviour across server *versions*; the containers run
        // with `ssl = off` and present no certificate. It previously said `tls()` and connected
        // anyway, because `sslmode=prefer` silently fell back to plaintext — so a line that read
        // as TLS coverage was providing none. `sslmode=require` is now set whenever the transport
        // is TLS, which makes that impossible; TLS itself is covered by the connection suites and
        // by `wire::tests`.
        transport: TransportConfig::plaintext(),
        ..Default::default()
    };

    let mut connection = PostgresConnection::new(config);
    connection.connect().await?;
    assert_eq!(connection.source_type(), "postgres");

    // Every supported release streams a large transaction before its commit (pgoutput v2),
    // and rustcdc delivers it whole.
    let mut stream = connection.start_stream(None).await?;
    admin_client
        .batch_execute(&format!(
            "INSERT INTO public.{table_name} SELECT g, repeat('v', 200) FROM generate_series(1, 2000) g"
        ))
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    let mut rows = 0;
    while rows < 2000 && std::time::Instant::now() < deadline {
        rows += stream
            .next_events(500)
            .await?
            .iter()
            .filter(|event| !event.op.is_schema_change())
            .count();
    }
    assert_eq!(
        rows, 2000,
        "{image_tag}: the streamed transaction arrived incomplete"
    );
    let streamed: i64 = admin_client
        .query_one(
            "SELECT stream_txns FROM pg_stat_replication_slots WHERE slot_name = $1",
            &[&slot_name],
        )
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?
        .get(0);
    assert!(
        streamed > 0,
        "{image_tag}: the server did not stream the transaction"
    );
    drop(stream);
    connection.close().await;

    Ok(())
}

/// Connect, stream a large transaction, and close on every tested PostgreSQL version. A tag that no longer pulls
/// fails: a retired image is the drift this matrix exists to catch.
#[tokio::test]
async fn postgres_connection_every_tested_version() -> rustcdc::Result<()> {
    if skip_postgres_version_matrix_case() {
        return Ok(());
    }
    for tag in matrix::tested(matrix::Engine::Postgres) {
        let id = tag.split('-').next().unwrap_or(tag).replace('.', "_");
        run_postgres_version_connection_test(
            tag,
            &format!("pg{id}_test_slot"),
            &format!("pg{id}_test_pub"),
            &format!("pg{id}_test_table"),
        )
        .await
        .map_err(|error| rustcdc::Error::SourceError(format!("postgres {tag}: {error}")))?;
        println!("✓ PostgreSQL {tag} compatibility verified");
    }
    Ok(())
}
