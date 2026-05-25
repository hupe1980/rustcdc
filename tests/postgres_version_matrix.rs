#![cfg(feature = "postgres")]

use rustcdc::{source::Source, PostgresConnection, PostgresSourceConfig};
use rustcdc::TransportConfig;
use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    GenericImage, ImageExt,
};
use tokio_postgres::NoTls;

fn skip_postgres_version_matrix_case() -> bool {
    if std::env::var("CDC_RS_RUN_DOCKER_TESTS").as_deref() == Ok("1") {
        return false;
    }

    eprintln!("skipping postgres version matrix test (set CDC_RS_RUN_DOCKER_TESTS=1)");
    true
}

macro_rules! postgres_version_test {
    ($name:ident, $image_tag:literal, $slot_name:literal, $publication_name:literal, $table_name:literal, $label:literal) => {
        #[tokio::test]
        async fn $name() -> rustcdc::Result<()> {
            if skip_postgres_version_matrix_case() {
                return Ok(());
            }

            run_postgres_version_connection_test(
                $image_tag,
                $slot_name,
                $publication_name,
                $table_name,
            )
            .await?;

            println!($label);
            Ok(())
        }
    };
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
        conn_timeout_secs: 30,
        stream_poll_interval_ms: 50,
        max_events_per_poll: 1_000,
        transport: TransportConfig::tls(),
        ..Default::default()
    };

    let connection = PostgresConnection::new(config);
    connection.connect().await?;
    assert_eq!(connection.source_type(), "postgres");
    connection.close().await;

    Ok(())
}

postgres_version_test!(
    postgres_version_12_connection,
    "12-alpine",
    "pg12_test_slot",
    "pg12_test_pub",
    "pg12_test_table",
    "✓ PostgreSQL 12 compatibility verified"
);

postgres_version_test!(
    postgres_version_14_connection,
    "14-alpine",
    "pg14_test_slot",
    "pg14_test_pub",
    "pg14_test_table",
    "✓ PostgreSQL 14 compatibility verified"
);

postgres_version_test!(
    postgres_version_15_connection,
    "15-alpine",
    "pg15_test_slot",
    "pg15_test_pub",
    "pg15_test_table",
    "✓ PostgreSQL 15 compatibility verified"
);

postgres_version_test!(
    postgres_version_16_connection,
    "16-alpine",
    "pg16_test_slot",
    "pg16_test_pub",
    "pg16_test_table",
    "✓ PostgreSQL 16 compatibility verified"
);
