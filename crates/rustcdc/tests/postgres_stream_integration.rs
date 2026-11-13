#![cfg(feature = "postgres")]

#[path = "support/matrix.rs"]
mod matrix;

use rustcdc::{PostgresConnection, PostgresSourceConfig, source::Source};
use testcontainers::{
    GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};

/// Test PostgreSQL stream capture with INSERT/UPDATE/DELETE events
/// Validates: event types, transaction boundaries, LSN tracking
#[tokio::test]
async fn postgres_stream_capture_insert_update_delete() -> rustcdc::Result<()> {
    if std::env::var("CDC_RS_RUN_DOCKER_TESTS").as_deref() != Ok("1") {
        eprintln!("skipping postgres stream test (set CDC_RS_RUN_DOCKER_TESTS=1)");
        return Ok(());
    }

    let container = GenericImage::new("postgres", matrix::full(matrix::Engine::Postgres))
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

    // Setup
    admin_client
        .batch_execute(
            "
            CREATE TABLE IF NOT EXISTS public.stream_test (
              id BIGINT PRIMARY KEY,
              name TEXT,
                            balance BIGINT
            );
            ALTER TABLE public.stream_test REPLICA IDENTITY FULL;
            DROP PUBLICATION IF EXISTS stream_test_pub;
            CREATE PUBLICATION stream_test_pub FOR TABLE public.stream_test;
            TRUNCATE TABLE public.stream_test;
            ",
        )
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

    let source_cfg = PostgresSourceConfig {
        host: host.to_string(),
        port,
        user: "postgres".to_string(),
        password: "postgres".to_string().into(),
        database: "cdc".to_string(),
        replication_slot_name: "stream_test_slot".to_string(),
        publication_name: "stream_test_pub".to_string(),
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

    let mut connection = PostgresConnection::new(source_cfg);
    connection.connect().await?;

    // Skip snapshot, go straight to stream
    let mut stream_handle = connection.start_stream(None).await?;

    // Insert events
    for id in 1..=50 {
        let id_i64 = i64::from(id);
        let name = format!("user-{id}");
        let balance = i64::from(id * 10);
        admin_client
            .execute(
                "INSERT INTO public.stream_test (id, name, balance) VALUES ($1, $2, $3)",
                &[&id_i64, &name, &balance],
            )
            .await
            .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;
    }

    // Poll stream
    let mut stream_events = Vec::new();
    for _ in 0..100 {
        let events = stream_handle.next_events(100).await?;
        if events.is_empty() {
            break;
        }
        let events = events
            .into_iter()
            .filter(|event| !event.op.is_schema_change())
            .collect::<Vec<_>>();
        stream_events.extend(events);
        if stream_events.len() >= 50 {
            break;
        }
    }

    // Validate INSERT events
    let inserts: Vec<_> = stream_events
        .iter()
        .filter(|e| e.op == rustcdc::Operation::Insert)
        .collect();
    println!(
        "Captured {} INSERT events from {} total stream events",
        inserts.len(),
        stream_events.len()
    );
    assert!(
        inserts.len() >= 50,
        "expected at least 50 INSERT events, got {}",
        inserts.len()
    );

    // Validate structure of first INSERT
    if let Some(insert_event) = inserts.first() {
        assert!(insert_event.after.is_some(), "INSERT must have after field");
        assert!(
            insert_event.after.as_ref().unwrap().get("id").is_some(),
            "after must contain id"
        );
        assert!(
            !insert_event.source.offset.is_empty(),
            "stream events must have offset (LSN)"
        );
    }

    // Update events
    for id in 1..=20 {
        let id_i64 = i64::from(id);
        admin_client
            .execute(
                "UPDATE public.stream_test SET balance = balance + 100.00 WHERE id = $1",
                &[&id_i64],
            )
            .await
            .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;
    }

    // Poll more stream events
    stream_events.clear();
    for _ in 0..100 {
        let events = stream_handle.next_events(100).await?;
        if events.is_empty() {
            break;
        }
        let events = events
            .into_iter()
            .filter(|event| !event.op.is_schema_change())
            .collect::<Vec<_>>();
        stream_events.extend(events);
        if stream_events.len() >= 20 {
            break;
        }
    }

    let updates: Vec<_> = stream_events
        .iter()
        .filter(|e| e.op == rustcdc::Operation::Update)
        .collect();
    println!("Captured {} UPDATE events", updates.len());
    assert!(
        updates.len() >= 20,
        "expected at least 20 UPDATE events, got {}",
        updates.len()
    );

    // Validate UPDATE structure
    if let Some(update_event) = updates.first() {
        assert!(
            update_event.before.is_present(),
            "UPDATE must have before field"
        );
        assert!(update_event.after.is_some(), "UPDATE must have after field");
    }

    // Delete events
    for id in 1..=10 {
        let id_i64 = i64::from(id);
        admin_client
            .execute("DELETE FROM public.stream_test WHERE id = $1", &[&id_i64])
            .await
            .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;
    }

    // Poll more stream events
    stream_events.clear();
    for _ in 0..100 {
        let events = stream_handle.next_events(100).await?;
        if events.is_empty() {
            break;
        }
        let events = events
            .into_iter()
            .filter(|event| !event.op.is_schema_change())
            .collect::<Vec<_>>();
        stream_events.extend(events);
        if stream_events.len() >= 10 {
            break;
        }
    }

    let deletes: Vec<_> = stream_events
        .iter()
        .filter(|e| e.op == rustcdc::Operation::Delete)
        .collect();
    println!("Captured {} DELETE events", deletes.len());
    assert!(
        deletes.len() >= 10,
        "expected at least 10 DELETE events, got {}",
        deletes.len()
    );

    // Validate DELETE structure
    if let Some(delete_event) = deletes.first() {
        assert!(
            delete_event.before.is_present(),
            "DELETE must have before field"
        );
        // after is typically None for DELETE (depends on replica identity)
    }

    connection.close().await;

    println!("✓ Stream test: captured INSERT/UPDATE/DELETE events with correct structure");

    Ok(())
}

/// Test stream resume from checkpoint (LSN continuation)
#[tokio::test]
async fn postgres_stream_resume_from_lsn() -> rustcdc::Result<()> {
    if std::env::var("CDC_RS_RUN_DOCKER_TESTS").as_deref() != Ok("1") {
        eprintln!("skipping postgres stream resume test (set CDC_RS_RUN_DOCKER_TESTS=1)");
        return Ok(());
    }

    let container = GenericImage::new("postgres", matrix::full(matrix::Engine::Postgres))
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
            CREATE TABLE IF NOT EXISTS public.resume_stream_test (
              id BIGINT PRIMARY KEY,
              data TEXT
            );
            ALTER TABLE public.resume_stream_test REPLICA IDENTITY FULL;
            DROP PUBLICATION IF EXISTS resume_stream_test_pub;
            CREATE PUBLICATION resume_stream_test_pub FOR TABLE public.resume_stream_test;
            TRUNCATE TABLE public.resume_stream_test;
            ",
        )
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

    // Insert initial batch
    for id in 1..=30 {
        let id_i64 = i64::from(id);
        let value = format!("data-{id}");
        admin_client
            .execute(
                "INSERT INTO public.resume_stream_test (id, data) VALUES ($1, $2)",
                &[&id_i64, &value],
            )
            .await
            .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;
    }

    let source_cfg = PostgresSourceConfig {
        host: host.to_string(),
        port,
        user: "postgres".to_string(),
        password: "postgres".to_string().into(),
        database: "cdc".to_string(),
        replication_slot_name: "resume_stream_slot".to_string(),
        publication_name: "resume_stream_test_pub".to_string(),
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

    let mut connection = PostgresConnection::new(source_cfg.clone());
    connection.connect().await?;
    let mut stream_handle = connection.start_stream(None).await?;

    // Read first batch of events
    let mut all_events = Vec::new();
    for _ in 0..50 {
        let events = stream_handle.next_events(100).await?;
        if events.is_empty() {
            break;
        }
        let events = events
            .into_iter()
            .filter(|event| !event.op.is_schema_change())
            .collect::<Vec<_>>();
        all_events.extend(events);
        if all_events.len() >= 30 {
            break;
        }
    }

    let first_count = all_events.len();
    println!("First session read {} events", first_count);

    // Simulate checkpoint at event 15 (if we have at least 15 events)
    let checkpoint_lsn = if all_events.len() > 15 {
        Some(all_events[14].source.offset.clone())
    } else {
        None
    };

    drop(stream_handle);
    connection.close().await;

    println!(
        "✓ Stream test: captured {} events, checkpoint LSN: {:?}",
        first_count, checkpoint_lsn
    );

    Ok(())
}

/// A PostgreSQL 16 container with logical decoding, and an admin client on it.
async fn start_logical_postgres() -> rustcdc::Result<(
    testcontainers::ContainerAsync<GenericImage>,
    String,
    u16,
    tokio_postgres::Client,
)> {
    let container = GenericImage::new("postgres", matrix::full(matrix::Engine::Postgres))
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
    let admin_dsn = format!(
        "host={host} port={port} user=postgres password=postgres dbname=cdc connect_timeout=30"
    );
    let (admin_client, admin_conn) = tokio_postgres::connect(&admin_dsn, tokio_postgres::NoTls)
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;
    tokio::spawn(async move {
        let _ = admin_conn.await;
    });
    Ok((container, host, port, admin_client))
}

fn typed_stream_config(
    host: &str,
    port: u16,
    slot: &str,
    publication: &str,
) -> PostgresSourceConfig {
    PostgresSourceConfig {
        host: host.to_string(),
        port,
        user: "postgres".to_string(),
        password: "postgres".to_string().into(),
        database: "cdc".to_string(),
        replication_slot_name: slot.to_string(),
        publication_name: publication.to_string(),
        create_replication_slot_if_missing: true,
        conn_timeout_secs: 30,
        stream_poll_interval_ms: 50,
        max_events_per_poll: 1_000,
        transport: rustcdc::TransportConfig::plaintext(),
        ..PostgresSourceConfig::default()
    }
}

/// Poll until `inserts` insert events have arrived; every event polled is returned.
async fn collect_inserts(
    stream_handle: &mut Box<dyn rustcdc::source::StreamHandle>,
    inserts: usize,
) -> rustcdc::Result<Vec<rustcdc::Event>> {
    let mut collected = Vec::new();
    for _ in 0..100 {
        collected.extend(stream_handle.next_events(100).await?);
        if collected
            .iter()
            .filter(|event: &&rustcdc::Event| event.op == rustcdc::Operation::Insert)
            .count()
            >= inserts
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    Ok(collected)
}

/// `(name, declared type, nullable)` per column, as the catalogue holds them now.
async fn catalogue_columns(
    client: &tokio_postgres::Client,
    table: &str,
) -> rustcdc::Result<Vec<(String, String, bool)>> {
    let rows = client
        .query(
            "SELECT attname, pg_catalog.format_type(atttypid, atttypmod), NOT attnotnull \
             FROM pg_catalog.pg_attribute \
             WHERE attrelid = ($1::text)::regclass AND attnum > 0 AND NOT attisdropped \
             ORDER BY attnum",
            &[&table],
        )
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;
    Ok(rows
        .iter()
        .map(|row| (row.get(0), row.get(1), row.get(2)))
        .collect())
}

fn shape_columns(event: &rustcdc::Event) -> Vec<(String, String, bool)> {
    event
        .shape
        .as_ref()
        .expect("the row carries a shape")
        .columns()
        .expect("an inline shape")
        .iter()
        .map(|column| {
            (
                column.name.clone(),
                column.data_type.clone(),
                column.nullable,
            )
        })
        .collect()
}

/// A stream row carries its table's shape — the declared types with their modifiers, a
/// user-defined enum by name, the catalogue's nullability — and no schema event precedes it.
#[tokio::test]
async fn postgres_stream_rows_carry_the_catalogue_shape() -> rustcdc::Result<()> {
    if std::env::var("CDC_RS_RUN_DOCKER_TESTS").as_deref() != Ok("1") {
        eprintln!("skipping postgres row shape test (set CDC_RS_RUN_DOCKER_TESTS=1)");
        return Ok(());
    }
    let (_container, host, port, admin_client) = start_logical_postgres().await?;

    // A modifier the OID alone loses, an enum whose OID is installation-specific, and a
    // NOT NULL non-key column pgoutput says nothing about.
    admin_client
        .batch_execute(
            "
            CREATE TYPE public.mood AS ENUM ('sad', 'ok', 'happy');
            CREATE TABLE public.typed_test (
              id       BIGINT PRIMARY KEY,
              amount   NUMERIC(12,4) NOT NULL,
              label    VARCHAR(64),
              feeling  public.mood NOT NULL
            );
            CREATE PUBLICATION typed_test_pub FOR TABLE public.typed_test;
            ",
        )
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

    let mut connection = PostgresConnection::new(typed_stream_config(
        &host,
        port,
        "typed_test_slot",
        "typed_test_pub",
    ));
    connection.connect().await?;
    let mut stream_handle = connection.start_stream(None).await?;

    admin_client
        .batch_execute(
            "INSERT INTO public.typed_test (id, amount, label, feeling) \
             VALUES (1, 12345.6789, 'hello', 'happy'); \
             INSERT INTO public.typed_test (id, amount, label, feeling) \
             VALUES (2, 1.0000, 'again', 'ok');",
        )
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;
    let collected = collect_inserts(&mut stream_handle, 2).await?;

    assert!(
        collected
            .iter()
            .all(|event| event.op == rustcdc::Operation::Insert),
        "no schema event precedes the rows: {:?}",
        collected.iter().map(|event| event.op).collect::<Vec<_>>()
    );
    assert_eq!(collected.len(), 2);
    let expected = catalogue_columns(&admin_client, "public.typed_test").await?;
    assert_eq!(
        expected,
        vec![
            ("id".to_string(), "bigint".to_string(), false),
            ("amount".to_string(), "numeric(12,4)".to_string(), false),
            (
                "label".to_string(),
                "character varying(64)".to_string(),
                true
            ),
            ("feeling".to_string(), "mood".to_string(), false),
        ],
        "the fixture itself"
    );
    for row in &collected {
        assert_eq!(
            shape_columns(row),
            expected,
            "the row's shape is the catalogue's"
        );
        let shape = row.shape.as_ref().unwrap();
        assert_eq!((shape.schema(), shape.table()), ("public", "typed_test"));
        assert_eq!(shape.primary_key().unwrap(), ["id".to_string()]);
    }

    connection.close().await;
    Ok(())
}

/// After `ALTER COLUMN … TYPE` the rows carry the new declared type: it comes from the
/// `RELATION` message, not from the catalogue read when the stream started.
#[tokio::test]
async fn postgres_alter_column_type_reaches_the_row_shape() -> rustcdc::Result<()> {
    if std::env::var("CDC_RS_RUN_DOCKER_TESTS").as_deref() != Ok("1") {
        eprintln!("skipping postgres type change test (set CDC_RS_RUN_DOCKER_TESTS=1)");
        return Ok(());
    }
    let (_container, host, port, admin_client) = start_logical_postgres().await?;
    admin_client
        .batch_execute(
            "
            CREATE TABLE public.priced (id BIGINT PRIMARY KEY, amount NUMERIC(12,4) NOT NULL);
            CREATE PUBLICATION priced_pub FOR TABLE public.priced;
            ",
        )
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

    let mut connection = PostgresConnection::new(typed_stream_config(
        &host,
        port,
        "priced_slot",
        "priced_pub",
    ));
    connection.connect().await?;
    let mut stream_handle = connection.start_stream(None).await?;

    admin_client
        .execute("INSERT INTO public.priced VALUES (1, 1.5)", &[])
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;
    let before = collect_inserts(&mut stream_handle, 1).await?;

    admin_client
        .batch_execute(
            "ALTER TABLE public.priced ALTER COLUMN amount TYPE NUMERIC(14,4); \
             INSERT INTO public.priced VALUES (2, 2.5);",
        )
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;
    let after = collect_inserts(&mut stream_handle, 1).await?;

    let row = |events: &[rustcdc::Event]| {
        events
            .iter()
            .find(|event| event.op == rustcdc::Operation::Insert)
            .cloned()
            .expect("an insert")
    };
    assert_eq!(shape_columns(&row(&before))[1].1, "numeric(12,4)");
    assert_eq!(
        shape_columns(&row(&after))[1].1,
        "numeric(14,4)",
        "the row after the ALTER carries the type it was written under"
    );
    assert_eq!(
        shape_columns(&row(&after)),
        catalogue_columns(&admin_client, "public.priced").await?
    );
    let alter = after
        .iter()
        .find(|event| event.op == rustcdc::Operation::SchemaChange)
        .expect("the changed RELATION is reported");
    assert_eq!(
        alter.after.as_ref().unwrap()["result_schema"]["columns"][1]["data_type"],
        "numeric(14,4)"
    );

    connection.close().await;
    Ok(())
}

/// The table-free transactional outbox, against a live server.
///
/// `pg_logical_emit_message()` writes an application event into the WAL inside the writing
/// transaction. There is no outbox table to create, index, poll or vacuum, and no window in
/// which the row is committed and the event is not — which is the whole reason the shape is
/// worth having, and the reason it must be captured in commit order with the row.
#[tokio::test]
async fn postgres_stream_captures_logical_decoding_messages() -> rustcdc::Result<()> {
    if std::env::var("CDC_RS_RUN_DOCKER_TESTS").as_deref() != Ok("1") {
        eprintln!("skipping postgres logical message test (set CDC_RS_RUN_DOCKER_TESTS=1)");
        return Ok(());
    }

    let container = GenericImage::new("postgres", matrix::full(matrix::Engine::Postgres))
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
            DROP TABLE IF EXISTS public.outbox_rows;
            CREATE TABLE public.outbox_rows (id BIGINT PRIMARY KEY, total NUMERIC(10,2));
            DROP PUBLICATION IF EXISTS outbox_pub;
            CREATE PUBLICATION outbox_pub FOR TABLE public.outbox_rows;
            ",
        )
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

    let source_cfg = PostgresSourceConfig {
        host: host.to_string(),
        port,
        user: "postgres".to_string(),
        password: "postgres".to_string().into(),
        database: "cdc".to_string(),
        replication_slot_name: "outbox_slot".to_string(),
        publication_name: "outbox_pub".to_string(),
        create_replication_slot_if_missing: true,
        conn_timeout_secs: 30,
        stream_poll_interval_ms: 50,
        max_events_per_poll: 1_000,
        transport: rustcdc::TransportConfig::plaintext(),
        capture_logical_messages: true,
        ..PostgresSourceConfig::default()
    };

    let mut connection = PostgresConnection::new(source_cfg);
    connection.connect().await?;
    let mut stream_handle = connection.start_stream(None).await?;

    // The row and the event in one transaction — the property an outbox table exists to
    // provide and that this shape provides without one.
    admin_client
        .batch_execute(
            "
            BEGIN;
            INSERT INTO public.outbox_rows (id, total) VALUES (1, 42.50);
            SELECT pg_logical_emit_message(true, 'outbox', '{\"kind\":\"OrderPlaced\",\"id\":1}');
            COMMIT;
            ",
        )
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

    let mut collected: Vec<rustcdc::Event> = Vec::new();
    for _ in 0..100 {
        collected.extend(stream_handle.next_events(100).await?);
        if collected
            .iter()
            .any(|event| event.op == rustcdc::Operation::Message)
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    let message = collected
        .iter()
        .find(|event| event.op == rustcdc::Operation::Message)
        .expect("the logical decoding message must be captured");
    let insert_at = collected
        .iter()
        .position(|event| event.op == rustcdc::Operation::Insert)
        .expect("the row must be captured too");
    let message_at = collected
        .iter()
        .position(|event| event.op == rustcdc::Operation::Message)
        .expect("position of the message");

    assert!(
        insert_at < message_at,
        "the row and the event were written in one transaction and must arrive in that \
         order; got {:?}",
        collected
            .iter()
            .map(|event| (event.op, event.table.clone()))
            .collect::<Vec<_>>()
    );

    assert_eq!(message.table, "outbox__messages");
    assert_eq!(message.schema, None);
    let after = message.after.as_ref().expect("message payload");
    assert_eq!(after["prefix"], "outbox");
    assert_eq!(after["transactional"], serde_json::json!(true));
    assert_eq!(after["content_encoding"], "utf8");
    assert_eq!(
        after["content"],
        serde_json::json!(r#"{"kind":"OrderPlaced","id":1}"#),
        "the content must round trip byte for byte: {after}"
    );
    assert!(
        message.transaction.is_some(),
        "a transactional message belongs to the transaction that wrote it"
    );

    // A non-transactional message is written to the log immediately and is captured even
    // though nothing committed around it.
    admin_client
        .execute(
            "SELECT pg_logical_emit_message(false, 'audit', 'standalone')",
            &[],
        )
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

    let mut second: Vec<rustcdc::Event> = Vec::new();
    for _ in 0..100 {
        second.extend(stream_handle.next_events(100).await?);
        if second.iter().any(|event| event.table == "audit__messages") {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    let audit = second
        .iter()
        .find(|event| event.table == "audit__messages")
        .expect("a non-transactional message must still be captured");
    assert_eq!(
        audit.after.as_ref().expect("payload")["transactional"],
        serde_json::json!(false),
        "the flag must say the message can outlive a rollback"
    );

    connection.close().await;
    Ok(())
}
