#![cfg(feature = "postgres")]

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

/// Every table's declared column types reach the stream before that table's first row.
///
/// Column values are text on every capture path, so a consumer needs the types to decode
/// them — and before this, a table that never underwent DDL had no type information
/// anywhere in the stream. pgoutput sends `RELATION` immediately before a table's first
/// row and the connector emitted nothing for it, because the schema event fired only when
/// a relation *changed*.
///
/// This asserts the whole claim against a live server: the announcement arrives first, it
/// carries the real declared types with their modifiers, and it carries the catalogue's
/// nullability rather than one inferred from the primary key.
#[tokio::test]
async fn postgres_stream_announces_declared_types_before_the_first_row() -> rustcdc::Result<()> {
    if std::env::var("CDC_RS_RUN_DOCKER_TESTS").as_deref() != Ok("1") {
        eprintln!("skipping postgres schema announcement test (set CDC_RS_RUN_DOCKER_TESTS=1)");
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

    // A schema chosen to break the old OID map in every way it could break:
    // a modifier it dropped, a user-defined enum whose OID it could not know, and a
    // NOT NULL non-key column whose nullability it inferred from the primary key.
    admin_client
        .batch_execute(
            "
            DROP TABLE IF EXISTS public.typed_test;
            DROP TYPE IF EXISTS public.mood;
            CREATE TYPE public.mood AS ENUM ('sad', 'ok', 'happy');
            CREATE TABLE public.typed_test (
              id       BIGINT PRIMARY KEY,
              amount   NUMERIC(12,4) NOT NULL,
              label    VARCHAR(64),
              feeling  public.mood NOT NULL
            );
            DROP PUBLICATION IF EXISTS typed_test_pub;
            CREATE PUBLICATION typed_test_pub FOR TABLE public.typed_test;
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
        replication_slot_name: "typed_test_slot".to_string(),
        publication_name: "typed_test_pub".to_string(),
        create_replication_slot_if_missing: true,
        conn_timeout_secs: 30,
        stream_poll_interval_ms: 50,
        max_events_per_poll: 1_000,
        transport: rustcdc::TransportConfig::plaintext(),
        ..PostgresSourceConfig::default()
    };

    let mut connection = PostgresConnection::new(source_cfg);
    connection.connect().await?;
    let mut stream_handle = connection.start_stream(None).await?;

    admin_client
        .execute(
            "INSERT INTO public.typed_test (id, amount, label, feeling) \
             VALUES (1, 12345.6789, 'hello', 'happy')",
            &[],
        )
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

    let mut collected = Vec::new();
    for _ in 0..100 {
        let events = stream_handle.next_events(100).await?;
        collected.extend(events);
        if collected
            .iter()
            .any(|event: &rustcdc::Event| event.op == rustcdc::Operation::Insert)
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    let insert_at = collected
        .iter()
        .position(|event| event.op == rustcdc::Operation::Insert)
        .expect("the insert must arrive");
    let schema_at = collected
        .iter()
        .position(|event| event.op == rustcdc::Operation::SchemaChange)
        .expect("the table's schema must be announced");

    assert!(
        schema_at < insert_at,
        "the schema must precede the first row it describes; got {:?}",
        collected
            .iter()
            .map(|event| (event.op, event.table.clone()))
            .collect::<Vec<_>>()
    );

    let after = collected[schema_at]
        .after
        .as_ref()
        .expect("schema event payload");
    assert_eq!(
        after["ddl_type"], "READ_SCHEMA",
        "a first sighting is an observation, not a change"
    );
    assert_eq!(collected[schema_at].table, "typed_test__ddl_events");

    let column = |name: &str| -> serde_json::Value {
        after["result_schema"]["columns"]
            .as_array()
            .expect("columns array")
            .iter()
            .find(|column| column["name"] == name)
            .cloned()
            .unwrap_or_else(|| panic!("column '{name}' missing from {after}"))
    };

    assert_eq!(
        column("amount")["data_type"],
        serde_json::json!("numeric(12,4)"),
        "the type modifier must survive; the pgoutput OID alone reports 'numeric'"
    );
    assert_eq!(
        column("label")["data_type"],
        serde_json::json!("character varying(64)"),
        "format_type reports PostgreSQL's own spelling"
    );
    assert_eq!(
        column("feeling")["data_type"],
        serde_json::json!("mood"),
        "a user-defined enum has an installation-specific OID, which the built-in map \
         could only render as pg_type_oid:<N>"
    );
    assert_eq!(
        column("amount")["nullable"],
        serde_json::json!(false),
        "a NOT NULL non-key column must not be published as nullable — pgoutput carries no \
         nullability, so this can only come from the catalogue"
    );
    assert_eq!(column("label")["nullable"], serde_json::json!(true));
    assert_eq!(column("id")["nullable"], serde_json::json!(false));
    assert_eq!(
        column("id")["constraints"],
        serde_json::json!(["primary_key"])
    );
    assert_eq!(
        after["result_schema"]["primary_keys"],
        serde_json::json!(["id"])
    );

    // The announcement is once per table per run, not once per row.
    admin_client
        .execute(
            "INSERT INTO public.typed_test (id, amount, label, feeling) \
             VALUES (2, 1.0000, 'again', 'ok')",
            &[],
        )
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

    let mut second = Vec::new();
    for _ in 0..100 {
        let events = stream_handle.next_events(100).await?;
        second.extend(events);
        if second
            .iter()
            .any(|event: &rustcdc::Event| event.op == rustcdc::Operation::Insert)
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert!(
        second
            .iter()
            .all(|event| event.op != rustcdc::Operation::SchemaChange),
        "an unchanged table must not be re-announced within a run"
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
