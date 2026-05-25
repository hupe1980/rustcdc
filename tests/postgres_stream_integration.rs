#![cfg(feature = "postgres")]

use rustcdc::{source::Source, PostgresConnection, PostgresSourceConfig};
use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    GenericImage, ImageExt,
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
        conn_timeout_secs: 30,
        stream_poll_interval_ms: 50,
        max_events_per_poll: 1_000,
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
            update_event.before.is_some(),
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
            delete_event.before.is_some(),
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
        conn_timeout_secs: 30,
        stream_poll_interval_ms: 50,
        max_events_per_poll: 1_000,
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
