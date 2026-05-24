#![cfg(feature = "postgres")]

use cdc_rs::{
    checkpoint::{Checkpoint, FileCheckpoint},
    source::Source,
    PostgresConnection, PostgresSourceConfig,
};
use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    GenericImage, ImageExt,
};
use tokio::time::{sleep, Duration};

/// Test large-table snapshot chunking (100K rows → 10K chunks)
/// Validates: chunking behavior, checkpoint persistence, and resumable snapshot handling
#[tokio::test]
async fn postgres_snapshot_large_table_chunked() -> cdc_rs::Result<()> {
    if std::env::var("CDC_RS_RUN_DOCKER_TESTS").as_deref() != Ok("1") {
        eprintln!("skipping postgres snapshot integration test (set CDC_RS_RUN_DOCKER_TESTS=1)");
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
        .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;

    let host = container
        .get_host()
        .await
        .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;
    let port = container
        .get_host_port_ipv4(5432.tcp())
        .await
        .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;

    let admin_dsn = format!(
        "host={host} port={port} user=postgres password=postgres dbname=cdc connect_timeout=30"
    );
    let (admin_client, admin_conn) = tokio_postgres::connect(&admin_dsn, tokio_postgres::NoTls)
        .await
        .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;
    tokio::spawn(async move {
        let _ = admin_conn.await;
    });

    // Setup large test table
    admin_client
        .batch_execute(
            "
            CREATE TABLE IF NOT EXISTS public.large_snapshot_test (
              id BIGINT PRIMARY KEY,
              payload TEXT NOT NULL
            );
                        ALTER TABLE public.large_snapshot_test REPLICA IDENTITY FULL;
                        DROP PUBLICATION IF EXISTS snapshot_test_pub;
                        CREATE PUBLICATION snapshot_test_pub FOR TABLE public.large_snapshot_test;
            TRUNCATE TABLE public.large_snapshot_test;
            ",
        )
        .await
        .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;

    // Insert 100K rows
    for batch_start in (1..=100_000).step_by(1000) {
        let batch_end = (batch_start + 999).min(100_000);
        let mut values = Vec::new();
        for id in batch_start..=batch_end {
            values.push(format!("({}, 'payload-{}')", id, id));
        }
        let sql = format!(
            "INSERT INTO public.large_snapshot_test (id, payload) VALUES {}",
            values.join(", ")
        );
        admin_client
            .batch_execute(&sql)
            .await
            .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;
    }

    // Verify insert count
    let count: i64 = admin_client
        .query_one("SELECT COUNT(*) FROM public.large_snapshot_test", &[])
        .await
        .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?
        .get(0);
    assert_eq!(count, 100_000, "expected 100K rows inserted");

    let source_cfg = PostgresSourceConfig {
        host: host.to_string(),
        port,
        user: "postgres".to_string(),
        password: "postgres".to_string().into(),
        database: "cdc".to_string(),
        replication_slot_name: "snapshot_test_slot".to_string(),
        publication_name: "snapshot_test_pub".to_string(),
        conn_timeout_secs: 30,
        stream_poll_interval_ms: 50,
        max_events_per_poll: 1_000,
        ..PostgresSourceConfig::default()
    };

    let mut connection = PostgresConnection::new(source_cfg);
    connection.connect().await?;

    let mut snapshot_handle = connection
        .start_snapshot(&["public.large_snapshot_test"])
        .await?;

    let mut total_rows = 0;
    let mut chunk_count = 0;
    let mut pks = std::collections::HashSet::new();

    loop {
        let events = snapshot_handle.next_chunk(10_000).await?;
        if events.is_empty() {
            break;
        }

        chunk_count += 1;
        total_rows += events.len();

        // Validate chunk: all events have expected structure
        for event in &events {
            assert_eq!(event.op, cdc_rs::Operation::Read);
            assert!(
                event.after.is_some(),
                "snapshot events must have after field"
            );
            assert!(
                event.before.is_none(),
                "snapshot events must not have before"
            );

            // Extract primary key for duplicate detection
            if let Some(after) = &event.after {
                if let Some(id) = after.get("id") {
                    let id_str = id.to_string();
                    let already_seen = !pks.insert(id_str.clone());
                    assert!(
                        !already_seen,
                        "duplicate primary key in snapshot: {}",
                        id_str
                    );
                }
            }
        }

        println!(
            "Chunk {}: {} rows (total so far: {})",
            chunk_count,
            events.len(),
            total_rows
        );
    }

    // Finish snapshot
    let snapshot_end = snapshot_handle.finish().await?;
    // snapshot_end contains snapshot_end_ts marker
    assert!(
        snapshot_end.snapshot_end_ts > 0,
        "snapshot end timestamp must be set"
    );

    // Validate final results
    assert_eq!(
        total_rows, 100_000,
        "expected to read 100K rows from snapshot"
    );
    assert!(
        chunk_count > 9,
        "expected at least 10 chunks for 100K rows with 10K chunk size"
    );
    assert_eq!(
        pks.len(),
        100_000,
        "no duplicates: pks.len() should equal total_rows"
    );

    connection.close().await;

    println!(
        "✓ Snapshot test: read 100K rows in {} chunks with zero duplicates",
        chunk_count
    );

    Ok(())
}

/// Test snapshot checkpoint persistence during long-running snapshot
/// Validates: checkpoint save mid-snapshot and deterministic resume without duplicates
#[tokio::test]
async fn postgres_snapshot_checkpoint_resume_continues_without_duplicates() -> cdc_rs::Result<()> {
    if std::env::var("CDC_RS_RUN_DOCKER_TESTS").as_deref() != Ok("1") {
        eprintln!("skipping postgres snapshot resumption test (set CDC_RS_RUN_DOCKER_TESTS=1)");
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
        .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;

    let host = container
        .get_host()
        .await
        .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;
    let port = container
        .get_host_port_ipv4(5432.tcp())
        .await
        .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;

    let admin_dsn = format!(
        "host={host} port={port} user=postgres password=postgres dbname=cdc connect_timeout=30"
    );
    let (admin_client, admin_conn) = tokio_postgres::connect(&admin_dsn, tokio_postgres::NoTls)
        .await
        .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;
    tokio::spawn(async move {
        let _ = admin_conn.await;
    });

    admin_client
        .batch_execute(
            "
            CREATE TABLE IF NOT EXISTS public.resumable_snapshot_test (
              id BIGINT PRIMARY KEY,
              data TEXT
            );
                        ALTER TABLE public.resumable_snapshot_test REPLICA IDENTITY FULL;
                        DROP PUBLICATION IF EXISTS resumable_snapshot_pub;
                        CREATE PUBLICATION resumable_snapshot_pub FOR TABLE public.resumable_snapshot_test;
            TRUNCATE TABLE public.resumable_snapshot_test;
            ",
        )
        .await
        .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;

    // Insert 50K rows
    for id in 1..=50_000 {
        let id_i64 = i64::from(id);
        let value = format!("data-{id}");
        admin_client
            .execute(
                "INSERT INTO public.resumable_snapshot_test (id, data) VALUES ($1, $2)",
                &[&id_i64, &value],
            )
            .await
            .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;
    }

    let source_cfg = PostgresSourceConfig {
        host: host.to_string(),
        port,
        user: "postgres".to_string(),
        password: "postgres".to_string().into(),
        database: "cdc".to_string(),
        replication_slot_name: "resumable_snapshot_slot".to_string(),
        publication_name: "resumable_snapshot_pub".to_string(),
        conn_timeout_secs: 30,
        stream_poll_interval_ms: 50,
        max_events_per_poll: 1_000,
        ..PostgresSourceConfig::default()
    };

    let mut connection = PostgresConnection::new(source_cfg.clone());
    connection.connect().await?;
    let mut snapshot_handle = connection
        .start_snapshot(&["public.resumable_snapshot_test"])
        .await?;

    // Read first 5K rows
    let mut total_first_read = 0;
    let mut seen_ids = std::collections::HashSet::new();
    for _ in 0..5 {
        let events = snapshot_handle.next_chunk(1000).await?;
        if events.is_empty() {
            break;
        }
        total_first_read += events.len();
        for event in events {
            let after = event.after.ok_or_else(|| {
                cdc_rs::Error::SourceError("snapshot row missing after payload".into())
            })?;
            let id = after
                .get("id")
                .and_then(|value| value.as_i64())
                .ok_or_else(|| cdc_rs::Error::SourceError("snapshot row missing id".into()))?;
            assert!(seen_ids.insert(id), "duplicate id in first phase: {id}");
        }
    }

    assert_eq!(
        total_first_read, 5000,
        "expected to read 5000 rows in first session"
    );

    // Save checkpoint (this should capture the cursor position)
    let checkpoint_dir = tempfile::tempdir().map_err(cdc_rs::Error::IoError)?;
    let mut checkpoint = FileCheckpoint::new(checkpoint_dir.path());
    snapshot_handle
        .checkpoint(&mut checkpoint, total_first_read as u64)
        .await?;

    let saved_offset = checkpoint.load().await?.ok_or_else(|| {
        cdc_rs::Error::CheckpointError("expected saved snapshot checkpoint".into())
    })?;
    assert_eq!(saved_offset.source_type(), "postgres_snapshot");

    println!("✓ First phase read 5000 rows and saved checkpoint payload");

    // Restart source and resume from snapshot checkpoint.
    drop(snapshot_handle);
    connection.close().await;

    let mut resumed_connection = PostgresConnection::new(source_cfg);
    resumed_connection.connect().await?;
    let mut resumed_snapshot = resumed_connection
        .start_snapshot_from_checkpoint(
            &["public.resumable_snapshot_test"],
            Some(saved_offset.as_ref()),
        )
        .await?;

    let mut resumed_count = 0usize;
    let mut resumed_ids = std::collections::HashSet::new();
    for _ in 0..200 {
        let events = resumed_snapshot.next_chunk(1000).await?;
        if events.is_empty() {
            break;
        }

        resumed_count += events.len();
        for event in events {
            let after = event.after.ok_or_else(|| {
                cdc_rs::Error::SourceError("resumed snapshot row missing after payload".into())
            })?;
            let id = after
                .get("id")
                .and_then(|value| value.as_i64())
                .ok_or_else(|| {
                    cdc_rs::Error::SourceError("resumed snapshot row missing id".into())
                })?;
            assert!(
                resumed_ids.insert(id),
                "duplicate id in resumed phase: {id}"
            );
        }
    }

    let _snapshot_end = resumed_snapshot.finish().await?;

    assert_eq!(total_first_read, 5000);
    assert_eq!(seen_ids.len(), 5000);

    // Resume should finish the remaining 45K rows with no overlap against phase 1.
    assert_eq!(
        resumed_count, 45_000,
        "expected remaining rows after resume"
    );
    for id in &seen_ids {
        assert!(
            !resumed_ids.contains(id),
            "resumed phase re-emitted already processed id {id}"
        );
    }

    let total_unique = seen_ids.len() + resumed_ids.len();
    assert_eq!(
        total_unique, 50_000,
        "expected exactly 50K unique ids across phases"
    );

    println!(
        "✓ Resume completed without duplicates: phase1={} phase2={} total_unique={}",
        total_first_read, resumed_count, total_unique
    );

    resumed_connection.close().await;

    Ok(())
}

/// Test snapshot checkpoint resume when table contents mutate between checkpoint
/// and resume windows.
///
/// Validates: no duplicate replay of already checkpointed rows and deterministic
/// convergence for the resumed window under insert/delete churn.
#[tokio::test]
async fn postgres_snapshot_checkpoint_resume_under_mutation_window() -> cdc_rs::Result<()> {
    if std::env::var("CDC_RS_RUN_DOCKER_TESTS").as_deref() != Ok("1") {
        eprintln!(
            "skipping postgres snapshot mutation-window resume test (set CDC_RS_RUN_DOCKER_TESTS=1)"
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
        .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;

    let host = container
        .get_host()
        .await
        .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;
    let port = container
        .get_host_port_ipv4(5432.tcp())
        .await
        .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;

    let admin_dsn = format!(
        "host={host} port={port} user=postgres password=postgres dbname=cdc connect_timeout=30"
    );
    let (admin_client, admin_conn) = tokio_postgres::connect(&admin_dsn, tokio_postgres::NoTls)
        .await
        .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;
    tokio::spawn(async move {
        let _ = admin_conn.await;
    });

    admin_client
        .batch_execute(
            "
            CREATE TABLE IF NOT EXISTS public.resume_mutation_test (
              id BIGINT PRIMARY KEY,
              data TEXT
            );
            ALTER TABLE public.resume_mutation_test REPLICA IDENTITY FULL;
            DROP PUBLICATION IF EXISTS resume_mutation_pub;
            CREATE PUBLICATION resume_mutation_pub FOR TABLE public.resume_mutation_test;
            TRUNCATE TABLE public.resume_mutation_test;
            ",
        )
        .await
        .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;

    for id in 1..=20_000_i64 {
        let value = format!("seed-{id}");
        admin_client
            .execute(
                "INSERT INTO public.resume_mutation_test (id, data) VALUES ($1, $2)",
                &[&id, &value],
            )
            .await
            .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;
    }

    let source_cfg = PostgresSourceConfig {
        host: host.to_string(),
        port,
        user: "postgres".to_string(),
        password: "postgres".to_string().into(),
        database: "cdc".to_string(),
        replication_slot_name: "resume_mutation_slot".to_string(),
        publication_name: "resume_mutation_pub".to_string(),
        conn_timeout_secs: 30,
        stream_poll_interval_ms: 50,
        max_events_per_poll: 1_000,
        ..PostgresSourceConfig::default()
    };

    let mut connection = PostgresConnection::new(source_cfg.clone());
    connection.connect().await?;
    let mut snapshot_handle = connection
        .start_snapshot(&["public.resume_mutation_test"])
        .await?;

    let mut phase1_ids = std::collections::HashSet::new();
    for _ in 0..4 {
        let events = snapshot_handle.next_chunk(1000).await?;
        if events.is_empty() {
            break;
        }
        for event in events {
            let after = event.after.ok_or_else(|| {
                cdc_rs::Error::SourceError("snapshot row missing after payload".into())
            })?;
            let id = after
                .get("id")
                .and_then(|value| value.as_i64())
                .ok_or_else(|| cdc_rs::Error::SourceError("snapshot row missing id".into()))?;
            assert!(phase1_ids.insert(id), "duplicate id in phase1: {id}");
        }
    }

    assert_eq!(phase1_ids.len(), 4_000, "expected 4K rows in phase1");

    let checkpoint_dir = tempfile::tempdir().map_err(cdc_rs::Error::IoError)?;
    let mut checkpoint = FileCheckpoint::new(checkpoint_dir.path());
    snapshot_handle
        .checkpoint(&mut checkpoint, phase1_ids.len() as u64)
        .await?;
    let saved_offset = checkpoint.load().await?.ok_or_else(|| {
        cdc_rs::Error::CheckpointError("expected saved snapshot checkpoint".into())
    })?;

    // Mutate data after checkpoint but before resume: remove a deterministic
    // span from the not-yet-read region and insert a new tail range.
    admin_client
        .execute(
            "DELETE FROM public.resume_mutation_test WHERE id BETWEEN 12000 AND 12100",
            &[],
        )
        .await
        .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;

    admin_client
        .execute(
            "
            INSERT INTO public.resume_mutation_test (id, data)
            SELECT s, 'mut-' || s::text
            FROM generate_series(20001::bigint, 20500::bigint) AS s
            ",
            &[],
        )
        .await
        .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;

    drop(snapshot_handle);
    connection.close().await;

    let mut resumed_connection = PostgresConnection::new(source_cfg);
    resumed_connection.connect().await?;
    let mut resumed_snapshot = resumed_connection
        .start_snapshot_from_checkpoint(
            &["public.resume_mutation_test"],
            Some(saved_offset.as_ref()),
        )
        .await?;

    let mut resumed_ids = std::collections::HashSet::new();
    for _ in 0..80 {
        let events = resumed_snapshot.next_chunk(1000).await?;
        if events.is_empty() {
            break;
        }

        for event in events {
            let after = event.after.ok_or_else(|| {
                cdc_rs::Error::SourceError("resumed snapshot row missing after payload".into())
            })?;
            let id = after
                .get("id")
                .and_then(|value| value.as_i64())
                .ok_or_else(|| {
                    cdc_rs::Error::SourceError("resumed snapshot row missing id".into())
                })?;
            assert!(
                resumed_ids.insert(id),
                "duplicate id in resumed phase: {id}"
            );
        }
    }

    let _snapshot_end = resumed_snapshot.finish().await?;

    for id in &phase1_ids {
        assert!(
            !resumed_ids.contains(id),
            "resumed phase re-emitted already checkpointed id {id}"
        );
    }

    for id in 4_001_i64..=20_000_i64 {
        if (12_000_i64..=12_100_i64).contains(&id) {
            assert!(
                !resumed_ids.contains(&id),
                "deleted id {id} should not appear after resume"
            );
        } else {
            assert!(
                resumed_ids.contains(&id),
                "baseline id {id} missing after resume"
            );
        }
    }

    for id in 20_001_i64..=20_500_i64 {
        assert!(
            resumed_ids.contains(&id),
            "inserted id {id} should appear in resumed window"
        );
    }

    let expected_resumed = (20_000 - 4_000 - 101 + 500) as usize;
    assert_eq!(
        resumed_ids.len(),
        expected_resumed,
        "unexpected resumed row count under mutation window"
    );

    resumed_connection.close().await;
    Ok(())
}

/// Test snapshot checkpoint resume across table boundary interruption.
/// Validates: table A completion + partial table B checkpoint and duplicate-free
/// resume semantics across both tables.
#[tokio::test]
async fn postgres_snapshot_checkpoint_resume_across_table_boundary() -> cdc_rs::Result<()> {
    if std::env::var("CDC_RS_RUN_DOCKER_TESTS").as_deref() != Ok("1") {
        eprintln!(
            "skipping postgres table-boundary snapshot resumption test (set CDC_RS_RUN_DOCKER_TESTS=1)"
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
        .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;

    let host = container
        .get_host()
        .await
        .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;
    let port = container
        .get_host_port_ipv4(5432.tcp())
        .await
        .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;

    let admin_dsn = format!(
        "host={host} port={port} user=postgres password=postgres dbname=cdc connect_timeout=30"
    );
    let (admin_client, admin_conn) = tokio_postgres::connect(&admin_dsn, tokio_postgres::NoTls)
        .await
        .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;
    tokio::spawn(async move {
        let _ = admin_conn.await;
    });

    admin_client
        .batch_execute(
            "
            CREATE TABLE IF NOT EXISTS public.resume_boundary_a (
              id BIGINT PRIMARY KEY,
              data TEXT
            );
            CREATE TABLE IF NOT EXISTS public.resume_boundary_b (
              id BIGINT PRIMARY KEY,
              data TEXT
            );
            ALTER TABLE public.resume_boundary_a REPLICA IDENTITY FULL;
            ALTER TABLE public.resume_boundary_b REPLICA IDENTITY FULL;
            DROP PUBLICATION IF EXISTS resume_boundary_pub;
            CREATE PUBLICATION resume_boundary_pub FOR TABLE public.resume_boundary_a, public.resume_boundary_b;
            TRUNCATE TABLE public.resume_boundary_a;
            TRUNCATE TABLE public.resume_boundary_b;
            ",
        )
        .await
        .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;

    for id in 1..=10_000_i64 {
        let value = format!("a-{id}");
        admin_client
            .execute(
                "INSERT INTO public.resume_boundary_a (id, data) VALUES ($1, $2)",
                &[&id, &value],
            )
            .await
            .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;
    }

    for id in 1..=10_000_i64 {
        let value = format!("b-{id}");
        admin_client
            .execute(
                "INSERT INTO public.resume_boundary_b (id, data) VALUES ($1, $2)",
                &[&id, &value],
            )
            .await
            .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;
    }

    let source_cfg = PostgresSourceConfig {
        host: host.to_string(),
        port,
        user: "postgres".to_string(),
        password: "postgres".to_string().into(),
        database: "cdc".to_string(),
        replication_slot_name: "resume_boundary_slot".to_string(),
        publication_name: "resume_boundary_pub".to_string(),
        conn_timeout_secs: 30,
        stream_poll_interval_ms: 50,
        max_events_per_poll: 1_000,
        ..PostgresSourceConfig::default()
    };

    let mut connection = PostgresConnection::new(source_cfg.clone());
    connection.connect().await?;
    let mut snapshot_handle = connection
        .start_snapshot(&["public.resume_boundary_a", "public.resume_boundary_b"])
        .await?;

    let mut phase1_keys = std::collections::HashSet::new();
    let mut phase1_a = 0usize;
    let mut phase1_b = 0usize;

    // Read until table A is fully done and table B is partially consumed.
    while phase1_a < 10_000 || phase1_b < 3_000 {
        let events = snapshot_handle.next_chunk(500).await?;
        if events.is_empty() {
            return Err(cdc_rs::Error::SourceError(
                "unexpected empty chunk before reaching boundary checkpoint target".into(),
            ));
        }

        for event in events {
            let after = event.after.ok_or_else(|| {
                cdc_rs::Error::SourceError("snapshot row missing after payload".into())
            })?;
            let id = after
                .get("id")
                .and_then(|value| value.as_i64())
                .ok_or_else(|| cdc_rs::Error::SourceError("snapshot row missing id".into()))?;

            let table_name = event.table.as_str();
            let key = format!("{table_name}:{id}");
            assert!(phase1_keys.insert(key), "duplicate key in phase1");

            if table_name == "resume_boundary_a" {
                phase1_a += 1;
            } else if table_name == "resume_boundary_b" {
                phase1_b += 1;
            } else {
                return Err(cdc_rs::Error::SourceError(format!(
                    "unexpected snapshot table '{table_name}'"
                )));
            }
        }
    }

    assert_eq!(
        phase1_a, 10_000,
        "table A should be fully consumed in phase1"
    );
    assert!(
        (3_000..10_000).contains(&phase1_b),
        "table B should be partially consumed in phase1"
    );

    let checkpoint_dir = tempfile::tempdir().map_err(cdc_rs::Error::IoError)?;
    let mut checkpoint = FileCheckpoint::new(checkpoint_dir.path());
    snapshot_handle
        .checkpoint(&mut checkpoint, phase1_keys.len() as u64)
        .await?;

    let saved_offset = checkpoint.load().await?.ok_or_else(|| {
        cdc_rs::Error::CheckpointError("expected saved snapshot checkpoint".into())
    })?;
    assert_eq!(saved_offset.source_type(), "postgres_snapshot");

    drop(snapshot_handle);
    connection.close().await;

    let mut resumed_connection = PostgresConnection::new(source_cfg);
    resumed_connection.connect().await?;
    let mut resumed_snapshot = resumed_connection
        .start_snapshot_from_checkpoint(
            &["public.resume_boundary_a", "public.resume_boundary_b"],
            Some(saved_offset.as_ref()),
        )
        .await?;

    let mut resumed_keys = std::collections::HashSet::new();
    let mut resumed_a = 0usize;
    let mut resumed_b = 0usize;

    for _ in 0..200 {
        let events = resumed_snapshot.next_chunk(500).await?;
        if events.is_empty() {
            break;
        }

        for event in events {
            let after = event.after.ok_or_else(|| {
                cdc_rs::Error::SourceError("resumed snapshot row missing after payload".into())
            })?;
            let id = after
                .get("id")
                .and_then(|value| value.as_i64())
                .ok_or_else(|| {
                    cdc_rs::Error::SourceError("resumed snapshot row missing id".into())
                })?;
            let table_name = event.table.as_str();
            let key = format!("{table_name}:{id}");
            assert!(resumed_keys.insert(key), "duplicate key in resumed phase");

            if table_name == "resume_boundary_a" {
                resumed_a += 1;
            } else if table_name == "resume_boundary_b" {
                resumed_b += 1;
            } else {
                return Err(cdc_rs::Error::SourceError(format!(
                    "unexpected resumed snapshot table '{table_name}'"
                )));
            }
        }
    }

    let _snapshot_end = resumed_snapshot.finish().await?;

    for key in &phase1_keys {
        assert!(
            !resumed_keys.contains(key),
            "resumed phase re-emitted already processed key {key}"
        );
    }

    assert_eq!(
        phase1_a + resumed_a,
        10_000,
        "table A total row count mismatch"
    );
    assert_eq!(
        phase1_b + resumed_b,
        10_000,
        "table B total row count mismatch"
    );

    let total_unique = phase1_keys.len() + resumed_keys.len();
    assert_eq!(
        total_unique, 20_000,
        "expected 20K unique keys across both phases"
    );

    println!(
        "✓ Table-boundary resume completed without duplicates: phase1(a={}, b={}) phase2(a={}, b={}) total_unique={}",
        phase1_a,
        phase1_b,
        resumed_a,
        resumed_b,
        total_unique
    );

    resumed_connection.close().await;
    Ok(())
}

/// Test empty table snapshot
#[tokio::test]
async fn postgres_snapshot_empty_table() -> cdc_rs::Result<()> {
    if std::env::var("CDC_RS_RUN_DOCKER_TESTS").as_deref() != Ok("1") {
        eprintln!("skipping postgres empty snapshot test (set CDC_RS_RUN_DOCKER_TESTS=1)");
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
        .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;

    let host = container
        .get_host()
        .await
        .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;
    let port = container
        .get_host_port_ipv4(5432.tcp())
        .await
        .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;

    let admin_dsn = format!(
        "host={host} port={port} user=postgres password=postgres dbname=cdc connect_timeout=30"
    );
    let (admin_client, admin_conn) = tokio_postgres::connect(&admin_dsn, tokio_postgres::NoTls)
        .await
        .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;
    tokio::spawn(async move {
        let _ = admin_conn.await;
    });

    admin_client
        .batch_execute(
            "
            CREATE TABLE IF NOT EXISTS public.empty_snapshot_test (
              id BIGINT PRIMARY KEY
            );
                        ALTER TABLE public.empty_snapshot_test REPLICA IDENTITY FULL;
                        DROP PUBLICATION IF EXISTS empty_snapshot_pub;
                        CREATE PUBLICATION empty_snapshot_pub FOR TABLE public.empty_snapshot_test;
            TRUNCATE TABLE public.empty_snapshot_test;
            ",
        )
        .await
        .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;

    let source_cfg = PostgresSourceConfig {
        host: host.to_string(),
        port,
        user: "postgres".to_string(),
        password: "postgres".to_string().into(),
        database: "cdc".to_string(),
        replication_slot_name: "empty_snapshot_slot".to_string(),
        publication_name: "empty_snapshot_pub".to_string(),
        conn_timeout_secs: 30,
        stream_poll_interval_ms: 50,
        max_events_per_poll: 1_000,
        ..PostgresSourceConfig::default()
    };

    let mut connection = PostgresConnection::new(source_cfg);
    connection.connect().await?;
    let mut snapshot_handle = connection
        .start_snapshot(&["public.empty_snapshot_test"])
        .await?;

    // First next_chunk should return empty
    let first_chunk = snapshot_handle.next_chunk(1000).await?;
    assert!(
        first_chunk.is_empty(),
        "empty table snapshot should return no rows"
    );

    // Finish should return snapshot end event
    let snapshot_end = snapshot_handle.finish().await?;
    assert!(snapshot_end.snapshot_end_ts > 0);

    connection.close().await;

    println!("✓ Empty table snapshot handled correctly");

    Ok(())
}

/// Test snapshot correctness under concurrent write pressure.
/// Validates: no baseline PK gaps and bounded duplicate window while concurrent
/// inserts are applied during snapshot reads.
#[tokio::test]
async fn postgres_snapshot_concurrent_write_pressure_correctness() -> cdc_rs::Result<()> {
    if std::env::var("CDC_RS_RUN_DOCKER_TESTS").as_deref() != Ok("1") {
        eprintln!("skipping postgres concurrent snapshot test (set CDC_RS_RUN_DOCKER_TESTS=1)");
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
        .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;

    let host = container
        .get_host()
        .await
        .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;
    let port = container
        .get_host_port_ipv4(5432.tcp())
        .await
        .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;

    let admin_dsn = format!(
        "host={host} port={port} user=postgres password=postgres dbname=cdc connect_timeout=30"
    );
    let (admin_client, admin_conn) = tokio_postgres::connect(&admin_dsn, tokio_postgres::NoTls)
        .await
        .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;
    tokio::spawn(async move {
        let _ = admin_conn.await;
    });

    admin_client
        .batch_execute(
            "
            CREATE TABLE IF NOT EXISTS public.concurrent_snapshot_test (
              id BIGINT PRIMARY KEY,
              payload TEXT NOT NULL
            );
            ALTER TABLE public.concurrent_snapshot_test REPLICA IDENTITY FULL;
            DROP PUBLICATION IF EXISTS concurrent_snapshot_pub;
            CREATE PUBLICATION concurrent_snapshot_pub FOR TABLE public.concurrent_snapshot_test;
            TRUNCATE TABLE public.concurrent_snapshot_test;
            ",
        )
        .await
        .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;

    let baseline_rows: i64 = 20_000;
    for batch_start in (1..=baseline_rows).step_by(1000) {
        let batch_end = (batch_start + 999).min(baseline_rows);
        let mut values = Vec::new();
        for id in batch_start..=batch_end {
            values.push(format!("({id}, 'baseline-{id}')"));
        }
        let sql = format!(
            "INSERT INTO public.concurrent_snapshot_test (id, payload) VALUES {}",
            values.join(", ")
        );
        admin_client
            .batch_execute(&sql)
            .await
            .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;
    }

    let source_cfg = PostgresSourceConfig {
        host: host.to_string(),
        port,
        user: "postgres".to_string(),
        password: "postgres".to_string().into(),
        database: "cdc".to_string(),
        replication_slot_name: "concurrent_snapshot_slot".to_string(),
        publication_name: "concurrent_snapshot_pub".to_string(),
        conn_timeout_secs: 30,
        stream_poll_interval_ms: 50,
        max_events_per_poll: 1_000,
        ..PostgresSourceConfig::default()
    };

    let writer_dsn = admin_dsn.clone();
    let writer = tokio::spawn(async move {
        let (writer_client, writer_conn) =
            tokio_postgres::connect(&writer_dsn, tokio_postgres::NoTls)
                .await
                .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;
        tokio::spawn(async move {
            let _ = writer_conn.await;
        });

        for round in 0_i64..120_i64 {
            let base = 1_000_000_i64 + round * 25_i64;
            writer_client
                .execute(
                    "
                    INSERT INTO public.concurrent_snapshot_test (id, payload)
                    SELECT s, 'concurrent-' || s::text
                    FROM generate_series($1::bigint, $2::bigint) AS s
                    ON CONFLICT (id) DO NOTHING
                    ",
                    &[&base, &(base + 24_i64)],
                )
                .await
                .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;

            if round % 8_i64 == 0 {
                sleep(Duration::from_millis(5)).await;
            }
        }

        Ok::<(), cdc_rs::Error>(())
    });

    let mut connection = PostgresConnection::new(source_cfg);
    connection.connect().await?;
    let mut snapshot_handle = connection
        .start_snapshot(&["public.concurrent_snapshot_test"])
        .await?;

    let mut seen_baseline = std::collections::HashSet::new();
    let mut baseline_duplicates: usize = 0;
    let mut total_events: usize = 0;

    loop {
        let events = snapshot_handle.next_chunk(1000).await?;
        if events.is_empty() {
            break;
        }

        total_events += events.len();
        for event in events {
            let after = event.after.ok_or_else(|| {
                cdc_rs::Error::SourceError("snapshot row missing after payload".into())
            })?;
            let id = after
                .get("id")
                .and_then(|value| value.as_i64())
                .ok_or_else(|| cdc_rs::Error::SourceError("snapshot row missing id".into()))?;

            if id <= baseline_rows && !seen_baseline.insert(id) {
                baseline_duplicates = baseline_duplicates.saturating_add(1);
            }
        }
    }

    let snapshot_end = snapshot_handle.finish().await?;
    assert!(snapshot_end.snapshot_end_ts > 0);

    let writer_result = writer
        .await
        .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;
    writer_result?;

    let mut missing = Vec::new();
    for id in 1_i64..=baseline_rows {
        if !seen_baseline.contains(&id) {
            missing.push(id);
            if missing.len() >= 10 {
                break;
            }
        }
    }

    assert!(
        missing.is_empty(),
        "baseline snapshot is missing ids; sample missing: {missing:?}"
    );
    assert!(
        baseline_duplicates <= 16,
        "expected bounded duplicate window for baseline rows, got {baseline_duplicates}"
    );
    assert!(
        total_events >= baseline_rows as usize,
        "snapshot must include at least baseline row count"
    );

    connection.close().await;
    Ok(())
}
