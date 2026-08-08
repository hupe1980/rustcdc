//! Integration tests that exercise startup reconciliation and confirm-LSN
//! behaviour against a real PostgreSQL instance (started via testcontainers).
//!
//! These tests cover the paths that the unit tests in `query.rs` cannot reach
//! because the I/O operations require a live database connection.
//!
//! Run with:
//!   CDC_RS_RUN_DOCKER_TESTS=1 cargo test --test postgres_query_integration --features postgres

#![cfg(feature = "postgres")]

use rustcdc::{
    checkpoint::{FileCheckpoint, PostgresOffset},
    source::Source,
    PostgresConnection, PostgresSourceConfig,
};
use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    GenericImage, ImageExt,
};

// ── Shared helpers ────────────────────────────────────────────────────────────

async fn start_pg_container() -> testcontainers::ContainerAsync<GenericImage> {
    GenericImage::new("postgres", "16-alpine")
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
        .expect("failed to start postgres container")
}

async fn admin_connect(host: impl std::fmt::Display, port: u16) -> tokio_postgres::Client {
    let dsn = format!(
        "host={host} port={port} user=postgres password=postgres dbname=cdc connect_timeout=30"
    );
    let (client, conn) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
        .await
        .expect("admin connection failed");
    tokio::spawn(async move {
        let _ = conn.await;
    });
    client
}

fn source_config(
    host: impl std::fmt::Display,
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
        // Both tests in this file are about the **SQL-peek** transport's slot bookkeeping:
        // `confirm_lsn` advancing the slot with `pg_replication_slot_advance`, and startup
        // repairing a checkpoint that sits ahead of `confirmed_flush_lsn`. Neither exists
        // under streaming replication, where the resume LSN is a `START_REPLICATION`
        // parameter and progress is reported with Standby Status Updates — so pinning the
        // transport is what keeps these covering the code they were written for.
        wal_transport: rustcdc::WalTransport::SqlPeek,
        ..PostgresSourceConfig::default()
    }
}

/// Parse a Postgres "HIGH/LOW" LSN string (e.g. `"0/1A2B3C4D"`) into a u64.
fn parse_pg_lsn(s: &str) -> u64 {
    let (high, low) = s.split_once('/').expect("invalid LSN format");
    let high = u64::from_str_radix(high, 16).expect("invalid LSN high bits");
    let low = u64::from_str_radix(low, 16).expect("invalid LSN low bits");
    (high << 32) | low
}

// ── Test 1: startup self-heal ─────────────────────────────────────────────────

/// Verifies that when the checkpoint LSN is ahead of the replication slot's
/// `confirmed_flush_lsn`, `start_stream` self-heals by advancing the slot to
/// the checkpoint position and continues streaming new events normally.
///
/// This is the BUG-5 scenario: a previous process confirmed a checkpoint LSN
/// but the corresponding `pg_replication_slot_advance` call failed (network
/// error, Postgres restart, or the type-casting bug fixed in 0.6.4).  On the
/// next restart the slot was behind the checkpoint.  Without the self-heal the
/// runtime returned a fatal error and looped forever.
#[tokio::test]
async fn startup_self_heals_when_checkpoint_lsn_ahead_of_slot() {
    if std::env::var("CDC_RS_RUN_DOCKER_TESTS").as_deref() != Ok("1") {
        eprintln!("skipping startup_self_heals test (set CDC_RS_RUN_DOCKER_TESTS=1 to enable)");
        return;
    }

    const SLOT: &str = "reconcile_selfheal_slot";
    const PUB: &str = "reconcile_selfheal_pub";

    let container = start_pg_container().await;
    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(5432.tcp()).await.unwrap();
    let admin = admin_connect(&host, port).await;

    // Create table and publication.
    admin
        .batch_execute(&format!(
            "
            CREATE TABLE IF NOT EXISTS public.selfheal_test (
              id   BIGINT PRIMARY KEY,
              val  TEXT
            );
            ALTER TABLE public.selfheal_test REPLICA IDENTITY FULL;
            DROP PUBLICATION IF EXISTS {PUB};
            CREATE PUBLICATION {PUB} FOR TABLE public.selfheal_test;
            TRUNCATE TABLE public.selfheal_test;
            "
        ))
        .await
        .unwrap();

    // Create replication slot manually.  confirmed_flush_lsn starts NULL.
    admin
        .execute(
            "SELECT pg_create_logical_replication_slot($1, 'pgoutput')",
            &[&SLOT],
        )
        .await
        .unwrap();

    // Advance slot to current WAL LSN so confirmed_flush_lsn is non-NULL (= L1).
    admin
        .execute(
            "SELECT pg_replication_slot_advance($1, pg_current_wal_lsn()::text::pg_lsn)",
            &[&SLOT],
        )
        .await
        .unwrap();

    // Insert rows so WAL advances to L2 > L1.
    for id in 1i64..=5 {
        admin
            .execute(
                "INSERT INTO public.selfheal_test VALUES ($1, $2)",
                &[&id, &format!("pre-{id}")],
            )
            .await
            .unwrap();
    }

    // Capture L2 (current WAL LSN) — the checkpoint will claim this as its
    // confirmed position, but the slot is still at L1.
    let l2_text: String = admin
        .query_one("SELECT pg_current_wal_lsn()::text", &[])
        .await
        .unwrap()
        .get(0);
    let l2_lsn = parse_pg_lsn(&l2_text);

    // Seed a checkpoint with LSN = L2 (ahead of the slot still at L1).
    // This simulates a process that confirmed L2 but whose slot advance failed.
    let checkpoint_dir = tempfile::tempdir().unwrap();
    let offset = PostgresOffset {
        lsn: l2_lsn,
        slot_name: SLOT.to_string(),
        incremental_snapshot: None,
    };
    use rustcdc::core::Offset as _;
    FileCheckpoint::restore_from_record(
        checkpoint_dir.path(),
        "postgres",
        offset.encode().unwrap(),
        5,
    )
    .unwrap();

    // Connect to Postgres (slot already exists; connect() will reuse it).
    let mut connection = PostgresConnection::new(source_config(&host, port, SLOT, PUB));
    connection.connect().await.expect("connect failed");

    // start_stream with the seeded checkpoint offset must NOT return an error.
    // Internally this calls reconcile_stream_resume_lsn_with_retry which detects
    // L2 > L1 and self-heals by advancing the slot to L2.
    let mut stream = connection
        .start_stream(Some(&offset))
        .await
        .expect("start_stream must succeed after self-heal (not return a fatal error)");

    // Assert: slot was advanced to at least L2.
    let confirmed_text: String = admin
        .query_one(
            "SELECT confirmed_flush_lsn::text \
             FROM pg_catalog.pg_replication_slots \
             WHERE slot_name = $1",
            &[&SLOT],
        )
        .await
        .unwrap()
        .get(0);
    let confirmed_lsn = parse_pg_lsn(&confirmed_text);
    assert!(
        confirmed_lsn >= l2_lsn,
        "slot must have been advanced to at least L2 by self-heal; \
         confirmed_flush_lsn = {confirmed_lsn:#X}, expected >= {l2_lsn:#X}"
    );

    // Insert rows after L2 — these must be streamed normally.
    for id in 10i64..=14 {
        admin
            .execute(
                "INSERT INTO public.selfheal_test VALUES ($1, $2)",
                &[&id, &format!("post-{id}")],
            )
            .await
            .unwrap();
    }

    let mut events = Vec::new();
    for _ in 0..100 {
        let batch = stream.next_events(200).await.unwrap();
        events.extend(batch);
        if events.len() >= 5 {
            break;
        }
    }

    let inserts: Vec<_> = events
        .iter()
        .filter(|e| e.op == rustcdc::Operation::Insert)
        .collect();
    assert!(
        inserts.len() >= 5,
        "expected 5 INSERT events for post-self-heal rows, got {}",
        inserts.len()
    );

    drop(stream);
    connection.close().await;
}

// ── Test 2: confirm_lsn keeps the slot advancing across batches ───────────────

/// Verifies that `confirm_lsn` keeps advancing the replication slot after
/// each event batch.  This is the integration-level regression guard for the
/// `lsn < confirmed_lsn` guard fix in `decoder.rs`: the old `<=` guard would
/// skip `pg_replication_slot_advance` when `lsn == confirmed_lsn`, which could
/// stall the stream in edge cases (e.g. after `idle_advance` set
/// `confirmed_lsn` to the exact WAL position of the next commit).
#[tokio::test]
async fn confirm_lsn_keeps_advancing_slot_across_batches() {
    if std::env::var("CDC_RS_RUN_DOCKER_TESTS").as_deref() != Ok("1") {
        eprintln!(
            "skipping confirm_lsn multi-batch test (set CDC_RS_RUN_DOCKER_TESTS=1 to enable)"
        );
        return;
    }

    const SLOT: &str = "confirm_lsn_advance_slot";
    const PUB: &str = "confirm_lsn_advance_pub";

    let container = start_pg_container().await;
    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(5432.tcp()).await.unwrap();
    let admin = admin_connect(&host, port).await;

    admin
        .batch_execute(&format!(
            "
            CREATE TABLE IF NOT EXISTS public.confirm_lsn_test (
              id  BIGINT PRIMARY KEY,
              val TEXT
            );
            ALTER TABLE public.confirm_lsn_test REPLICA IDENTITY FULL;
            DROP PUBLICATION IF EXISTS {PUB};
            CREATE PUBLICATION {PUB} FOR TABLE public.confirm_lsn_test;
            TRUNCATE TABLE public.confirm_lsn_test;
            "
        ))
        .await
        .unwrap();

    let mut connection = PostgresConnection::new(source_config(&host, port, SLOT, PUB));
    connection.connect().await.expect("connect failed");
    let mut stream = connection
        .start_stream(None)
        .await
        .expect("start_stream failed");

    // ── Batch 1 ──────────────────────────────────────────────────────────────
    for id in 1i64..=5 {
        admin
            .execute(
                "INSERT INTO public.confirm_lsn_test VALUES ($1, $2)",
                &[&id, &format!("b1-{id}")],
            )
            .await
            .unwrap();
    }

    let mut batch1 = Vec::new();
    for _ in 0..100 {
        let events = stream.next_events(200).await.unwrap();
        batch1.extend(events);
        if batch1.len() >= 5 {
            break;
        }
    }
    assert!(
        batch1.len() >= 5,
        "expected at least 5 events in batch 1, got {}",
        batch1.len()
    );

    // Confirm the LSN of the last event in batch 1.
    let last_lsn = parse_pg_lsn(&batch1.last().unwrap().source.offset);
    stream
        .confirm_lsn(last_lsn)
        .await
        .expect("confirm_lsn batch 1 failed");

    // Verify the slot advanced to at least the confirmed LSN.
    let after_b1_text: String = admin
        .query_one(
            "SELECT confirmed_flush_lsn::text \
             FROM pg_catalog.pg_replication_slots \
             WHERE slot_name = $1",
            &[&SLOT],
        )
        .await
        .unwrap()
        .get(0);
    let after_b1_lsn = parse_pg_lsn(&after_b1_text);
    assert!(
        after_b1_lsn >= last_lsn,
        "slot must advance to at least batch-1 LSN after confirm_lsn; \
         confirmed_flush_lsn = {after_b1_lsn:#X}, expected >= {last_lsn:#X}"
    );

    // ── Batch 2 ──────────────────────────────────────────────────────────────
    for id in 10i64..=14 {
        admin
            .execute(
                "INSERT INTO public.confirm_lsn_test VALUES ($1, $2)",
                &[&id, &format!("b2-{id}")],
            )
            .await
            .unwrap();
    }

    let mut batch2 = Vec::new();
    for _ in 0..100 {
        let events = stream.next_events(200).await.unwrap();
        batch2.extend(events);
        if batch2.len() >= 5 {
            break;
        }
    }
    assert!(
        batch2.len() >= 5,
        "expected at least 5 events in batch 2 (stream must not stall after confirm_lsn), got {}",
        batch2.len()
    );

    // Confirm batch 2's LSN and verify the slot advanced further.
    let last_lsn_b2 = parse_pg_lsn(&batch2.last().unwrap().source.offset);
    stream
        .confirm_lsn(last_lsn_b2)
        .await
        .expect("confirm_lsn batch 2 failed");

    let after_b2_text: String = admin
        .query_one(
            "SELECT confirmed_flush_lsn::text \
             FROM pg_catalog.pg_replication_slots \
             WHERE slot_name = $1",
            &[&SLOT],
        )
        .await
        .unwrap()
        .get(0);
    let after_b2_lsn = parse_pg_lsn(&after_b2_text);
    assert!(
        after_b2_lsn >= last_lsn_b2,
        "slot must advance to at least batch-2 LSN after confirm_lsn; \
         confirmed_flush_lsn = {after_b2_lsn:#X}, expected >= {last_lsn_b2:#X}"
    );
    assert!(
        after_b2_lsn > after_b1_lsn,
        "batch-2 slot LSN must be strictly greater than batch-1 slot LSN; \
         b1={after_b1_lsn:#X}, b2={after_b2_lsn:#X}"
    );

    drop(stream);
    connection.close().await;
}
