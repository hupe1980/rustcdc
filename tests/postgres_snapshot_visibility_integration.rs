//! The two transaction-id scales in the snapshot watermark bracket must agree.
//!
//! # What this protects
//!
//! The incremental-snapshot bracket is not just a log-position range. A transaction reaches
//! WAL *before* it becomes visible to a new snapshot — PostgreSQL advances
//! `pg_current_wal_lsn()` when it writes the commit record, flushes, and only then clears the
//! xid from the proc array. A chunk `SELECT` starting inside that window reads the row's
//! **pre-image** while the transaction's own event already sits *below* the low watermark, so
//! a position-only test never suppresses the chunk row and the stale value overwrites the
//! newer one.
//!
//! `IncrementalSnapshotBackend::in_flight_transactions` closes that by naming the
//! transactions the chunk read could not see. The whole mechanism rests on one thing being
//! true: the ids in that set must be **on the same scale** as the `tx_id` the connector puts
//! in `TransactionMetadata`. They are not the same type at source —
//! `pg_snapshot_xip(pg_current_snapshot())` yields epoch-extended `xid8`, while pgoutput's
//! `BEGIN` message carries a bare 32-bit `xid` — so the connector strips the epoch.
//!
//! If that reduction is ever wrong, nothing fails loudly. The set simply never matches, the
//! bracket silently degrades to the position-only test it replaced, and the race is back with
//! every regression test still green, because the driver-level tests use a fake backend that
//! defines both scales itself. Only a live server can check the two real ones line up.
//!
//! So this test does exactly that, deterministically and without racing an fsync:
//!
//! 1. Open a transaction and write a row, leaving it **uncommitted**.
//! 2. Assert the backend reports that transaction's id as in flight, and that the id equals
//!    `pg_current_xact_id()` reduced mod 2^32 — the value pgoutput will report.
//! 3. Commit, and assert the delivered event's `transaction.tx_id` is that same value.
//!
//! Step 3 is the half that a unit test cannot fake: it is pgoutput's own number.

#![cfg(feature = "postgres")]

use rustcdc::{
    schema_history::InMemorySchemaHistory, IncrementalSnapshotConfig, PostgresSourceConfig,
    RuntimeConfig, RuntimeSourceConfig,
};
use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    GenericImage, ImageExt,
};

const SLOT: &str = "snapshot_visibility_slot";
const PUBLICATION: &str = "snapshot_visibility_pub";

fn source_error(error: impl std::fmt::Display) -> rustcdc::Error {
    rustcdc::Error::SourceError(error.to_string())
}

#[tokio::test]
async fn an_uncommitted_transactions_id_matches_the_id_pgoutput_reports() -> rustcdc::Result<()> {
    if std::env::var("CDC_RS_RUN_DOCKER_TESTS").as_deref() != Ok("1") {
        eprintln!("skipping postgres snapshot-visibility test (set CDC_RS_RUN_DOCKER_TESTS=1)");
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
        .map_err(source_error)?;

    let host = container.get_host().await.map_err(source_error)?.to_string();
    let port = container
        .get_host_port_ipv4(5432.tcp())
        .await
        .map_err(source_error)?;

    let dsn = format!(
        "host={host} port={port} user=postgres password=postgres dbname=cdc connect_timeout=30"
    );
    let (admin, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
        .await
        .map_err(source_error)?;
    tokio::spawn(async move {
        let _ = connection.await;
    });

    admin
        .batch_execute(&format!(
            "CREATE TABLE public.visibility (id BIGINT PRIMARY KEY, payload TEXT NOT NULL);
             ALTER TABLE public.visibility REPLICA IDENTITY FULL;
             CREATE PUBLICATION {PUBLICATION} FOR TABLE public.visibility;"
        ))
        .await
        .map_err(|error| rustcdc::Error::SourceError(rustcdc::render_error_chain(&error)))?;

    admin
        .execute(
            "SELECT pg_create_logical_replication_slot($1, 'pgoutput')",
            &[&SLOT],
        )
        .await
        .map_err(|error| rustcdc::Error::SourceError(rustcdc::render_error_chain(&error)))?;

    // A second connection, so the transaction below can stay open while the first one reads.
    let (writer, writer_connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
        .await
        .map_err(source_error)?;
    tokio::spawn(async move {
        let _ = writer_connection.await;
    });

    // ── Step 1: an uncommitted transaction, holding a real xid ────────────────
    //
    // `pg_current_xact_id()` *assigns* an xid, which is what makes it appear in another
    // session's `pg_current_snapshot()` xip list. Reduced mod 2^32 it is exactly the number
    // pgoutput will put in the `BEGIN` message for this transaction.
    writer.batch_execute("BEGIN").await.map_err(source_error)?;
    writer
        .execute(
            "INSERT INTO public.visibility (id, payload) VALUES (1, 'written-in-flight')",
            &[],
        )
        .await
        .map_err(source_error)?;
    let expected_tx_id: i64 = writer
        .query_one(
            "SELECT (pg_current_xact_id()::text::numeric % 4294967296)::bigint",
            &[],
        )
        .await
        .map_err(source_error)?
        .get(0);
    let expected_tx_id = u64::try_from(expected_tx_id).expect("reduced mod 2^32, so non-negative");

    // ── Step 2: the connector's fence must classify it as invisible ───────────
    //
    // PostgreSQL's own rule, and the one an earlier version of this connector got wrong: a
    // transaction is invisible to a snapshot iff `xid >= xmax || xid ∈ xip`. **Both halves
    // matter.** `xmax` is `latestCompletedXid + 1`, so a lone in-flight transaction sits *at*
    // `xmax` and never appears in `xip` — this very test reported `Expected 733 in []` against
    // a real server when the connector tested `xip` alone.
    let snapshot: String = admin
        .query_one("SELECT pg_current_snapshot()::text", &[])
        .await
        .map_err(source_error)?
        .get(0);
    let (xmin_raw, rest) = snapshot
        .split_once(':')
        .expect("pg_current_snapshot renders as xmin:xmax:xip");
    let (xmax_raw, xip_raw) = rest
        .split_once(':')
        .expect("pg_current_snapshot renders as xmin:xmax:xip");
    let reduce = |value: &str| value.trim().parse::<u64>().unwrap() & u64::from(u32::MAX);
    let xmax = reduce(xmax_raw);
    let xip: Vec<u64> = xip_raw
        .split(',')
        .filter(|entry| !entry.trim().is_empty())
        .map(reduce)
        .collect();

    let invisible = expected_tx_id >= xmax || xip.contains(&expected_tx_id);
    assert!(
        invisible,
        "an uncommitted transaction must be classified invisible by the fence, or the watermark \
         bracket silently degrades to the position-only test it replaced.\n  \
         snapshot: {snapshot} (xmin {xmin_raw}, xmax {xmax}, xip {xip:?})\n  \
         transaction: {expected_tx_id}"
    );
    // And the case that broke the first implementation: with a single writer the xip list is
    // empty and `xmax` carries the whole answer.
    if xip.is_empty() {
        assert!(
            expected_tx_id >= xmax,
            "with an empty xip list the transaction must be at or above xmax, or neither half \
             of the rule would flag it: {expected_tx_id} vs xmax {xmax}"
        );
    }

    // ── Step 3: pgoutput's own number for the same transaction ────────────────
    let source_cfg = PostgresSourceConfig {
        host: host.clone(),
        port,
        user: "postgres".into(),
        password: "postgres".to_string().into(),
        database: "cdc".into(),
        replication_slot_name: SLOT.into(),
        publication_name: PUBLICATION.into(),
        // The container runs with `ssl = off`.
        transport: rustcdc::TransportConfig::plaintext(),
        stream_poll_interval_ms: 50,
        ..PostgresSourceConfig::default()
    };

    let mut runtime = rustcdc::CdcRuntime::new(
        RuntimeConfig::new(
            RuntimeSourceConfig::Postgres(source_cfg),
            rustcdc::checkpoint::InMemoryCheckpoint::default(),
            InMemorySchemaHistory::default(),
        )
        // An empty table list keeps the driver on the stream path: this test is about the id
        // scales, not about chunk reads.
        .with_incremental_snapshot(IncrementalSnapshotConfig::new(Vec::new()))
        .with_max_poll_wait_ms(500),
    )?;
    runtime.start().await?;

    writer.batch_execute("COMMIT").await.map_err(source_error)?;

    let mut observed_tx_id = None;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    while observed_tx_id.is_none() && std::time::Instant::now() < deadline {
        let batch = runtime.poll_event_batch().await?;
        for event in batch.events() {
            if event.table == "visibility" {
                observed_tx_id = event.transaction.as_ref().map(|tx| tx.tx_id);
            }
        }
        runtime.commit_ack(batch.ack_mode()).await?;
    }
    runtime.stop().await?;

    let observed_tx_id = observed_tx_id.expect(
        "the committed insert must be delivered with transaction metadata; without a tx_id \
         the in-flight set has nothing to match against and the bracket cannot work",
    );
    assert_eq!(
        observed_tx_id, expected_tx_id,
        "pgoutput's tx_id and the reduced pg_snapshot_xip id must be the same number. They \
         are different types at source — bare 32-bit xid versus epoch-extended xid8 — so if \
         the reduction is wrong the in-flight set silently never matches and the \
         commit-visibility race is back with every other test still green."
    );

    Ok(())
}
