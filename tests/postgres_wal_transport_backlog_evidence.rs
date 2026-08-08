//! Measures the condition that motivates the default WAL transport.
//!
//! `WalTransport::SqlPeek` reads the slot with `pg_logical_slot_peek_binary_changes`, which is
//! **non-consuming**: PostgreSQL begins decoding at the slot's `restart_lsn` and only *emits*
//! changes past `confirmed_flush_lsn`. A logical slot's `restart_lsn` cannot advance past the
//! start of the oldest transaction still running on the source, so **one long-running
//! transaction pins it** — and from then on every poll re-reads the WAL between the two
//! positions before producing anything.
//!
//! `StreamingReplication` pays that scan once, when the connection is established.
//!
//! What this harness establishes, on identical hardware and workload:
//!
//! * `SqlPeek` is consistently **4–5× slower** than the default transport for the same capture.
//! * It does **not** show peek degrading further as the WAL behind `restart_lsn` grows —
//!   doubling that distance (146 MiB → 292 MiB) did not slow it measurably. At these volumes the
//!   WAL had just been written and was served from page cache. Whether the re-read becomes
//!   expensive when that WAL is cold is *not* settled here, and the docs say so rather than
//!   asserting a cost this harness cannot measure.
//!
//! It is an **evidence harness**, not a pass/fail gate: absolute timings are hardware-dependent
//! and a flaky performance gate teaches people to ignore failures. It prints a table, and asserts
//! only that the **default** transport captures everything in both halves — the property an
//! embedder actually relies on.

#![cfg(feature = "postgres")]

use std::time::{Duration, Instant};

use rustcdc::{source::Source, Operation, PostgresConnection, PostgresSourceConfig, WalTransport};
use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    ContainerAsync, GenericImage, ImageExt,
};

/// Rows captured per measured run.
const ROWS: i64 = 300;
/// Rows of WAL filler written before capture, in `FILLER_BATCHES` bulk statements.
///
/// `pg_logical_slot_peek_binary_changes` reads WAL from the slot's `restart_lsn` on **every**
/// call (`XLogBeginRead(reader, restart_lsn)` in `pg_logical_slot_get_changes_guts`), so the
/// per-poll cost is proportional to this volume once the slot is pinned. A few megabytes is
/// indistinguishable from noise; this is sized to make the mechanism visible.
const FILLER_ROWS_PER_BATCH: i64 = 8_192;
const FILLER_BATCHES: i64 = 4;
/// Payload width per filler row — 4 KiB × 8 192 × 4 ≈ 128 MiB of WAL.
const PAYLOAD_BYTES: usize = 4_096;

struct Measurement {
    transport: &'static str,
    pinned: bool,
    elapsed: Duration,
    polls: u32,
    events: usize,
    restart_gap_bytes: i64,
}

async fn start_postgres() -> rustcdc::Result<ContainerAsync<GenericImage>> {
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
            "max_replication_slots=16",
            "-c",
            "max_wal_senders=16",
        ])
        .start()
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))
}

async fn connect(dsn: &str) -> rustcdc::Result<tokio_postgres::Client> {
    let (client, connection) = tokio_postgres::connect(dsn, tokio_postgres::NoTls)
        .await
        .map_err(|error| rustcdc::Error::SourceError(rustcdc::render_error_chain(&error)))?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    Ok(client)
}

/// WAL the decoder must read past on each poll: current end-of-WAL minus the slot's
/// `restart_lsn`. This is the independent variable — a pinned slot lets it grow without bound.
async fn restart_gap_bytes(admin: &tokio_postgres::Client, slot: &str) -> rustcdc::Result<i64> {
    let row = admin
        .query_one(
            "SELECT COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn), 0)::bigint \
             FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot],
        )
        .await
        .map_err(|error| rustcdc::Error::SourceError(rustcdc::render_error_chain(&error)))?;
    Ok(row.get::<_, i64>(0))
}

async fn measure(
    admin: &tokio_postgres::Client,
    host: &str,
    port: u16,
    slot: &str,
    transport: WalTransport,
    pinned: bool,
) -> rustcdc::Result<Measurement> {
    let config = PostgresSourceConfig {
        host: host.to_string(),
        port,
        user: "postgres".into(),
        password: "postgres".to_string().into(),
        database: "cdc".into(),
        replication_slot_name: slot.into(),
        publication_name: "backlog_pub".into(),
        transport: rustcdc::TransportConfig::plaintext(),
        stream_poll_interval_ms: 10,
        max_events_per_poll: 500,
        wal_transport: transport,
        ..PostgresSourceConfig::default()
    };

    let mut source = PostgresConnection::new(config);
    source.connect().await?;
    let mut stream = source.start_stream(None).await?;

    // The rows this run captures, written after the slot is positioned.
    for id in 1..=ROWS {
        admin
            .execute(
                "INSERT INTO public.measured (payload) VALUES ($1)",
                &[&format!("row-{id}")],
            )
            .await
            .map_err(|error| rustcdc::Error::SourceError(rustcdc::render_error_chain(&error)))?;
    }

    let started = Instant::now();
    let deadline = started + Duration::from_secs(60);
    let mut events = 0usize;
    let mut polls = 0u32;

    while events < ROWS as usize && Instant::now() < deadline {
        let batch = stream.next_events(500).await?;
        polls += 1;
        let captured = batch
            .iter()
            .filter(|event| event.op == Operation::Insert && event.table == "measured")
            .count();
        events += captured;
        // Confirm as a real consumer would: this is what advances `confirmed_flush_lsn` and,
        // for the peek transport, what determines where emission starts on the next poll.
        if let Some(last) = batch.last() {
            if let Ok(lsn) = parse_lsn(&last.source.offset) {
                stream.confirm_lsn(lsn).await?;
            }
        }
    }

    let elapsed = started.elapsed();
    let restart_gap = restart_gap_bytes(admin, slot).await?;
    drop(stream);
    source.close().await;

    Ok(Measurement {
        transport: match transport {
            WalTransport::SqlPeek => "SqlPeek",
            _ => "StreamingReplication",
        },
        pinned,
        elapsed,
        polls,
        events,
        restart_gap_bytes: restart_gap,
    })
}

/// Write the WAL filler in bulk statements.
///
/// `generate_series` server-side rather than a client round trip per row: the point is WAL
/// volume, and 128 MiB one INSERT at a time would dominate the test's runtime without changing
/// what is being measured.
async fn write_backlog(client: &tokio_postgres::Client, payload: &str) -> rustcdc::Result<()> {
    for _ in 0..FILLER_BATCHES {
        client
            .execute(
                "INSERT INTO public.filler (payload) \
                 SELECT $1 FROM generate_series(1, $2::bigint)",
                &[&payload, &FILLER_ROWS_PER_BATCH],
            )
            .await
            .map_err(|error| rustcdc::Error::SourceError(rustcdc::render_error_chain(&error)))?;
    }
    Ok(())
}

fn parse_lsn(text: &str) -> rustcdc::Result<u64> {
    let (high, low) = text
        .split_once('/')
        .ok_or_else(|| rustcdc::Error::SourceError(format!("not an LSN: {text}")))?;
    let high = u64::from_str_radix(high, 16)
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;
    let low = u64::from_str_radix(low, 16)
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;
    Ok((high << 32) | low)
}

#[tokio::test]
async fn a_pinned_restart_lsn_is_what_separates_the_two_wal_transports() -> rustcdc::Result<()> {
    if std::env::var("CDC_RS_RUN_DOCKER_TESTS").as_deref() != Ok("1") {
        eprintln!("skipping postgres WAL backlog evidence (set CDC_RS_RUN_DOCKER_TESTS=1)");
        return Ok(());
    }

    let container = start_postgres().await?;
    let host = container
        .get_host()
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?
        .to_string();
    let port = container
        .get_host_port_ipv4(5432.tcp())
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;
    let dsn = format!(
        "host={host} port={port} user=postgres password=postgres dbname=cdc connect_timeout=30"
    );

    let admin = connect(&dsn).await?;
    admin
        .batch_execute(
            "
            CREATE TABLE public.measured (id BIGSERIAL PRIMARY KEY, payload TEXT NOT NULL);
            ALTER TABLE public.measured REPLICA IDENTITY FULL;
            -- Deliberately NOT in the publication: writes here generate WAL the decoder must
            -- read past but never emits, which is precisely the re-scan cost under test.
            --
            -- `STORAGE EXTERNAL` disables TOAST *compression*. Without it a repetitive payload
            -- compresses to almost nothing and the WAL volume — the whole independent variable
            -- — never materialises.
            CREATE TABLE public.filler (id BIGSERIAL PRIMARY KEY, payload TEXT NOT NULL);
            ALTER TABLE public.filler ALTER COLUMN payload SET STORAGE EXTERNAL;
            CREATE PUBLICATION backlog_pub FOR TABLE public.measured;
            ",
        )
        .await
        .map_err(|error| rustcdc::Error::SourceError(rustcdc::render_error_chain(&error)))?;

    let mut results = Vec::new();
    let payload = "x".repeat(PAYLOAD_BYTES);

    // Every slot is created **before** any long transaction is opened.
    // `pg_create_logical_replication_slot` must build a consistent snapshot, which waits for all
    // currently-running transactions to finish — with the holder below already open it would
    // block indefinitely. (Worth knowing operationally too: provisioning a slot on a busy
    // database waits on whatever long transaction happens to be in flight.)
    for slot in [
        "backlog_unpinned_0",
        "backlog_unpinned_1",
        "backlog_pinned_0",
        "backlog_pinned_1",
    ] {
        admin
            .execute(
                "SELECT pg_create_logical_replication_slot($1, 'pgoutput')",
                &[&slot],
            )
            .await
            .map_err(|error| rustcdc::Error::SourceError(rustcdc::render_error_chain(&error)))?;
    }

    let filler = connect(&dsn).await?;

    // ── Unpinned baseline: same WAL volume, restart_lsn free to advance ─────────────────
    write_backlog(&filler, &payload).await?;
    for (index, transport) in [WalTransport::StreamingReplication, WalTransport::SqlPeek]
        .into_iter()
        .enumerate()
    {
        let slot = format!("backlog_unpinned_{index}");
        results.push(measure(&admin, &host, port, &slot, transport, false).await?);
    }

    // ── Pinned: a held transaction stops restart_lsn advancing ──────────────────────────
    //
    // Order is the whole experiment. The holder opens **first**, pinning `restart_lsn` near the
    // current position; the filler WAL is written **after**, so it all lands beyond the pin.
    // From then on a peek must read from `restart_lsn` through every one of those records before
    // it can emit anything, on *every* poll. The production shape is a long-running report, a
    // slow migration, or an idle-in-transaction client.
    let holder = connect(&dsn).await?;
    holder
        .batch_execute("BEGIN; INSERT INTO public.filler (payload) VALUES ('holder');")
        .await
        .map_err(|error| rustcdc::Error::SourceError(rustcdc::render_error_chain(&error)))?;

    write_backlog(&filler, &payload).await?;

    for (index, transport) in [WalTransport::StreamingReplication, WalTransport::SqlPeek]
        .into_iter()
        .enumerate()
    {
        let slot = format!("backlog_pinned_{index}");
        results.push(measure(&admin, &host, port, &slot, transport, true).await?);
    }

    holder
        .batch_execute("ROLLBACK;")
        .await
        .map_err(|error| rustcdc::Error::SourceError(rustcdc::render_error_chain(&error)))?;

    // ── Report ───────────────────────────────────────────────────────────────
    println!("\n  WAL transport under a pinned restart_lsn ({ROWS} rows per run)");
    println!(
        "  {:<22} {:>8} {:>11} {:>7} {:>9} {:>14}",
        "transport", "pinned", "elapsed_ms", "polls", "events", "restart_gap"
    );
    for row in &results {
        println!(
            "  {:<22} {:>8} {:>11} {:>7} {:>9} {:>11} KiB",
            row.transport,
            row.pinned,
            row.elapsed.as_millis(),
            row.polls,
            row.events,
            row.restart_gap_bytes / 1024,
        );
    }

    let cell = |transport: &str, pinned: bool| {
        results
            .iter()
            .find(|row| row.transport == transport && row.pinned == pinned)
            .map(|row| row.elapsed.as_secs_f64())
            .expect("every cell was measured")
    };
    let streaming_ratio = cell("StreamingReplication", true) / cell("StreamingReplication", false);
    let peek_ratio = cell("SqlPeek", true) / cell("SqlPeek", false);
    println!(
        "\n  pinned/unpinned slowdown — streaming {streaming_ratio:.2}x, peek {peek_ratio:.2}x\n"
    );

    // The ratio is hardware-dependent, so it is reported rather than asserted — a flaky
    // performance gate teaches people to ignore failures. `SqlPeek` under a pinned restart_lsn
    // may legitimately fail to drain within the deadline; that *is* the finding, so it is
    // reported too. What must hold unconditionally is that the **default** transport captures
    // everything in both halves, which is the property an embedder relies on.
    for row in results.iter().filter(|row| row.transport == "StreamingReplication") {
        assert!(
            row.events >= ROWS as usize,
            "the default transport must capture every row regardless of restart_lsn; \
             pinned={} captured {} of {ROWS} in {:?}",
            row.pinned,
            row.events,
            row.elapsed
        );
    }
    for row in results.iter().filter(|row| row.transport == "SqlPeek") {
        if row.events < ROWS as usize {
            println!(
                "  NOTE: SqlPeek (pinned={}) drained only {} of {ROWS} rows before the deadline",
                row.pinned, row.events
            );
        }
    }

    Ok(())
}
