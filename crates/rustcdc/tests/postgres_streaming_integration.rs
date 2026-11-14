//! Large transactions streamed before their commit (pgoutput v2).
//!
//! The server streams a transaction once it passes `logical_decoding_work_mem`; the container
//! sets that to its 64 kB minimum so a few thousand rows are enough. Two things are pinned:
//!
//! * **Nothing uncommitted leaves rustcdc, and what does is what v1 delivers.** The streaming
//!   transport (v2) and `sql_peek` (always v1) capture the same workload, including a savepoint
//!   rolled back mid-stream and a streamed transaction that aborts, and must agree field by
//!   field. The slots' statistics prove the server really streamed one and spilled the other,
//!   so the comparison cannot pass on a workload too small to exercise anything.
//! * **A held-open transaction is visible, spills past the memory budget, and is delivered
//!   whole at its commit.**

#![cfg(feature = "postgres")]

#[path = "support/matrix.rs"]
mod matrix;

use std::time::{Duration, Instant};

use rustcdc::{Event, PostgresConnection, PostgresSourceConfig, WalTransport, source::Source};
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};

fn docker_tests_enabled(name: &str) -> bool {
    if std::env::var("CDC_RS_RUN_DOCKER_TESTS").as_deref() == Ok("1") {
        return true;
    }
    eprintln!("skipping {name} (set CDC_RS_RUN_DOCKER_TESTS=1)");
    false
}

fn err(error: impl std::fmt::Display) -> rustcdc::Error {
    rustcdc::Error::SourceError(error.to_string())
}

async fn start_postgres() -> rustcdc::Result<ContainerAsync<GenericImage>> {
    GenericImage::new("postgres", matrix::full(matrix::Engine::Postgres))
        .with_exposed_port(5432.tcp())
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
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
            "-c",
            "logical_decoding_work_mem=64kB",
        ])
        .start()
        .await
        .map_err(|error| rustcdc::Error::SourceError(rustcdc::render_error_chain(&error)))
}

async fn client(host: &str, port: u16) -> rustcdc::Result<tokio_postgres::Client> {
    let dsn = format!("host={host} port={port} user=postgres password=postgres dbname=cdc");
    let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
        .await
        .map_err(err)?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    Ok(client)
}

async fn endpoint(container: &ContainerAsync<GenericImage>) -> rustcdc::Result<(String, u16)> {
    let host = container.get_host().await.map_err(err)?.to_string();
    let port = container
        .get_host_port_ipv4(5432.tcp())
        .await
        .map_err(err)?;
    Ok((host, port))
}

fn config(host: &str, port: u16, slot: &str, transport: WalTransport) -> PostgresSourceConfig {
    PostgresSourceConfig {
        host: host.to_string(),
        port,
        user: "postgres".into(),
        password: "postgres".to_string().into(),
        database: "cdc".into(),
        replication_slot_name: slot.into(),
        publication_name: "streamed_pub".into(),
        transport: rustcdc::TransportConfig::plaintext(),
        stream_poll_interval_ms: 50,
        max_events_per_poll: 1000,
        wal_transport: transport,
        ..PostgresSourceConfig::default()
    }
}

async fn setup(admin: &tokio_postgres::Client, slots: &[&str]) -> rustcdc::Result<()> {
    admin
        .batch_execute(
            "CREATE TABLE public.streamed (id BIGINT PRIMARY KEY, v TEXT);
             CREATE PUBLICATION streamed_pub FOR TABLE public.streamed;",
        )
        .await
        .map_err(err)?;
    for slot in slots {
        admin
            .execute(
                "SELECT pg_create_logical_replication_slot($1, 'pgoutput')",
                &[slot],
            )
            .await
            .map_err(err)?;
    }
    Ok(())
}

/// Read exactly `want` rows, confirming each batch as a consumer would: `sql_peek` re-reads
/// from the confirmed position. A workload ends with a marker row, so a stray row shows up as
/// the marker not being last.
async fn drain(
    stream: &mut Box<dyn rustcdc::source::StreamHandle>,
    want: usize,
    label: &str,
) -> rustcdc::Result<Vec<Event>> {
    let deadline = Instant::now() + Duration::from_secs(120);
    let mut rows = Vec::new();
    while rows.len() < want && Instant::now() < deadline {
        let batch = stream.next_events(500).await?;
        // A transaction's end, as the runtime confirms it: a change's own LSN is mid-transaction.
        if let Some(lsn) = batch
            .last()
            .and_then(|event| stream.resume_offset_for(event))
            .and_then(|offset| parse_lsn(&offset))
        {
            stream.confirm_lsn(lsn).await?;
        }
        rows.extend(
            batch
                .into_iter()
                .filter(|event| !event.op.is_schema_change()),
        );
    }
    if rows.len() < want {
        return Err(rustcdc::Error::TimeoutError(format!(
            "the {label} capture produced {} of {want} rows",
            rows.len()
        )));
    }
    rows.truncate(want);
    Ok(rows)
}

fn parse_lsn(text: &str) -> Option<u64> {
    let (high, low) = text.split_once('/')?;
    Some((u64::from_str_radix(high, 16).ok()? << 32) | u64::from_str_radix(low, 16).ok()?)
}

/// `(stream_txns, spill_txns)` for a slot, once the server has reported any decoding at all.
async fn slot_stats(admin: &tokio_postgres::Client, slot: &str) -> rustcdc::Result<(i64, i64)> {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let row = admin
            .query_opt(
                "SELECT stream_txns, spill_txns FROM pg_stat_replication_slots WHERE slot_name = $1",
                &[&slot],
            )
            .await
            .map_err(err)?;
        let stats = row.map(|row| (row.get::<_, i64>(0), row.get::<_, i64>(1)));
        match stats {
            Some((streamed, spilled)) if streamed + spilled > 0 => return Ok((streamed, spilled)),
            _ if Instant::now() > deadline => {
                return Err(err(format!(
                    "{slot}: the server reported neither streaming nor spilling; the workload \
                     never passed logical_decoding_work_mem"
                )));
            }
            _ => tokio::time::sleep(Duration::from_millis(250)).await,
        }
    }
}

fn key(event: &Event) -> i64 {
    event
        .after
        .as_ref()
        .or(event.before.row())
        .and_then(|row| row.get("id"))
        .and_then(|id| id.as_str().and_then(|s| s.parse().ok()).or(id.as_i64()))
        .expect("every row carries its id")
}

fn captured(event: &Event) -> (rustcdc::Operation, String, Option<String>, String) {
    (
        event.op,
        event.table.clone(),
        event.after.as_ref().map(ToString::to_string),
        event.source.offset.clone(),
    )
}

#[tokio::test]
async fn a_streamed_transaction_releases_only_committed_rows_as_v1_would() -> rustcdc::Result<()> {
    if !docker_tests_enabled("postgres streaming parity") {
        return Ok(());
    }
    let container = start_postgres().await?;
    let (host, port) = endpoint(&container).await?;
    let admin = client(&host, port).await?;
    setup(&admin, &["slot_v2", "slot_v1"]).await?;

    let mut v2_source = PostgresConnection::new(config(
        &host,
        port,
        "slot_v2",
        WalTransport::StreamingReplication,
    ));
    v2_source.connect().await?;
    let mut v2 = v2_source.start_stream(None).await?;
    let mut v1_source =
        PostgresConnection::new(config(&host, port, "slot_v1", WalTransport::SqlPeek));
    v1_source.connect().await?;
    let mut v1 = v1_source.start_stream(None).await?;

    // Every part is past 64 kB, so the server streams it before what settles it: the savepoint
    // rollback and the abort arrive as Stream Abort, not as changes never sent.
    admin
        .batch_execute(
            "BEGIN;
             INSERT INTO public.streamed SELECT g, repeat('k', 200) FROM generate_series(1, 2000) g;
             SAVEPOINT s1;
             INSERT INTO public.streamed SELECT g, repeat('r', 200) FROM generate_series(100001, 102000) g;
             ROLLBACK TO SAVEPOINT s1;
             UPDATE public.streamed SET v = 'updated' WHERE id <= 10;
             COMMIT;
             BEGIN;
             INSERT INTO public.streamed SELECT g, repeat('a', 200) FROM generate_series(200001, 202000) g;
             ROLLBACK;
             INSERT INTO public.streamed VALUES (999999, 'after');",
        )
        .await
        .map_err(err)?;

    const COMMITTED: usize = 2000 + 10 + 1;
    let v2_rows = drain(&mut v2, COMMITTED, "v2 streaming").await?;
    let v1_rows = drain(&mut v1, COMMITTED, "v1 sql_peek").await?;

    assert_eq!(
        v2_rows.last().map(key),
        Some(999_999),
        "the marker must be the last of exactly the committed rows"
    );
    let rolled_back: Vec<i64> = v2_rows
        .iter()
        .map(key)
        .filter(|id| (100_001..=102_000).contains(id) || (200_001..=202_000).contains(id))
        .collect();
    assert!(
        rolled_back.is_empty(),
        "rows PostgreSQL rolled back reached the stream: {:?}…",
        &rolled_back[..rolled_back.len().min(5)]
    );
    // A streamed transaction must reach the sink exactly as v1 delivers it.
    if let Some((index, (streamed, unstreamed))) = v2_rows
        .iter()
        .map(captured)
        .zip(v1_rows.iter().map(captured))
        .enumerate()
        .find(|(_, (streamed, unstreamed))| streamed != unstreamed)
    {
        panic!("row {index} differs:\n  v2: {streamed:?}\n  v1: {unstreamed:?}");
    }

    let (v2_streamed, v2_spilled) = slot_stats(&admin, "slot_v2").await?;
    let (_, v1_spilled) = slot_stats(&admin, "slot_v1").await?;
    // The aborted transaction may not stream at all: a server that has already decoded its
    // abort skips it. The committed one cannot be skipped.
    assert!(
        v2_streamed >= 1,
        "v2 must have streamed (streamed {v2_streamed}, spilled {v2_spilled})"
    );
    assert_eq!(
        v2_spilled, 0,
        "streaming is what spares the primary's disk (streamed {v2_streamed})"
    );
    assert!(
        v1_spilled > 0,
        "v1 must have spilled the same workload on the primary"
    );
    Ok(())
}

#[tokio::test]
async fn a_held_open_transaction_is_visible_spills_and_arrives_whole() -> rustcdc::Result<()> {
    if !docker_tests_enabled("postgres streaming spill") {
        return Ok(());
    }
    let container = start_postgres().await?;
    let (host, port) = endpoint(&container).await?;
    let admin = client(&host, port).await?;
    setup(&admin, &["slot_spill"]).await?;

    let spill_directory = tempfile::tempdir().map_err(err)?;
    let mut source_config = config(
        &host,
        port,
        "slot_spill",
        WalTransport::StreamingReplication,
    );
    source_config.stream_buffer_memory_bytes = 16 * 1024;
    source_config.stream_spill_directory = Some(spill_directory.path().to_path_buf());
    let mut source = PostgresConnection::new(source_config);
    source.connect().await?;
    let mut stream = source.start_stream(None).await?;

    let writer = client(&host, port).await?;
    writer
        .batch_execute(
            "BEGIN;
             INSERT INTO public.streamed SELECT g, repeat('h', 200) FROM generate_series(1, 3000) g;",
        )
        .await
        .map_err(err)?;

    let deadline = Instant::now() + Duration::from_secs(60);
    let open = loop {
        let events = stream.next_events(250).await?;
        assert!(
            events.iter().all(|event| event.op.is_schema_change()),
            "an uncommitted row was released"
        );
        if let Some(open) = stream
            .streamed_transactions()
            .and_then(|streamed| streamed.largest)
            .filter(|open| open.spilled)
        {
            break open;
        }
        assert!(
            Instant::now() < deadline,
            "the open transaction never showed as spilled: {:?}",
            stream.streamed_transactions()
        );
    };
    assert!(open.changes > 0 && open.bytes > 16 * 1024, "{open:?}");

    writer.batch_execute("COMMIT").await.map_err(err)?;
    let rows = drain(&mut stream, 3000, "spilled").await?;
    let mut ids: Vec<i64> = rows.iter().map(key).collect();
    ids.sort_unstable();
    assert_eq!(ids, (1..=3000).collect::<Vec<_>>(), "every row once");
    assert!(
        stream
            .streamed_transactions()
            .is_some_and(|streamed| streamed.open == 0 && streamed.largest.is_none()),
        "{:?}",
        stream.streamed_transactions()
    );
    Ok(())
}
