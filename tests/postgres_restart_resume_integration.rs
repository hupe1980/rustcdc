//! A clean restart with no new writes must deliver nothing.
//!
//! Regression coverage for the resume coordinate. PostgreSQL logical decoding filters at
//! **transaction** granularity: `START_REPLICATION ... X` re-sends every transaction whose
//! commit record sits at or after `X`. A change's own LSN always precedes its transaction's
//! commit record, so checkpointing one replayed that entire transaction on every restart —
//! deterministically, with no writes on the source at all.
//!
//! Nudging the LSN forward does not fix it; the commit record is still ahead of `X + 1`.
//! The resume position has to be the one *after* the commit record, which pgoutput reports
//! as the COMMIT message's `end_lsn` and the connector returns from
//! `StreamHandle::resume_offset_for`.
//!
//! Both transports are covered because they failed differently: streaming replication
//! replayed the transaction once per restart, while `SqlPeek` re-emitted it on **every
//! poll** — the peek is non-consuming and has no client-side position filter, so the replay
//! was unbounded rather than a single duplicate.
#![cfg(feature = "postgres")]

use rustcdc::source::Source;
use rustcdc::{checkpoint::PostgresOffset, PostgresConnection, PostgresSourceConfig, WalTransport};
use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    GenericImage, ImageExt,
};

async fn run(transport: WalTransport, slot: &str) -> rustcdc::Result<usize> {
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
        .map_err(|e| rustcdc::Error::SourceError(e.to_string()))?;

    let host = container
        .get_host()
        .await
        .map_err(|e| rustcdc::Error::SourceError(e.to_string()))?;
    let port = container
        .get_host_port_ipv4(5432.tcp())
        .await
        .map_err(|e| rustcdc::Error::SourceError(e.to_string()))?;

    let dsn = format!(
        "host={host} port={port} user=postgres password=postgres dbname=cdc connect_timeout=30"
    );
    let (admin, conn) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
        .await
        .map_err(|e| rustcdc::Error::SourceError(e.to_string()))?;
    tokio::spawn(async move {
        let _ = conn.await;
    });

    admin
        .batch_execute(
            "CREATE TABLE public.t (id BIGINT PRIMARY KEY, data TEXT);
             ALTER TABLE public.t REPLICA IDENTITY FULL;
             CREATE PUBLICATION p FOR TABLE public.t;",
        )
        .await
        .map_err(|e| rustcdc::Error::SourceError(e.to_string()))?;

    let cfg = PostgresSourceConfig {
        host: host.to_string(),
        port,
        user: "postgres".into(),
        password: "postgres".to_string().into(),
        database: "cdc".into(),
        replication_slot_name: slot.to_string(),
        publication_name: "p".into(),
        create_replication_slot_if_missing: true,
        conn_timeout_secs: 30,
        stream_poll_interval_ms: 50,
        max_events_per_poll: 1_000,
        wal_transport: transport,
        transport: rustcdc::TransportConfig::plaintext(),
        ..PostgresSourceConfig::default()
    };

    // ── Session 1: stream one insert, confirm it, shut down cleanly ───────────
    let mut connection = PostgresConnection::new(cfg.clone());
    connection.connect().await?;
    let mut stream = connection.start_stream(None).await?;

    admin
        .execute("INSERT INTO public.t (id, data) VALUES (1, 'one')", &[])
        .await
        .map_err(|e| rustcdc::Error::SourceError(e.to_string()))?;

    let mut delivered = Vec::new();
    for _ in 0..60 {
        let events = stream.next_events(250).await?;
        delivered.extend(events);
        if !delivered.is_empty() {
            break;
        }
    }
    assert_eq!(delivered.len(), 1, "session 1 must see exactly the insert");
    let change_offset = delivered[0].source.offset.clone();
    // What the runtime checkpoints: the transaction's end position, not the change's own
    // LSN. `resume_offset_for` is the connector's answer to "where does a restart resume?".
    let resume_offset = stream
        .resume_offset_for(&delivered[0])
        .expect("postgres reports a resume position for a delivered event");
    let lsn = u64::from_str_radix(&resume_offset.replace('/', ""), 16).expect("lsn parses");
    println!("session 1 change={change_offset} resume={resume_offset} lsn={lsn}");
    assert_ne!(
        change_offset, resume_offset,
        "the resume position must be past the commit record, not the change's own LSN"
    );

    // What the runtime does on commit_ack.
    stream.confirm_lsn(lsn).await?;

    // Idle, then clean shutdown.
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    drop(stream);
    connection.close().await;

    // ── Session 2: resume from that checkpoint, no new DML ────────────────────
    let mut connection = PostgresConnection::new(cfg);
    connection.connect().await?;
    let offset = PostgresOffset::new(lsn, slot.to_string());
    let mut stream = connection.start_stream(Some(&offset)).await?;

    let mut replayed = Vec::new();
    for _ in 0..20 {
        let events = stream.next_events(250).await?;
        replayed.extend(events);
    }
    for event in &replayed {
        println!("REPLAYED offset={} op={}", event.source.offset, event.op);
    }
    drop(stream);
    connection.close().await;

    Ok(replayed.len())
}

#[tokio::test(flavor = "multi_thread")]
async fn streaming_replication_restart_delivers_nothing_new() -> rustcdc::Result<()> {
    if std::env::var("CDC_RS_RUN_DOCKER_TESTS").as_deref() != Ok("1") {
        return Ok(());
    }
    let n = run(WalTransport::StreamingReplication, "resume_stream").await?;
    assert_eq!(n, 0, "a clean restart with no new writes must deliver nothing; the resume position must sit past the last delivered transaction's commit record");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn sql_peek_restart_delivers_nothing_new() -> rustcdc::Result<()> {
    if std::env::var("CDC_RS_RUN_DOCKER_TESTS").as_deref() != Ok("1") {
        return Ok(());
    }
    let n = run(WalTransport::SqlPeek, "resume_peek").await?;
    assert_eq!(n, 0, "a clean restart with no new writes must deliver nothing; the resume position must sit past the last delivered transaction's commit record");
    Ok(())
}
