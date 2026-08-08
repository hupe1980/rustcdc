//! The two WAL transports must produce identical event streams.
//!
//! rustcdc reads the WAL either over `START_REPLICATION ... LOGICAL`
//! ([`WalTransport::StreamingReplication`], the default) or over
//! `pg_logical_slot_peek_binary_changes` ([`WalTransport::SqlPeek`], the fallback for
//! environments that cannot grant a replication connection). They share the pgoutput
//! decoder, event construction, table filtering and checkpointing — only the bytes' route
//! from the server differs.
//!
//! That sharing is the whole design, and this is what holds it honest: the same workload is
//! captured through both transports and the resulting canonical events are compared field
//! by field. A divergence means one transport is delivering something the other is not,
//! which is exactly the drift that makes a fallback path dangerous.
//!
//! Also covered here, because it is the part with no unit-testable surface:
//!
//! * **SCRAM-SHA-256** against a server configured to require it (PostgreSQL 16's default).
//! * **MD5**, against a server configured for it, since the SQL transport supports it and a
//!   streaming transport that could not would be a silent downgrade.
//! * **Resume from a checkpoint LSN**, which is where an incorrect `START_REPLICATION`
//!   start position shows up.

#![cfg(feature = "postgres")]

use rustcdc::{
    source::Source, Event, Operation, PostgresConnection, PostgresSourceConfig, WalTransport,
};
use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    ContainerAsync, GenericImage, ImageExt,
};

/// Start a logical-replication-capable PostgreSQL with a given password encryption.
async fn start_postgres(password_encryption: &str) -> rustcdc::Result<ContainerAsync<GenericImage>> {
    GenericImage::new("postgres", "16-alpine")
        .with_exposed_port(5432.tcp())
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_USER", "postgres")
        .with_env_var("POSTGRES_PASSWORD", "postgres")
        .with_env_var("POSTGRES_DB", "cdc")
        // `POSTGRES_INITDB_ARGS` is what decides the method baked into pg_hba.conf, so it
        // must be set at init time rather than with a later `ALTER SYSTEM`.
        .with_env_var(
            "POSTGRES_INITDB_ARGS",
            format!("--auth-host={password_encryption} --auth-local=trust"),
        )
        .with_env_var("POSTGRES_HOST_AUTH_METHOD", password_encryption)
        .with_cmd(vec![
            "postgres",
            "-c",
            "wal_level=logical",
            "-c",
            "max_replication_slots=16",
            "-c",
            "max_wal_senders=16",
            "-c",
            &format!("password_encryption={password_encryption}"),
        ])
        .start()
        .await
        .map_err(|error| rustcdc::Error::SourceError(rustcdc::render_error_chain(&error)))
}

async fn admin_client(
    container: &ContainerAsync<GenericImage>,
) -> rustcdc::Result<(tokio_postgres::Client, String, u16)> {
    let host = container
        .get_host()
        .await
        .map_err(|error| rustcdc::Error::SourceError(rustcdc::render_error_chain(&error)))?
        .to_string();
    let port = container
        .get_host_port_ipv4(5432.tcp())
        .await
        .map_err(|error| rustcdc::Error::SourceError(rustcdc::render_error_chain(&error)))?;

    let dsn = format!(
        "host={host} port={port} user=postgres password=postgres dbname=cdc connect_timeout=30"
    );
    let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
        .await
        .map_err(|error| rustcdc::Error::SourceError(rustcdc::render_error_chain(&error)))?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    Ok((client, host, port))
}

fn source_config(host: &str, port: u16, slot: &str, transport: WalTransport) -> PostgresSourceConfig {
    PostgresSourceConfig {
        host: host.to_string(),
        port,
        user: "postgres".into(),
        password: "postgres".to_string().into(),
        database: "cdc".into(),
        replication_slot_name: slot.into(),
        publication_name: "parity_pub".into(),
        // The container has no server certificate; TLS itself is covered by the
        // connection suites.
        transport: rustcdc::TransportConfig::plaintext(),
        stream_poll_interval_ms: 50,
        max_events_per_poll: 500,
        wal_transport: transport,
        ..PostgresSourceConfig::default()
    }
}

/// Create a logical replication slot.
///
/// Separate from the DDL batch on purpose: `batch_execute` runs its statements in one
/// implicit transaction, and PostgreSQL refuses to create a logical slot in a transaction
/// that has already written ("cannot create logical replication slot in transaction that
/// has performed writes").
async fn create_slot(admin: &tokio_postgres::Client, slot: &str) -> rustcdc::Result<()> {
    admin
        .execute(
            "SELECT pg_create_logical_replication_slot($1, 'pgoutput')",
            &[&slot],
        )
        .await
        .map_err(|error| rustcdc::Error::SourceError(rustcdc::render_error_chain(&error)))?;
    Ok(())
}

/// The fields that define a captured change, for comparison across transports.
///
/// Deliberately excludes wall-clock `ts` and the transaction id, which are not expected to
/// be byte-identical between two independent captures of the same workload.
#[derive(Debug, PartialEq, Eq)]
struct Captured {
    op: Operation,
    schema: Option<String>,
    table: String,
    before: Option<String>,
    after: Option<String>,
    primary_key: Option<Vec<String>>,
    offset: String,
}

fn capture(event: &Event) -> Captured {
    Captured {
        op: event.op,
        schema: event.schema.clone(),
        table: event.table.clone(),
        before: event.before.as_ref().map(ToString::to_string),
        after: event.after.as_ref().map(ToString::to_string),
        primary_key: event.primary_key.clone(),
        // The LSN must agree too: both transports report the change's own WAL position, and
        // a mismatch would mean the checkpoints they write are not interchangeable.
        offset: event.source.offset.clone(),
    }
}

/// Drain a stream until it has produced **exactly** `want` events, or the deadline passes.
///
/// Waiting for a known count rather than for the stream to go quiet is what makes the parity
/// comparison deterministic. Two independently drained streams progress at different rates —
/// especially with several container-backed suites competing for the machine — so a
/// quiet-based drain can return 6 events from one transport and 4 from the other and report a
/// divergence that is really just a slower reader.
async fn drain(
    stream: &mut Box<dyn rustcdc::source::StreamHandle>,
    want: usize,
    label: &str,
) -> rustcdc::Result<Vec<Event>> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    let mut collected = Vec::new();
    while collected.len() < want && std::time::Instant::now() < deadline {
        collected.extend(stream.next_events(500).await?);
    }
    if collected.len() < want {
        return Err(rustcdc::Error::TimeoutError(format!(
            "the {label} transport produced {} of {want} expected events within 60s",
            collected.len()
        )));
    }
    Ok(collected)
}

/// The workload both transports capture. Covers every operation the decoder handles.
async fn apply_workload(admin: &tokio_postgres::Client) -> rustcdc::Result<()> {
    admin
        .batch_execute(
            "
            BEGIN;
            INSERT INTO public.parity (id, name, balance)
              VALUES (1, 'alice', 100), (2, 'bob', 200);
            COMMIT;
            UPDATE public.parity SET balance = 150 WHERE id = 1;
            DELETE FROM public.parity WHERE id = 2;
            BEGIN;
            INSERT INTO public.parity (id, name, balance) VALUES (3, 'carol', 300);
            UPDATE public.parity SET name = 'carol-2' WHERE id = 3;
            COMMIT;
            ",
        )
        .await
        .map_err(|error| rustcdc::Error::SourceError(rustcdc::render_error_chain(&error)))?;
    Ok(())
}

#[tokio::test]
async fn both_wal_transports_capture_byte_identical_event_streams() -> rustcdc::Result<()> {
    if std::env::var("CDC_RS_RUN_DOCKER_TESTS").as_deref() != Ok("1") {
        eprintln!("skipping postgres WAL transport parity test (set CDC_RS_RUN_DOCKER_TESTS=1)");
        return Ok(());
    }

    // scram-sha-256 is PostgreSQL 16's default, so this also exercises the SCRAM exchange.
    let container = start_postgres("scram-sha-256").await?;
    let (admin, host, port) = admin_client(&container).await?;

    admin
        .batch_execute(
            "
            CREATE TABLE public.parity (id BIGINT PRIMARY KEY, name TEXT, balance BIGINT);
            ALTER TABLE public.parity REPLICA IDENTITY FULL;
            CREATE PUBLICATION parity_pub FOR TABLE public.parity;
            ",
        )
        .await
        .map_err(|error| rustcdc::Error::SourceError(rustcdc::render_error_chain(&error)))?;

    // Both slots are created before the workload, so both see exactly the same WAL.
    create_slot(&admin, "slot_streaming").await?;
    create_slot(&admin, "slot_peek").await?;

    let mut streaming_source = PostgresConnection::new(source_config(
        &host,
        port,
        "slot_streaming",
        WalTransport::StreamingReplication,
    ));
    streaming_source.connect().await?;
    let mut streaming = streaming_source.start_stream(None).await?;

    let mut peek_source =
        PostgresConnection::new(source_config(&host, port, "slot_peek", WalTransport::SqlPeek));
    peek_source.connect().await?;
    let mut peek = peek_source.start_stream(None).await?;

    apply_workload(&admin).await?;

    // The workload commits six changes: two inserts, an update, a delete, then an insert and
    // an update in one transaction.
    const EXPECTED_EVENTS: usize = 6;
    let streaming_events = drain(&mut streaming, EXPECTED_EVENTS, "streaming").await?;
    let peek_events = drain(&mut peek, EXPECTED_EVENTS, "peek").await?;

    let streaming_captured: Vec<Captured> = streaming_events.iter().map(capture).collect();
    let peek_captured: Vec<Captured> = peek_events.iter().map(capture).collect();

    assert!(
        !streaming_captured.is_empty(),
        "the streaming transport captured nothing; START_REPLICATION or the CopyBoth loop \
         is not delivering"
    );
    assert_eq!(
        streaming_captured, peek_captured,
        "the two WAL transports must decode the same WAL into the same events. A \
         difference means the shared decoder is being fed differently by one of them, \
         which makes their checkpoints non-interchangeable.\n\
         streaming: {streaming_captured:#?}\npeek: {peek_captured:#?}"
    );

    // Sanity-check the workload actually exercised each operation, so a silently empty
    // capture cannot pass the equality assertion above.
    let ops: Vec<Operation> = streaming_captured.iter().map(|event| event.op).collect();
    for expected in [Operation::Insert, Operation::Update, Operation::Delete] {
        assert!(
            ops.contains(&expected),
            "the workload must produce a {expected:?} event; got {ops:?}"
        );
    }

    Ok(())
}

#[tokio::test]
async fn the_streaming_transport_resumes_from_a_checkpoint_lsn() -> rustcdc::Result<()> {
    if std::env::var("CDC_RS_RUN_DOCKER_TESTS").as_deref() != Ok("1") {
        eprintln!("skipping postgres streaming resume test (set CDC_RS_RUN_DOCKER_TESTS=1)");
        return Ok(());
    }

    let container = start_postgres("scram-sha-256").await?;
    let (admin, host, port) = admin_client(&container).await?;

    admin
        .batch_execute(
            "
            CREATE TABLE public.parity (id BIGINT PRIMARY KEY, name TEXT, balance BIGINT);
            ALTER TABLE public.parity REPLICA IDENTITY FULL;
            CREATE PUBLICATION parity_pub FOR TABLE public.parity;
            ",
        )
        .await
        .map_err(|error| rustcdc::Error::SourceError(rustcdc::render_error_chain(&error)))?;
    create_slot(&admin, "slot_resume").await?;

    let config = source_config(&host, port, "slot_resume", WalTransport::StreamingReplication);

    let mut first_source = PostgresConnection::new(config.clone());
    first_source.connect().await?;
    let mut first = first_source.start_stream(None).await?;

    admin
        .batch_execute("INSERT INTO public.parity (id, name, balance) VALUES (1, 'before', 1)")
        .await
        .map_err(|error| rustcdc::Error::SourceError(rustcdc::render_error_chain(&error)))?;

    let before_restart = drain(&mut first, 1, "streaming").await?;
    assert!(
        !before_restart.is_empty(),
        "the first stream must capture the pre-restart change"
    );

    // Confirm and checkpoint exactly as the runtime would.
    let mut checkpoint = rustcdc::checkpoint::InMemoryCheckpoint::default();
    first.save_position(&mut checkpoint).await?;
    let resume = {
        use rustcdc::checkpoint::Checkpoint;
        checkpoint
            .load()
            .await?
            .ok_or_else(|| rustcdc::Error::CheckpointError("no checkpoint written".into()))?
    };
    // Drop the handle *before* reconnecting: it owns the replication socket, and until it
    // is gone the server still counts a walsender as active on the slot. A second
    // START_REPLICATION on an active slot is refused.
    drop(first);
    first_source.close().await;
    // Give the server a moment to reap the walsender.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    admin
        .batch_execute("INSERT INTO public.parity (id, name, balance) VALUES (2, 'after', 2)")
        .await
        .map_err(|error| rustcdc::Error::SourceError(rustcdc::render_error_chain(&error)))?;

    let mut resumed_source = PostgresConnection::new(config);
    resumed_source.connect().await?;
    let mut resumed = resumed_source.start_stream(Some(resume.as_ref())).await?;

    let after_restart = drain(&mut resumed, 1, "resumed").await?;
    let names: Vec<String> = after_restart
        .iter()
        .filter_map(|event| {
            event
                .after
                .as_ref()?
                .get("name")?
                .as_str()
                .map(ToString::to_string)
        })
        .collect();
    assert!(
        names.iter().any(|name| name == "after"),
        "a stream resumed from the checkpoint LSN must deliver the change written after it; \
         got {names:?}"
    );

    Ok(())
}

#[tokio::test]
async fn the_streaming_transport_authenticates_with_md5() -> rustcdc::Result<()> {
    if std::env::var("CDC_RS_RUN_DOCKER_TESTS").as_deref() != Ok("1") {
        eprintln!("skipping postgres md5 auth test (set CDC_RS_RUN_DOCKER_TESTS=1)");
        return Ok(());
    }

    // PostgreSQL deprecates MD5, but plenty of running servers are still configured for it,
    // and the SQL transport authenticates against them. A streaming transport that could
    // not would be a silent downgrade discovered only in production.
    let container = start_postgres("md5").await?;
    let (admin, host, port) = admin_client(&container).await?;

    admin
        .batch_execute(
            "
            CREATE TABLE public.parity (id BIGINT PRIMARY KEY, name TEXT, balance BIGINT);
            ALTER TABLE public.parity REPLICA IDENTITY FULL;
            CREATE PUBLICATION parity_pub FOR TABLE public.parity;
            ",
        )
        .await
        .map_err(|error| rustcdc::Error::SourceError(rustcdc::render_error_chain(&error)))?;
    create_slot(&admin, "slot_md5").await?;

    let mut source = PostgresConnection::new(source_config(
        &host,
        port,
        "slot_md5",
        WalTransport::StreamingReplication,
    ));
    source.connect().await?;
    let mut stream = source.start_stream(None).await?;

    admin
        .batch_execute("INSERT INTO public.parity (id, name, balance) VALUES (1, 'md5', 1)")
        .await
        .map_err(|error| rustcdc::Error::SourceError(rustcdc::render_error_chain(&error)))?;

    let events = drain(&mut stream, 1, "streaming/md5").await?;
    assert!(
        events.iter().any(|event| event.op == Operation::Insert),
        "the streaming transport must capture changes on an md5-authenticated server"
    );

    Ok(())
}
