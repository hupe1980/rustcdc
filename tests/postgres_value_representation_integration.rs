//! The same column must have the same JSON type whichever path it arrived by.
//!
//! This is the test that would have caught the defect it exists for. A row backfilled by a
//! snapshot gave `{"id": 1}` while the same row updated a moment later gave `{"id": "1"}`,
//! because the chunk read went through `row_to_json` (which preserves SQL types) and the
//! live stream came from pgoutput (which is text). A sink reaching for `as_i64()` read one
//! and silently saw `None` for the other.
//!
//! Nothing asserted cross-path consistency, so nothing objected — the per-path type-fidelity
//! suites each checked their own path and agreed with themselves.
//!
//! The contract is now: **every scalar column value is a JSON string, on every capture path
//! and every connector; SQL `NULL` is JSON `null`.** Text is the lossless form —
//! `numeric(38,4)` and `int8` above 2^53 do not survive a JSON number, which becomes an
//! IEEE-754 double in most consumers.

#![cfg(feature = "postgres")]

use rustcdc::{
    checkpoint::FileCheckpoint, schema_history::InMemorySchemaHistory, IncrementalSnapshotConfig,
    Operation, PostgresSourceConfig, RuntimeConfig, RuntimeSourceConfig,
};
use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    GenericImage, ImageExt,
};

const SLOT: &str = "value_repr_slot";

/// Columns chosen so a JSON-number representation would visibly corrupt them.
const CREATE_TABLE: &str = "CREATE TABLE public.repr (
     id BIGINT PRIMARY KEY,
     huge BIGINT NOT NULL,
     exact NUMERIC(38, 4) NOT NULL,
     ratio DOUBLE PRECISION NOT NULL,
     flag BOOLEAN NOT NULL,
     label TEXT,
     absent TEXT
 );
 ALTER TABLE public.repr REPLICA IDENTITY FULL;
 CREATE PUBLICATION repr_pub FOR TABLE public.repr;";

#[tokio::test(flavor = "multi_thread")]
async fn snapshot_and_stream_agree_on_the_json_type_of_every_column() -> rustcdc::Result<()> {
    if std::env::var("CDC_RS_RUN_DOCKER_TESTS").as_deref() != Ok("1") {
        eprintln!("skipping value representation test (set CDC_RS_RUN_DOCKER_TESTS=1)");
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
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?
        .to_string();
    let port = container
        .get_host_port_ipv4(5432.tcp())
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

    let dsn = format!(
        "host={host} port={port} user=postgres password=postgres dbname=cdc connect_timeout=30"
    );
    let (admin, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;
    tokio::spawn(async move {
        let _ = connection.await;
    });

    admin
        .batch_execute(CREATE_TABLE)
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

    // Row 1 exists before the slot, so the snapshot reads it.
    admin
        .batch_execute(
            "INSERT INTO public.repr VALUES
             (1, 9223372036854775807, 12345678901234.5678, 0.1, true, 'snapshot', NULL);",
        )
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

    admin
        .execute(
            "SELECT pg_create_logical_replication_slot($1, 'pgoutput')",
            &[&SLOT],
        )
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

    let checkpoint_dir = tempfile::tempdir().map_err(rustcdc::Error::IoError)?;
    let mut runtime = rustcdc::CdcRuntime::new(
        RuntimeConfig::new(
            RuntimeSourceConfig::Postgres(PostgresSourceConfig {
                host: host.clone(),
                port,
                user: "postgres".into(),
                password: "postgres".to_string().into(),
                database: "cdc".into(),
                replication_slot_name: SLOT.into(),
                publication_name: "repr_pub".into(),
                transport: rustcdc::TransportConfig::plaintext(),
                stream_poll_interval_ms: 50,
                max_events_per_poll: 200,
                ..PostgresSourceConfig::default()
            }),
            FileCheckpoint::new(checkpoint_dir.path()),
            InMemorySchemaHistory::default(),
        )
        .with_incremental_snapshot(IncrementalSnapshotConfig::new(vec![
            "public.repr".to_string()
        ]))
        .with_max_buffer_size(500)
        .with_max_poll_wait_ms(300),
    )?;

    runtime.start().await?;

    // Row 2 lands after the stream is open, so the stream carries it. Identical values, so
    // any difference between the two events is a representation difference and nothing else.
    admin
        .batch_execute(
            "INSERT INTO public.repr VALUES
             (2, 9223372036854775807, 12345678901234.5678, 0.1, true, 'stream', NULL);",
        )
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

    let mut from_snapshot: Option<serde_json::Value> = None;
    let mut from_stream: Option<serde_json::Value> = None;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(90);

    while std::time::Instant::now() < deadline && (from_snapshot.is_none() || from_stream.is_none())
    {
        let batch = runtime.poll_event_batch().await?;
        if batch.is_empty() {
            continue;
        }
        for event in batch.events() {
            let Some(after) = event.after.as_ref() else {
                continue;
            };
            match event.op {
                Operation::Read => from_snapshot = Some(after.clone()),
                Operation::Insert => from_stream = Some(after.clone()),
                _ => {}
            }
        }
        runtime.commit_ack(batch.ack_mode()).await?;
    }

    let snapshot = from_snapshot.expect("the snapshot delivered row 1");
    let stream = from_stream.expect("the stream delivered row 2");

    // ── The contract: every scalar is a JSON string ───────────────────────────
    //
    // Asserted separately from the agreement check below, because two paths that *agree* on
    // emitting JSON numbers would satisfy that check while breaking the rule the whole
    // envelope rests on: a JSON number is an IEEE-754 double downstream, so `numeric(38,4)`
    // and `bigint` past 2^53 do not survive one. Nothing pinned this before, so a regression
    // to numbers would have passed.
    for column in ["id", "huge", "exact", "ratio", "flag", "label"] {
        for (path, row) in [("snapshot", &snapshot), ("stream", &stream)] {
            let kind = json_kind(&row[column]);
            assert_eq!(
                kind, "string",
                "column '{column}' arrived from the {path} as a JSON {kind}. Every scalar must                  be a string: a JSON number is a double by the time a consumer sees it.
                   value: {:?}",
                row[column],
            );
        }
    }

    // ── The property: same JSON type, column by column ───────────────────────
    for column in ["id", "huge", "exact", "ratio", "flag", "label", "absent"] {
        let snapshot_kind = json_kind(&snapshot[column]);
        let stream_kind = json_kind(&stream[column]);
        assert_eq!(
            snapshot_kind, stream_kind,
            "column '{column}' has JSON type {snapshot_kind} from the snapshot and \
             {stream_kind} from the stream. A sink cannot read both without branching on \
             which path the row arrived by.\n  snapshot: {:?}\n  stream:   {:?}",
            snapshot[column], stream[column],
        );
    }

    // ── And the representation is the lossless one ───────────────────────────
    for row in [&snapshot, &stream] {
        assert_eq!(
            row["huge"],
            serde_json::json!("9223372036854775807"),
            "a bigint above 2^53 must survive exactly; a JSON number would not",
        );
        assert_eq!(
            row["exact"],
            serde_json::json!("12345678901234.5678"),
            "numeric(38,4) must survive exactly; a JSON number becomes an f64",
        );
        // PostgreSQL's own text form for a boolean, which is what pgoutput emits because it
        // calls the type's output function. The snapshot casts each column with `::text`,
        // which invokes exactly the same function — so the two agree character for
        // character rather than merely agreeing on the JSON type.
        assert_eq!(row["flag"], serde_json::json!("t"));
        // SQL NULL stays distinguishable from the string "null".
        assert_eq!(
            row["absent"],
            serde_json::Value::Null,
            "a SQL NULL must stay JSON null, not become a string",
        );
    }

    let _ = runtime.force_stop().await;
    Ok(())
}

fn json_kind(value: &serde_json::Value) -> &'static str {
    match value {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "bool",
        serde_json::Value::Number(_) => "number",
        serde_json::Value::String(_) => "string",
        serde_json::Value::Array(_) => "array",
        serde_json::Value::Object(_) => "object",
    }
}
