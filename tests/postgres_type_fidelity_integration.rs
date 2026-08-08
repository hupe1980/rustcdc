#![cfg(feature = "postgres")]

//! Type-fidelity and REPLICA IDENTITY coverage for the PostgreSQL connector.
//!
//! # Why this file exists
//!
//! Every other integration schema in this repository is `BIGINT` + `TEXT`. That gap is
//! exactly what allowed a silent-corruption defect to survive in the SQL Server
//! connector: its client-side decoder handled five Rust types and returned `null` for
//! decimal, datetime2, uniqueidentifier, varbinary and xml — indistinguishable from a
//! genuine SQL NULL — and no test used any of those types.
//!
//! These tests assert on the *decoded values* of types whose handling is easy to get
//! silently wrong: exact numerics, timestamps, UUIDs, binary, JSON, arrays and enums.
//!
//! They also cover `REPLICA IDENTITY DEFAULT` — the PostgreSQL default and the
//! overwhelmingly common production configuration — which every other integration test
//! avoids by forcing `FULL`.

use rustcdc::{
    core::Operation, source::Source, PostgresConnection, PostgresSourceConfig, RowWrite,
};
use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    ContainerAsync, GenericImage, ImageExt,
};

fn skip() -> bool {
    if std::env::var("CDC_RS_RUN_DOCKER_TESTS").as_deref() != Ok("1") {
        eprintln!("skipping postgres type-fidelity test (set CDC_RS_RUN_DOCKER_TESTS=1)");
        return true;
    }
    false
}

async fn start_postgres() -> rustcdc::Result<(ContainerAsync<GenericImage>, String, u16)> {
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

    Ok((container, host, port))
}

async fn admin_client(host: &str, port: u16) -> rustcdc::Result<tokio_postgres::Client> {
    let dsn = format!(
        "host={host} port={port} user=postgres password=postgres dbname=cdc connect_timeout=30"
    );
    let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    Ok(client)
}

fn source_config(host: &str, port: u16, slot: &str, publication: &str) -> PostgresSourceConfig {
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
        ..PostgresSourceConfig::default()
    }
}

async fn drain(
    handle: &mut Box<dyn rustcdc::source::StreamHandle>,
) -> rustcdc::Result<Vec<rustcdc::Event>> {
    let mut collected = Vec::new();
    for _ in 0..60 {
        let events = handle.next_events(100).await?;
        if events.is_empty() && !collected.is_empty() {
            break;
        }
        collected.extend(events);
        if collected.len() >= 8 {
            break;
        }
    }
    Ok(collected)
}

/// Values of non-trivial types must survive the WAL round trip intact.
///
/// The assertions below are deliberately about *exact* values, not just presence. A
/// decoder that silently substitutes `null`, truncates precision, or reorders a
/// composite would pass a presence check and fail these.
#[tokio::test]
async fn postgres_decodes_non_trivial_types_without_loss() -> rustcdc::Result<()> {
    if skip() {
        return Ok(());
    }

    let (_container, host, port) = start_postgres().await?;
    let admin = admin_client(&host, port).await?;

    admin
        .batch_execute(
            "
            CREATE TYPE public.mood AS ENUM ('sad', 'ok', 'happy');
            CREATE TABLE public.types_test (
              id             BIGINT PRIMARY KEY,
              exact_amount   NUMERIC(20, 4),
              big_int        BIGINT,
              real_value     DOUBLE PRECISION,
              flag           BOOLEAN,
              created_at     TIMESTAMPTZ,
              plain_date     DATE,
              span           INTERVAL,
              identifier     UUID,
              payload        JSONB,
              raw            BYTEA,
              tags           TEXT[],
              feeling        public.mood,
              maybe_null     TEXT
            );
            ALTER TABLE public.types_test REPLICA IDENTITY FULL;
            DROP PUBLICATION IF EXISTS types_pub;
            CREATE PUBLICATION types_pub FOR TABLE public.types_test;
            ",
        )
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

    let mut connection =
        PostgresConnection::new(source_config(&host, port, "types_test_slot", "types_pub"));
    connection.connect().await?;
    let mut stream = connection.start_stream(None).await?;

    // A value chosen to break naive handling: 16 significant digits exceeds f64's exact
    // integer range, so any path through a float loses precision.
    admin
        .batch_execute(
            "
            INSERT INTO public.types_test VALUES (
              1,
              1234567890123.4567,
              9223372036854775807,
              0.1,
              true,
              '2026-07-20 12:34:56.789+00',
              '2026-07-20',
              '1 day 02:03:04',
              'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11',
              '{\"nested\": {\"k\": [1, 2, 3]}}',
              '\\xdeadbeef',
              ARRAY['alpha', 'beta'],
              'happy',
              NULL
            );
            ",
        )
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

    let events = drain(&mut stream).await?;
    let insert = events
        .iter()
        .find(|event| event.op == Operation::Insert)
        .expect("expected an insert event");
    let after = insert
        .after
        .as_ref()
        .expect("insert must carry an after image");

    let field = |name: &str| -> String {
        after
            .get(name)
            .unwrap_or_else(|| panic!("column '{name}' missing from payload: {after}"))
            .as_str()
            .unwrap_or_else(|| panic!("column '{name}' is not a string: {after}"))
            .to_string()
    };

    // Exact numeric: full precision, no float rounding. This is the assertion that
    // catches a decoder routing NUMERIC through f64.
    assert_eq!(field("exact_amount"), "1234567890123.4567");

    // i64::MAX survives — a decoder going through a JSON number would lose it beyond 2^53.
    assert_eq!(field("big_int"), "9223372036854775807");

    assert_eq!(field("real_value"), "0.1");

    // Booleans arrive in pgoutput's TEXT output form — `t`/`f`, not `true`/`false`.
    // Asserted explicitly because it is a real consumer-visible detail: code expecting
    // a JSON boolean or the string "true" will silently mis-handle every bool column.
    assert_eq!(field("flag"), "t");
    assert_eq!(field("identifier"), "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
    assert_eq!(field("plain_date"), "2026-07-20");
    assert_eq!(field("feeling"), "happy");

    // bytea arrives in PostgreSQL hex output form; the bytes must be recoverable.
    assert!(
        field("raw").to_lowercase().ends_with("deadbeef"),
        "bytea lost its content: {}",
        field("raw")
    );

    // Timestamp keeps sub-second precision.
    let created = field("created_at");
    assert!(created.starts_with("2026-07-20"), "{created}");
    assert!(
        created.contains("789"),
        "sub-second precision lost: {created}"
    );

    // Composite types keep their structure.
    let payload = field("payload");
    assert!(payload.contains("nested"), "{payload}");
    assert!(payload.contains('3'), "array inside jsonb lost: {payload}");
    let tags = field("tags");
    assert!(tags.contains("alpha") && tags.contains("beta"), "{tags}");
    let span = field("span");
    assert!(span.contains('1') && span.contains("02:03:04"), "{span}");

    // A genuine SQL NULL must be JSON null — never absent, never an empty string.
    assert_eq!(
        after.get("maybe_null"),
        Some(&serde_json::Value::Null),
        "a real NULL must decode to JSON null, distinguishably from a missing column"
    );

    // No column may be reported unavailable: nothing here is an unchanged TOAST value.
    assert!(
        insert.unavailable_columns.is_empty(),
        "unexpected unavailable columns: {:?}",
        insert.unavailable_columns
    );

    Ok(())
}

/// `REPLICA IDENTITY DEFAULT` is the PostgreSQL default and the common production
/// configuration, and every other integration test in this repo avoids it by forcing
/// `FULL`. Under `DEFAULT` the before-image of an UPDATE/DELETE is key-only, which the
/// envelope must report via `before_is_key_only` so consumers do not mistake it for a
/// complete prior row.
#[tokio::test]
async fn postgres_replica_identity_default_reports_key_only_before_image() -> rustcdc::Result<()> {
    if skip() {
        return Ok(());
    }

    let (_container, host, port) = start_postgres().await?;
    let admin = admin_client(&host, port).await?;

    admin
        .batch_execute(
            "
            CREATE TABLE public.ri_default (
              id    BIGINT PRIMARY KEY,
              name  TEXT,
              notes TEXT
            );
            -- Deliberately NOT setting REPLICA IDENTITY FULL: DEFAULT is the case
            -- every other integration test avoids.
            DROP PUBLICATION IF EXISTS ri_default_pub;
            CREATE PUBLICATION ri_default_pub FOR TABLE public.ri_default;
            ",
        )
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

    let mut connection = PostgresConnection::new(source_config(
        &host,
        port,
        "ri_default_slot",
        "ri_default_pub",
    ));
    connection.connect().await?;
    let mut stream = connection.start_stream(None).await?;

    admin
        .batch_execute(
            "
            INSERT INTO public.ri_default VALUES (1, 'alice', 'first');
            UPDATE public.ri_default SET name = 'alice-v2' WHERE id = 1;
            DELETE FROM public.ri_default WHERE id = 1;
            ",
        )
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

    let events = drain(&mut stream).await?;

    let update = events
        .iter()
        .find(|event| event.op == Operation::Update)
        .expect("expected an update event");

    // The after-image is always complete.
    let after = update
        .after
        .as_ref()
        .expect("update must carry an after image");
    assert_eq!(after.get("name").and_then(|v| v.as_str()), Some("alice-v2"));
    assert_eq!(after.get("notes").and_then(|v| v.as_str()), Some("first"));

    // Under DEFAULT the before-image is key-only (or absent when the key did not
    // change). Whenever it IS present, the envelope must say it is not a full row —
    // otherwise a consumer computing a diff silently treats missing columns as changes.
    if let Some(before) = update.before.as_ref() {
        assert!(
            update.before_is_key_only,
            "a key-only before-image must be flagged: {before}"
        );
        assert!(
            before.get("id").is_some(),
            "a key-only before-image must still carry the key: {before}"
        );
    }

    // The event key must resolve in both phases regardless of replica identity —
    // this is what downstream compaction and upserts depend on.
    assert_eq!(
        update.primary_key.as_deref(),
        Some(["id".to_string()].as_slice()),
        "primary key must be reported under REPLICA IDENTITY DEFAULT"
    );
    assert!(
        update.primary_key_values().is_some(),
        "primary key values must resolve so the record is not emitted unkeyed"
    );

    let delete = events
        .iter()
        .find(|event| event.op == Operation::Delete)
        .expect("expected a delete event");
    assert!(
        delete
            .before
            .as_ref()
            .and_then(|before| before.get("id"))
            .is_some(),
        "a DELETE must identify the row it removed"
    );

    Ok(())
}

/// Unchanged-TOAST is the one case where the **after**-image is incomplete, and it is
/// widely misunderstood as something `REPLICA IDENTITY FULL` fixes. It is not: replica
/// identity governs the *old* tuple only. This test forces `FULL` and still expects the
/// hole, so the documented guidance stays honest.
///
/// It also pins the per-image split. A TOASTed column that *was* modified is present in
/// `after` and absent from `before` — if the two lists were merged, a correct sink would
/// skip writing a value that genuinely changed.
#[tokio::test]
async fn postgres_unchanged_toast_is_reported_per_image_even_under_replica_identity_full(
) -> rustcdc::Result<()> {
    if skip() {
        return Ok(());
    }

    let (_container, host, port) = start_postgres().await?;
    let admin = admin_client(&host, port).await?;

    admin
        .batch_execute(
            "
            CREATE TABLE public.toast_test (
              id       BIGINT PRIMARY KEY,
              small    TEXT,
              big_kept TEXT,
              big_changed TEXT
            );
            -- FULL is deliberate: it is the setting operators reach for expecting it to
            -- close this hole.
            ALTER TABLE public.toast_test REPLICA IDENTITY FULL;
            -- STORAGE EXTERNAL disables compression, so a large value is guaranteed to
            -- go out-of-line. With the default EXTENDED, a compressible value is stored
            -- inline and never becomes a TOAST pointer — which is exactly what makes
            -- this defect so easy to miss in testing.
            ALTER TABLE public.toast_test ALTER COLUMN big_kept SET STORAGE EXTERNAL;
            ALTER TABLE public.toast_test ALTER COLUMN big_changed SET STORAGE EXTERNAL;
            DROP PUBLICATION IF EXISTS toast_pub;
            CREATE PUBLICATION toast_pub FOR TABLE public.toast_test;
            ",
        )
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

    // Comfortably past the ~2 kB TOAST threshold. Combined with STORAGE EXTERNAL above,
    // these are guaranteed to be stored out-of-line and therefore eligible for the
    // unchanged-TOAST placeholder.
    let filler = |seed: u64| -> String {
        let mut state = seed;
        (0..40_000)
            .map(|_| {
                state = state
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1);
                (b'a' + ((state >> 33) % 26) as u8) as char
            })
            .collect()
    };
    let big_kept = filler(1);
    let big_changed_old = filler(2);
    let big_changed_new = filler(3);

    admin
        .execute(
            "INSERT INTO public.toast_test VALUES (1, 'small', $1, $2)",
            &[&big_kept, &big_changed_old],
        )
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

    let mut connection =
        PostgresConnection::new(source_config(&host, port, "toast_slot", "toast_pub"));
    connection.connect().await?;
    let mut stream = connection.start_stream(None).await?;

    // `big_kept` is untouched; `big_changed` is rewritten. Both are TOASTed.
    admin
        .execute(
            "UPDATE public.toast_test SET small = 'small-v2', big_changed = $1 WHERE id = 1",
            &[&big_changed_new],
        )
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

    let events = drain(&mut stream).await?;
    let update = events
        .iter()
        .find(|event| event.op == Operation::Update)
        .expect("expected an update event");

    let after = update
        .after
        .as_ref()
        .expect("update must carry an after image");

    // The claim under test: FULL does not make the after-image complete.
    assert!(
        update.unavailable_columns.contains(&"big_kept".to_string()),
        "an unmodified TOASTed column must be reported unavailable even under \
         REPLICA IDENTITY FULL; got {:?}",
        update.unavailable_columns
    );
    assert!(
        after.get("big_kept").is_none(),
        "an unavailable column must be ABSENT from after, not present as a placeholder"
    );

    // The modified TOASTed column is fully present and must not be marked unavailable —
    // this is exactly what a merged before/after list would get wrong.
    assert_eq!(
        after.get("big_changed").and_then(|v| v.as_str()),
        Some(big_changed_new.as_str()),
        "a modified TOASTed column must arrive complete in the after image"
    );
    assert!(
        !update
            .unavailable_columns
            .contains(&"big_changed".to_string()),
        "a column that genuinely changed must never be marked unavailable, or a correct \
         sink skips writing it: {:?}",
        update.unavailable_columns
    );
    assert_eq!(
        after.get("small").and_then(|v| v.as_str()),
        Some("small-v2"),
        "non-TOASTed columns are unaffected"
    );

    // The envelope invariants must hold on a real payload, not just in unit fixtures.
    update
        .validate()
        .expect("a real unchanged-TOAST event must satisfy the envelope invariants");

    // The write plan must degrade to a merge, never a full-row replace.
    match update.row_write() {
        RowWrite::Merge {
            key,
            columns,
            unavailable_columns,
        } => {
            // pgoutput delivers values in their text form, so the key is "1", not 1.
            let id = key
                .get("id")
                .expect("the merge key must carry the primary key");
            assert!(
                id.as_str() == Some("1") || id.as_i64() == Some(1),
                "unexpected key encoding: {id}"
            );
            assert!(columns.get("big_kept").is_none());
            assert!(unavailable_columns.contains(&"big_kept".to_string()));
        }
        other => panic!(
            "an unchanged-TOAST update must yield a partial write, not {other:?} — a \
             full-row write would erase a 40 kB value that never changed"
        ),
    }

    Ok(())
}
