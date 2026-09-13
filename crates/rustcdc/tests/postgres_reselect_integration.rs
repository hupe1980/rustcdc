#![cfg(feature = "postgres")]
//! Unchanged TOASTed values, with and without `reselect_unavailable_columns`.
//!
//! Both halves run against the same live server and the same statement, because the
//! feature's whole claim is a *difference* between them. Asserting only the reselecting
//! half would pass just as well if PostgreSQL had sent the value all along — and then the
//! test would prove nothing about the code under test.
//!
//! `SET STORAGE EXTERNAL` makes the behaviour deterministic rather than probable. With the
//! default `EXTENDED` storage PostgreSQL compresses first and only pushes the value
//! out-of-line if it is still too big, so a repetitive test string can stay inline — and an
//! inline value is *in* the WAL, so there would be no hole to fill and the test would pass
//! vacuously.

use rustcdc::{Operation, PostgresConnection, PostgresSourceConfig, source::Source};
use testcontainers::{
    GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};

/// Comfortably past the ~2 kB out-of-line threshold, and not compressible to under it.
fn toast_body() -> String {
    (0..4000)
        .map(|i| char::from(b'a' + (i % 26) as u8))
        .collect()
}

#[tokio::test]
async fn unchanged_toasted_values_are_absent_by_default_and_reselected_when_asked()
-> rustcdc::Result<()> {
    if std::env::var("CDC_RS_RUN_DOCKER_TESTS").as_deref() != Ok("1") {
        eprintln!("skipping postgres reselect test (set CDC_RS_RUN_DOCKER_TESTS=1)");
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
    let (admin, admin_conn) = tokio_postgres::connect(&admin_dsn, tokio_postgres::NoTls)
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;
    tokio::spawn(async move {
        let _ = admin_conn.await;
    });

    admin
        .batch_execute(
            "
            CREATE TABLE public.docs (
              id BIGINT PRIMARY KEY,
              title TEXT,
              body TEXT
            );
            ALTER TABLE public.docs ALTER COLUMN body SET STORAGE EXTERNAL;
            DROP PUBLICATION IF EXISTS docs_pub;
            CREATE PUBLICATION docs_pub FOR TABLE public.docs;
            ",
        )
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

    let body = toast_body();
    admin
        .execute(
            "INSERT INTO public.docs (id, title, body) VALUES (1, 'first', $1)",
            &[&body],
        )
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

    let config = |slot: &str, reselect: bool| PostgresSourceConfig {
        host: host.to_string(),
        port,
        user: "postgres".to_string(),
        password: "postgres".to_string().into(),
        database: "cdc".to_string(),
        replication_slot_name: slot.to_string(),
        publication_name: "docs_pub".to_string(),
        create_replication_slot_if_missing: true,
        conn_timeout_secs: 30,
        stream_poll_interval_ms: 50,
        max_events_per_poll: 1_000,
        transport: rustcdc::TransportConfig::plaintext(),
        reselect_unavailable_columns: reselect,
        ..PostgresSourceConfig::default()
    };

    // One UPDATE that does not touch `body`, captured twice: once by a connector with the
    // feature off and once by a connector with it on. Two slots over one statement is what
    // makes the comparison exact — the alternative, two statements, could differ for
    // reasons that have nothing to do with the feature.
    let mut plain = PostgresConnection::new(config("docs_plain", false));
    plain.connect().await?;
    let mut plain_stream = plain.start_stream(None).await?;

    let mut filled = PostgresConnection::new(config("docs_reselect", true));
    filled.connect().await?;
    let mut filled_stream = filled.start_stream(None).await?;

    admin
        .execute("UPDATE public.docs SET title = 'renamed' WHERE id = 1", &[])
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

    async fn first_update(
        stream: &mut Box<dyn rustcdc::source::StreamHandle>,
    ) -> rustcdc::Result<rustcdc::Event> {
        for _ in 0..100 {
            for event in stream.next_events(200).await? {
                if event.op == Operation::Update {
                    return Ok(event);
                }
            }
        }
        Err(rustcdc::Error::SourceError(
            "no UPDATE event arrived within the poll budget".into(),
        ))
    }

    let plain_event = first_update(&mut plain_stream).await?;
    let filled_event = first_update(&mut filled_stream).await?;

    // ── Default: the hole is reported, not invented ──────────────────────────
    //
    // This half is the control. If it ever stops holding, the other half proves nothing,
    // because PostgreSQL would be sending the value without being asked.
    assert_eq!(
        plain_event.unavailable_columns,
        vec!["body".to_string()],
        "an unchanged TOASTed column must be reported absent, not guessed at"
    );
    let plain_after = plain_event
        .after
        .as_ref()
        .expect("update has an after image");
    assert!(
        plain_after.get("body").is_none(),
        "absent means absent: the key must not be present at all, so a consumer cannot \
         read it as NULL"
    );
    assert_eq!(
        plain_after.get("title").and_then(|v| v.as_str()),
        Some("renamed"),
        "the column the statement did change must be present"
    );

    // ── Reselected: the value is recovered, and the list stops claiming a hole ──
    let filled_after = filled_event
        .after
        .as_ref()
        .expect("update has an after image");
    assert_eq!(
        filled_after.get("body").and_then(|v| v.as_str()),
        Some(body.as_str()),
        "the reselected value must be the row's actual body, byte for byte"
    );
    assert!(
        filled_event.unavailable_columns.is_empty(),
        "a filled column must be removed from unavailable_columns, or a sink reading that \
         list will refuse to write the value that was just recovered; got {:?}",
        filled_event.unavailable_columns
    );

    Ok(())
}

/// A row deleted before the reselect runs leaves the columns absent.
///
/// The failure mode this rules out is the tempting one: treating "no row came back" as
/// "the column is NULL" would write NULL over a value that was never touched, which is the
/// exact loss `unavailable_columns` exists to prevent. Degrading to the default behaviour
/// is the only answer that is not a guess.
#[tokio::test]
async fn a_row_deleted_before_the_reselect_leaves_the_columns_absent() -> rustcdc::Result<()> {
    if std::env::var("CDC_RS_RUN_DOCKER_TESTS").as_deref() != Ok("1") {
        eprintln!("skipping postgres reselect delete test (set CDC_RS_RUN_DOCKER_TESTS=1)");
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
    let (admin, admin_conn) = tokio_postgres::connect(&admin_dsn, tokio_postgres::NoTls)
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;
    tokio::spawn(async move {
        let _ = admin_conn.await;
    });

    admin
        .batch_execute(
            "
            CREATE TABLE public.docs (
              id BIGINT PRIMARY KEY,
              title TEXT,
              body TEXT
            );
            ALTER TABLE public.docs ALTER COLUMN body SET STORAGE EXTERNAL;
            DROP PUBLICATION IF EXISTS docs_pub;
            CREATE PUBLICATION docs_pub FOR TABLE public.docs;
            ",
        )
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

    let body = toast_body();
    admin
        .execute(
            "INSERT INTO public.docs (id, title, body) VALUES (1, 'first', $1)",
            &[&body],
        )
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

    let mut connection = PostgresConnection::new(PostgresSourceConfig {
        host: host.to_string(),
        port,
        user: "postgres".to_string(),
        password: "postgres".to_string().into(),
        database: "cdc".to_string(),
        replication_slot_name: "docs_deleted".to_string(),
        publication_name: "docs_pub".to_string(),
        create_replication_slot_if_missing: true,
        conn_timeout_secs: 30,
        stream_poll_interval_ms: 50,
        max_events_per_poll: 1_000,
        transport: rustcdc::TransportConfig::plaintext(),
        reselect_unavailable_columns: true,
        ..PostgresSourceConfig::default()
    });
    connection.connect().await?;
    let mut stream = connection.start_stream(None).await?;

    // The UPDATE leaves a hole; the DELETE removes the row that could fill it. Both commit
    // before the first poll, so the reselect is guaranteed to run after the row is gone —
    // this is the race, made deterministic.
    admin
        .batch_execute(
            "UPDATE public.docs SET title = 'renamed' WHERE id = 1;
             DELETE FROM public.docs WHERE id = 1;",
        )
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

    let mut update = None;
    for _ in 0..100 {
        for event in stream.next_events(200).await? {
            if event.op == Operation::Update {
                update = Some(event);
            }
        }
        if update.is_some() {
            break;
        }
    }
    let update = update.expect("the UPDATE event must arrive");

    assert_eq!(
        update.unavailable_columns,
        vec!["body".to_string()],
        "with the row gone there is nothing to read, so the column must stay absent rather \
         than being reported as recovered"
    );
    assert!(
        update
            .after
            .as_ref()
            .expect("after image")
            .get("body")
            .is_none(),
        "a failed reselect must not leave a NULL behind"
    );

    Ok(())
}
