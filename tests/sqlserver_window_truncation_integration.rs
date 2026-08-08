//! Multi-capture-instance LSN windows that a single poll cannot read in full.
//!
//! SQL Server CDC is read one LSN window at a time, and every capture instance in the
//! window is queried with its own `TOP (max_events_per_poll)`. Instances therefore
//! truncate at *different* positions, so the only globally safe resume point inside a
//! window is the minimum last-row position across the instances that returned a full
//! page — the "truncation cursor". Rows beyond it are dropped from the batch and re-read
//! on the next poll.
//!
//! That cursor used to be a local variable in the fill path, applied only if the buffer
//! happened to drain in the same poll. With two or more capture instances a window
//! routinely produces more events than one poll returns, so the buffer did *not* drain
//! there: the cursor was discarded, and the deferred `advance_window()` at the eventual
//! drain point moved the window past the unread remainder. Those rows were gone —
//! permanently, silently, with `events_polled` still reporting a plausible count.
//!
//! The reproduction needs three things together, which is why no existing suite caught
//! it: **two** capture instances, a `max_events_per_poll` small enough to truncate, and
//! enough rows in one window that the retained prefix still spans more than one page.

#![cfg(feature = "sqlserver")]

use std::collections::BTreeSet;

use rustcdc::{source::Source, Operation, SqlServerConnection};

#[path = "sqlserver_testkit.rs"]
mod sqlserver_testkit;

type SqlClient = tiberius::Client<tokio_util::compat::Compat<tokio::net::TcpStream>>;

const DATABASE: &str = "rustcdc_window_truncation";
/// Small enough that both capture instances truncate, and small enough that the retained
/// prefix of the window still spans several pages.
const MAX_EVENTS_PER_POLL: usize = 5;
/// Per table. Both tables are written before the first poll, so every row lands in one
/// LSN window.
const ROWS_PER_TABLE: i32 = 30;

async fn sql_exec(client: &mut SqlClient, sql: &str) -> rustcdc::Result<()> {
    client
        .execute(sql, &[])
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;
    Ok(())
}

/// `sp_cdc_enable_table` deadlocks against the capture job under container load.
async fn sql_exec_with_retry(client: &mut SqlClient, sql: &str) -> rustcdc::Result<()> {
    for attempt in 1..=8 {
        match sql_exec(client, sql).await {
            Ok(()) => return Ok(()),
            Err(error) => {
                let message = error.to_string().to_ascii_lowercase();
                let deadlocked =
                    message.contains("deadlock victim") || message.contains("code: 1205");
                if deadlocked && attempt < 8 {
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                    continue;
                }
                return Err(error);
            }
        }
    }
    unreachable!("loop returns on the final attempt")
}

#[tokio::test]
async fn a_window_truncated_across_two_capture_instances_loses_no_rows() -> rustcdc::Result<()> {
    const TEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(300);

    tokio::time::timeout(TEST_TIMEOUT, run())
        .await
        .map_err(|_| {
            rustcdc::Error::TimeoutError(
                "sqlserver window-truncation integration exceeded 300s timeout".to_string(),
            )
        })?
}

async fn run() -> rustcdc::Result<()> {
    if sqlserver_testkit::skip_docker_test("sqlserver window-truncation integration test") {
        return Ok(());
    }

    let container = match sqlserver_testkit::start_sqlserver_container("2022-latest").await {
        Ok(container) => container,
        Err(ref error) if sqlserver_testkit::is_skip_error(error) => return Ok(()),
        Err(error) => return Err(error),
    };
    let (host, port) = sqlserver_testkit::host_and_port(&container).await?;

    let mut admin = sqlserver_testkit::connect_admin_with_retry(
        &host,
        port,
        40,
        std::time::Duration::from_secs(2),
    )
    .await?;

    sql_exec(
        &mut admin,
        &format!("IF DB_ID('{DATABASE}') IS NULL CREATE DATABASE {DATABASE}"),
    )
    .await?;

    // Two tables, so CDC creates two capture instances.
    for table in ["orders", "shipments"] {
        sql_exec(
            &mut admin,
            &format!(
                "USE {DATABASE}; IF OBJECT_ID('dbo.{table}', 'U') IS NULL \
                 CREATE TABLE dbo.{table} (id INT NOT NULL PRIMARY KEY, note NVARCHAR(50) NOT NULL)"
            ),
        )
        .await?;
        sql_exec(
            &mut admin,
            &format!("USE {DATABASE}; DELETE FROM dbo.{table}"),
        )
        .await?;
    }

    sqlserver_testkit::enable_cdc(&host, port, DATABASE).await?;
    for table in ["orders", "shipments"] {
        sql_exec_with_retry(
            &mut admin,
            &format!(
                "USE {DATABASE}; IF NOT EXISTS (SELECT 1 FROM cdc.change_tables \
                 WHERE source_object_id = OBJECT_ID('dbo.{table}')) \
                 EXEC sys.sp_cdc_enable_table @source_schema='dbo', @source_name='{table}', \
                 @role_name=NULL, @supports_net_changes=0"
            ),
        )
        .await?;
    }
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    let mut config = sqlserver_testkit::source_config(host.clone(), port, DATABASE.into(), 30);
    config.max_events_per_poll = MAX_EVENTS_PER_POLL;

    let mut source = SqlServerConnection::new(config);
    source.connect().await?;
    let mut stream = source.start_stream(None).await?;

    // Everything below is written before the first poll, so it all falls inside one LSN
    // window and both capture instances are guaranteed to truncate.
    for table in ["orders", "shipments"] {
        for id in 1..=ROWS_PER_TABLE {
            sql_exec(
                &mut admin,
                &format!(
                    "USE {DATABASE}; INSERT INTO dbo.{table} (id, note) VALUES ({id}, 'n{id}')"
                ),
            )
            .await?;
        }
    }

    let expected = usize::try_from(ROWS_PER_TABLE).expect("row count fits usize") * 2;
    let mut seen: BTreeSet<(String, i64)> = BTreeSet::new();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(180);
    let scan = format!("USE {DATABASE}; EXEC sys.sp_cdc_scan");

    while std::time::Instant::now() < deadline && seen.len() < expected {
        let batch = stream.next_events(500).await?;
        assert!(
            batch.len() <= MAX_EVENTS_PER_POLL,
            "a poll must not exceed max_events_per_poll; got {}",
            batch.len()
        );
        if batch.is_empty() {
            // Nudge the capture job; containers run it lazily.
            let _ = sql_exec(&mut admin, &scan).await;
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            continue;
        }
        for event in batch {
            if event.op != Operation::Insert {
                continue;
            }
            let id = event
                .after
                .as_ref()
                .and_then(|after| after.get("id"))
                .and_then(|value| {
                    // The CDC projection renders integers as JSON numbers, but a wide
                    // integer arrives as a string; accept either.
                    value
                        .as_i64()
                        .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
                })
                .expect("insert event carries its primary key");
            seen.insert((event.table.clone(), id));
        }
    }

    // Report the gap rather than just the count: the failure mode is a contiguous run of
    // rows vanishing from the middle of a window, and knowing which run is the diagnosis.
    if seen.len() < expected {
        let mut missing = Vec::new();
        for table in ["orders", "shipments"] {
            for id in 1..=i64::from(ROWS_PER_TABLE) {
                if !seen.contains(&(table.to_string(), id)) {
                    missing.push(format!("{table}#{id}"));
                }
            }
        }
        panic!(
            "a truncated CDC window dropped {} of {expected} rows: {}. The window advanced \
             past rows that `max_events_per_poll` had cut off from at least one capture \
             instance.",
            missing.len(),
            missing.join(", ")
        );
    }

    source.close().await;
    Ok(())
}
