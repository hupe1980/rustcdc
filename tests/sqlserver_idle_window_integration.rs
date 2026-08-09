//! A change committed after a long idle period must still be captured.
//!
//! # What this protects
//!
//! SQL Server CDC is read one LSN window at a time, and the window's upper bound comes from
//! `fn_cdc_get_max_lsn()` — what the capture job has *harvested* into `cdc.*`, which does not move
//! while nothing is being harvested.
//!
//! The window used to be clamped so it was never inverted: after reading `[S, M]` the next window
//! became `[M+1, max(M, M+1)]` = `[M+1, M+1]`, and the *next* advance incremented from that clamped
//! end. So every empty poll pushed the read point one minimal LSN step further above the harvested
//! maximum, indefinitely, and whether a later change was captured depended on its LSN still being
//! above wherever the point had crept to.
//!
//! The naive repair is worse: parking at `[M+1, M+1]` and advancing from that end once the maximum
//! moves skips `M+1`, an LSN that was never readable while the window sat there.
//!
//! The rule now is that `lsn_end` never exceeds the harvested maximum, so an empty window is
//! *represented* (`lsn_start > lsn_end`) rather than clamped, and the lower bound moves only when
//! something was consumed.
//!
//! # Why the idle phase deliberately does not force a capture pass
//!
//! The other SQL Server suites call `sys.sp_cdc_scan` whenever a poll comes back empty, because the
//! capture job is slow to run under container load. Doing that here would defeat the test: a scan
//! harvests whatever is in the log, so `fn_cdc_get_max_lsn()` would keep advancing and the read
//! point would never be left behind a standing maximum — which is the exact condition the creep
//! needed.
//!
//! So the idle phase polls **without** scanning, holding the harvested maximum still while the
//! window advances repeatedly. Only after the write does the test force scans, so the change is
//! harvested and the maximum jumps. A change landing in the window that reopens is the property
//! under test; a change that never arrives is the creep.

#![cfg(feature = "sqlserver")]

use rustcdc::{source::Source, Operation, SqlServerConnection};

#[path = "sqlserver_testkit.rs"]
mod sqlserver_testkit;

type SqlClient = tiberius::Client<tokio_util::compat::Compat<tokio::net::TcpStream>>;

const DATABASE: &str = "rustcdc_idle_window";
/// Enough empty polls that a one-step-per-poll creep would have carried the read point well past
/// the standing harvested maximum before the write lands.
const IDLE_POLLS: usize = 40;

async fn sql_exec(client: &mut SqlClient, sql: &str) -> rustcdc::Result<()> {
    client
        .execute(sql, &[])
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;
    Ok(())
}

#[tokio::test]
async fn a_change_after_a_long_idle_period_is_still_captured() -> rustcdc::Result<()> {
    if sqlserver_testkit::skip_docker_test("sqlserver idle window") {
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
        &format!("IF DB_ID('{DATABASE}') IS NULL CREATE DATABASE [{DATABASE}]"),
    )
    .await?;
    sql_exec(
        &mut admin,
        &format!(
            "USE [{DATABASE}]; IF OBJECT_ID('dbo.idle_probe', 'U') IS NULL \
             CREATE TABLE dbo.idle_probe (id INT NOT NULL PRIMARY KEY, v NVARCHAR(32) NOT NULL)"
        ),
    )
    .await?;
    sqlserver_testkit::enable_cdc(&host, port, DATABASE).await?;
    sqlserver_testkit::sql_exec_with_retry(
        &mut admin,
        &format!(
            "USE [{DATABASE}]; IF NOT EXISTS (SELECT 1 FROM cdc.change_tables \
             WHERE source_object_id = OBJECT_ID('dbo.idle_probe')) \
             EXEC sys.sp_cdc_enable_table @source_schema='dbo', @source_name='idle_probe', \
             @role_name=NULL, @supports_net_changes=0"
        ),
    )
    .await?;
    // The capture job registers the instance asynchronously, and `start_stream` enumerates
    // instances once at open.
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    let mut source = SqlServerConnection::new(sqlserver_testkit::source_config(
        host.clone(),
        port,
        DATABASE.to_string(),
        30,
    ));
    source.connect().await?;
    let mut stream = source.start_stream(None).await?;

    // ── Idle: advance the window repeatedly against a standing harvested maximum ──
    //
    // No `sp_cdc_scan` here, deliberately — see the module docs. Each poll finds nothing and
    // advances; under the old rule the read point ended up `IDLE_POLLS` minimal LSN steps above the
    // maximum, under the new one it parks at one step past and stays.
    for _ in 0..IDLE_POLLS {
        let events = stream.next_events(100).await?;
        assert!(
            events.is_empty(),
            "nothing has been written yet, so no poll may produce an event: {events:?}"
        );
    }

    // ── Then write, force the harvest, and require the change ─────────────────
    sql_exec(
        &mut admin,
        &format!("USE [{DATABASE}]; INSERT INTO dbo.idle_probe (id, v) VALUES (1, N'after-idle')"),
    )
    .await?;

    let scan_sql = format!("USE [{DATABASE}]; EXEC sys.sp_cdc_scan");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(120);
    let mut captured = None;
    while captured.is_none() && std::time::Instant::now() < deadline {
        let batch = stream.next_events(200).await?;
        if batch.is_empty() {
            // Only now: harvest what the write put in the log, so the maximum advances.
            let _ = sql_exec(&mut admin, &scan_sql).await;
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            continue;
        }
        captured = batch
            .into_iter()
            .find(|event| event.op == Operation::Insert && event.table.eq_ignore_ascii_case("idle_probe"));
    }

    let captured = captured.unwrap_or_else(|| {
        panic!(
            "a row written after {IDLE_POLLS} idle polls was never captured. The LSN read point \
             has moved past where the change was harvested — the creep this rule exists to \
             prevent, and silent data loss rather than an error."
        )
    });
    let after = captured
        .after
        .as_ref()
        .expect("an insert carries an after image");
    assert_eq!(
        after.get("v").and_then(serde_json::Value::as_str),
        Some("after-idle"),
        "the captured row must be the one written: {after}"
    );

    Ok(())
}
