//! The MySQL incremental-snapshot watermark must be an executed-GTID set, and the two id spaces
//! it joins must actually line up.
//!
//! # What this protects
//!
//! With `binlog_order_commits = ON` (the default) a transaction is written to the binlog in the
//! **flush** stage and engine-committed afterwards, and `SHOW MASTER STATUS`'s `File`/`Position`
//! advance at the flush. So a transaction can sit *below* a coordinate-based low watermark and
//! still be invisible to a chunk `SELECT` that starts next — the chunk holds the row's pre-image,
//! the ordinal test finds nothing to suppress, and the stale value is emitted over the newer one.
//!
//! `Executed_Gtid_Set` is updated **after** the engine commit, so a GTID present in it belongs to
//! a transaction whose rows are already visible. The connector therefore brackets by set
//! difference: inside iff the event's GTID is in `high` and not in `low`.
//!
//! That rests on one thing no unit test can establish: **the GTID a binlog event carries and the
//! GTIDs in `Executed_Gtid_Set` must be the same identifiers.** If they are not — a different
//! rendering, a different source uuid, a stray suffix — set membership silently never matches,
//! the bracket degrades to the ordinal test it replaced, and every unit test stays green because
//! they define both sides themselves.
//!
//! So this test asserts exactly that join, against a real server:
//!
//! 1. read `Executed_Gtid_Set` — the low watermark;
//! 2. commit a row;
//! 3. read `Executed_Gtid_Set` again — the high watermark;
//! 4. stream the event and assert its GTID is in `high` and **not** in `low`.
//!
//! Step 4 is the half a fake backend cannot fake: the GTID is the server's own, arriving through
//! the connector's binlog decoder.

#![cfg(feature = "mysql")]

use rustcdc::{source::Source, MysqlConnection, MysqlSourceConfig, Operation};
use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    GenericImage, ImageExt,
};

#[path = "rustls_provider_common.rs"]
mod rustls_provider_common;
use rustls_provider_common::install_rustls_provider;

fn source_error(error: impl std::fmt::Display) -> rustcdc::Error {
    rustcdc::Error::SourceError(error.to_string())
}

/// Read `Executed_Gtid_Set` the way the connector's watermark does.
async fn executed_gtid_set(pool: &sqlx::MySqlPool) -> rustcdc::Result<String> {
    let row: (String, u64, String, String, String) =
        sqlx::query_as("SHOW MASTER STATUS").fetch_one(pool).await.map_err(source_error)?;
    Ok(row.4)
}

/// Membership test over MySQL's `uuid:a-b:c,uuid2:d` rendering, mirroring `GtidSet`.
fn set_contains(set: &str, gtid: &str) -> bool {
    let Some((want_uuid, want_seq)) = gtid.trim().rsplit_once(':') else {
        return false;
    };
    let Ok(want_seq) = want_seq.parse::<u64>() else {
        return false;
    };
    for entry in set.split(',') {
        let entry: String = entry.chars().filter(|c| !c.is_whitespace()).collect();
        let mut parts = entry.split(':');
        let Some(uuid) = parts.next() else { continue };
        if !uuid.eq_ignore_ascii_case(want_uuid) {
            continue;
        }
        for interval in parts {
            let (start, end) = match interval.split_once('-') {
                Some((start, end)) => (start.parse::<u64>(), end.parse::<u64>()),
                None => (interval.parse::<u64>(), interval.parse::<u64>()),
            };
            if let (Ok(start), Ok(end)) = (start, end) {
                if want_seq >= start && want_seq <= end {
                    return true;
                }
            }
        }
    }
    false
}

#[tokio::test]
async fn an_events_gtid_falls_inside_the_executed_set_difference() -> rustcdc::Result<()> {
    install_rustls_provider();
    if std::env::var("CDC_RS_RUN_DOCKER_TESTS").as_deref() != Ok("1") {
        eprintln!("skipping mysql GTID watermark test (set CDC_RS_RUN_DOCKER_TESTS=1)");
        return Ok(());
    }

    let container = GenericImage::new("mysql", "8.0")
        .with_exposed_port(3306.tcp())
        .with_wait_for(WaitFor::message_on_stderr("ready for connections"))
        .with_env_var("MYSQL_ROOT_PASSWORD", "rootpass")
        .with_env_var("MYSQL_DATABASE", "cdc")
        .with_cmd(vec![
            "--log-bin=mysql-bin",
            "--binlog-format=ROW",
            "--binlog-row-metadata=FULL",
            "--binlog-row-image=FULL",
            "--server-id=1",
            // The whole point: the GTID bracket is only available with GTID mode on.
            "--gtid-mode=ON",
            "--enforce-gtid-consistency=ON",
        ])
        .start()
        .await
        .map_err(source_error)?;

    let host = container.get_host().await.map_err(source_error)?.to_string();
    let port = container
        .get_host_port_ipv4(3306.tcp())
        .await
        .map_err(source_error)?;

    // `WaitFor::message_on_stderr("ready for connections")` fires during MySQL's
    // *initialisation* phase, before the final restart that serves clients — and the GTID flags
    // lengthen initialisation, so a single connect attempt races it and fails with
    // `got 0 bytes at EOF`. Retry until the server is actually accepting.
    // No `ssl-mode=DISABLED`: sqlx negotiates TLS, which is how MySQL 8's
    // `caching_sha2_password` completes without sqlx's optional RSA feature. This is also
    // why `install_rustls_provider` above is required.
    let dsn = format!("mysql://root:rootpass@{host}:{port}/cdc");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(90);
    let pool = loop {
        match sqlx::MySqlPool::connect(&dsn).await {
            Ok(pool) => break pool,
            Err(error) if std::time::Instant::now() < deadline => {
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                let _ = error;
            }
            Err(error) => return Err(source_error(error)),
        }
    };

    sqlx::query("CREATE TABLE gtid_probe (id BIGINT PRIMARY KEY, v TEXT NOT NULL)")
        .execute(&pool)
        .await
        .map_err(source_error)?;

    // Sanity: the server really is in GTID mode, or the rest of the test proves nothing.
    let gtid_mode: (String, String) = sqlx::query_as("SHOW VARIABLES LIKE 'gtid_mode'")
        .fetch_one(&pool)
        .await
        .map_err(source_error)?;
    assert_eq!(
        gtid_mode.1, "ON",
        "the container must run with gtid_mode=ON or this test cannot exercise the bracket"
    );

    let mut connection = MysqlConnection::new(MysqlSourceConfig {
        host: host.clone(),
        port,
        user: "root".into(),
        password: "rootpass".to_string().into(),
        database: "cdc".into(),
        server_id: 4242,
        gtid_mode_enabled: true,
        transport: rustcdc::TransportConfig::plaintext(),
        stream_poll_interval_ms: 50,
        ..MysqlSourceConfig::default()
    });
    connection.connect().await?;
    let mut stream = connection.start_stream(None).await?;

    // ── The bracket, in order ─────────────────────────────────────────────────
    let low = executed_gtid_set(&pool).await?;

    sqlx::query("INSERT INTO gtid_probe (id, v) VALUES (1, 'inside')")
        .execute(&pool)
        .await
        .map_err(source_error)?;

    let high = executed_gtid_set(&pool).await?;
    assert_ne!(
        low, high,
        "committing a row must advance Executed_Gtid_Set, or the watermark carries no signal"
    );

    // ── The join no unit test can establish ───────────────────────────────────
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    let mut event_gtid = None;
    while event_gtid.is_none() && std::time::Instant::now() < deadline {
        for event in stream.next_events(500).await? {
            if event.table == "gtid_probe" && event.op == Operation::Insert {
                event_gtid = event
                    .source
                    .offset
                    .split_once("#gtid=")
                    .map(|(_, gtid)| gtid.to_owned());
            }
        }
    }

    let event_gtid = event_gtid.expect(
        "the insert must be delivered with a #gtid= suffix. Without one the connector has no id \
         to test against the executed set, and the bracket silently falls back to the ordinal \
         test it replaced.",
    );

    assert!(
        set_contains(&high, &event_gtid),
        "the event's GTID must be in the high watermark's executed set. If it is not, the two id \
         spaces do not line up and set membership never matches — the bracket degrades to the \
         ordinal test with every unit test still green.\n  event gtid: {event_gtid}\n  \
         high: {high}"
    );
    assert!(
        !set_contains(&low, &event_gtid),
        "the event's GTID must be absent from the low watermark's set: it committed after that \
         watermark was read, so the chunk read could not have seen it.\n  event gtid: \
         {event_gtid}\n  low: {low}"
    );

    Ok(())
}
