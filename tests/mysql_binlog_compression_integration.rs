//! Capture correctness with `binlog_transaction_compression = ON`.
//!
//! MySQL 8.0.20 added transaction compression: the server writes a whole transaction as
//! a single zstd `Transaction_payload_event` instead of a sequence of individual events.
//! The driver decompresses it transparently and hands back the inner `BEGIN` /
//! `TABLE_MAP` / rows / `XID` events — but those inner headers carry `log_pos = 0`,
//! because they were never written to the binlog file individually and so have no
//! position of their own. MySQL's own rule is that the resume coordinate for anything
//! inside a compressed transaction is the **end position of the payload event**.
//!
//! Taking the inner zero at face value made every commit inside a compressed transaction
//! checkpoint at `<file>:0`. That is not a coordinate the server will accept — it
//! rejects a dump request below position 4 outright — so a restart after any compressed
//! transaction could not resume at all. The checkpoint's monotonicity guard did not catch
//! it either, because the committed-event count still advanced.
//!
//! Two properties are asserted here, both against a live server:
//!
//! 1. Every captured event carries a usable binlog position (`> 0`).
//! 2. A fresh stream resumed from a position captured inside a compressed transaction
//!    picks up the changes that follow it — no gap, no failure to start.

#![cfg(feature = "mysql")]

use rustcdc::{
    checkpoint::MysqlOffset, source::Source, MysqlConnection, MysqlSourceConfig, TransportConfig,
};
use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    GenericImage, ImageExt,
};
use tokio::time::{sleep, Duration};

async fn connect_admin_pool(dsn: &str) -> rustcdc::Result<sqlx::MySqlPool> {
    let mut last_error = None;
    for _ in 0..30 {
        match sqlx::mysql::MySqlPoolOptions::new()
            .max_connections(1)
            .connect(dsn)
            .await
        {
            Ok(pool) => return Ok(pool),
            Err(error) => {
                last_error = Some(error);
                sleep(Duration::from_millis(500)).await;
            }
        }
    }

    Err(rustcdc::Error::SourceError(format!(
        "failed to connect mysql admin pool: {}",
        last_error
            .map(|error| error.to_string())
            .unwrap_or_else(|| "unknown error".to_string())
    )))
}

/// Split `"<file>:<pos>"` (optionally followed by `#gtid=...`) into its parts.
fn parse_offset(offset: &str) -> (String, u32) {
    let coordinate = offset.split_once("#gtid=").map_or(offset, |(head, _)| head);
    let (file, position) = coordinate
        .rsplit_once(':')
        .unwrap_or_else(|| panic!("mysql offset must be '<file>:<pos>', got {offset:?}"));
    (
        file.to_string(),
        position
            .parse()
            .unwrap_or_else(|_| panic!("mysql offset position must be numeric, got {offset:?}")),
    )
}

/// Drain the stream until it goes quiet twice or `want` events have arrived.
async fn drain(
    stream: &mut Box<dyn rustcdc::source::StreamHandle>,
    want: usize,
) -> rustcdc::Result<Vec<rustcdc::Event>> {
    let mut collected = Vec::new();
    let mut quiet = 0;
    for _ in 0..60 {
        let events = stream.next_events(500).await?;
        if events.is_empty() {
            quiet += 1;
            if quiet == 2 {
                break;
            }
            sleep(Duration::from_millis(100)).await;
            continue;
        }
        quiet = 0;
        collected.extend(events);
        if collected.len() >= want {
            break;
        }
    }
    Ok(collected)
}

#[tokio::test]
async fn compressed_transactions_keep_a_resumable_binlog_position() -> rustcdc::Result<()> {
    if std::env::var("CDC_RS_RUN_DOCKER_TESTS").as_deref() != Ok("1") {
        eprintln!(
            "skipping mysql binlog-compression integration test (set CDC_RS_RUN_DOCKER_TESTS=1)"
        );
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
            // The configuration under test. `ZSTD` is the only supported algorithm.
            "--binlog-transaction-compression=ON",
            "--binlog-transaction-compression-level-zstd=3",
        ])
        .start()
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

    let host = container
        .get_host()
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;
    let port = container
        .get_host_port_ipv4(3306.tcp())
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

    let admin_pool =
        connect_admin_pool(&format!("mysql://root:rootpass@{host}:{port}/cdc")).await?;

    // Confirm the server really is compressing, so a silent failure to enable the option
    // cannot turn this into a test that passes for the wrong reason.
    let compression: (String,) =
        sqlx::query_as("SELECT @@GLOBAL.binlog_transaction_compression_level_zstd, 1")
            .fetch_one(&admin_pool)
            .await
            .map(|(level, _): (u32, i32)| (level.to_string(),))
            .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;
    let enabled: (bool,) = sqlx::query_as("SELECT @@GLOBAL.binlog_transaction_compression")
        .fetch_one(&admin_pool)
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;
    assert!(
        enabled.0,
        "the server must have transaction compression on for this test to mean anything \
         (zstd level {})",
        compression.0
    );

    sqlx::query(
        "CREATE TABLE compressed (
            id BIGINT PRIMARY KEY AUTO_INCREMENT,
            payload TEXT
        ) ENGINE=InnoDB",
    )
    .execute(&admin_pool)
    .await
    .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

    let config = MysqlSourceConfig {
        host: host.to_string(),
        port,
        user: "root".to_string(),
        password: "rootpass".to_string().into(),
        database: "cdc".to_string(),
        server_id: 231,
        // Deliberately file+position, not GTID: that is the default, and the coordinate
        // the defect corrupted. A GTID-positioned stream was shielded by its GTID set.
        gtid_mode_enabled: false,
        transport: TransportConfig::tls_insecure_skip_verify(),
        conn_timeout_secs: 30,
        stream_poll_interval_ms: 50,
        max_events_per_poll: 1_000,
        ..Default::default()
    };

    let mut connection = MysqlConnection::new(config.clone());
    connection.connect().await?;
    let mut stream = connection.start_stream(None).await?;

    // One multi-row transaction with enough repetitive payload that zstd is a clear win,
    // so the server chooses the compressed representation.
    let mut tx = admin_pool
        .begin()
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;
    for _ in 1..=20 {
        sqlx::query("INSERT INTO compressed (payload) VALUES (?)")
            .bind("x".repeat(4_096))
            .execute(&mut *tx)
            .await
            .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;
    }
    tx.commit()
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

    let events = drain(&mut stream, 20).await?;
    assert_eq!(
        events.len(),
        20,
        "every row of the compressed transaction must be captured"
    );

    for event in &events {
        let (file, position) = parse_offset(&event.source.offset);
        assert!(
            position > 0,
            "an event unpacked from a compressed transaction payload must carry the \
             payload's end position, not the inner header's zero. Got {file}:{position} \
             — a restart from position 0 is rejected by the server outright."
        );
        assert!(
            position >= 4,
            "binlog positions below 4 are inside the file magic and cannot be resumed \
             from: {file}:{position}"
        );
    }

    // The offset the runtime would have made durable for the last committed row.
    let (resume_file, resume_position) = parse_offset(&events[events.len() - 1].source.offset);

    // Prove the coordinate is actually resumable: a fresh stream started from it must
    // pick up what follows and must not fail to start.
    sqlx::query("INSERT INTO compressed (payload) VALUES ('after-restart')")
        .execute(&admin_pool)
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

    let mut resumed_connection = MysqlConnection::new(config);
    resumed_connection.connect().await?;
    let resume_offset = MysqlOffset::new(
        "mysql".to_string(),
        resume_file.clone(),
        resume_position,
        String::new(),
    );
    let mut resumed = resumed_connection
        .start_stream(Some(&resume_offset))
        .await?;

    let after_restart = drain(&mut resumed, 1).await?;
    let payloads: Vec<String> = after_restart
        .iter()
        .filter_map(|event| event.after.as_ref()?.get("payload")?.as_str())
        .map(ToString::to_string)
        .collect();
    assert!(
        payloads.iter().any(|value| value == "after-restart"),
        "a stream resumed from {resume_file}:{resume_position} must deliver the change \
         written after it; got {payloads:?}"
    );

    Ok(())
}
