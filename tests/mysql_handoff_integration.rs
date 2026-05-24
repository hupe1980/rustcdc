#![cfg(feature = "mysql")]

use cdc_rs::{source::Source, MysqlConnection, MysqlSourceConfig};
use cdc_rs::TransportConfig;
use std::collections::HashSet;
use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    GenericImage, ImageExt,
};
use tokio::time::{sleep, Duration};

fn json_object_get<'a>(
    value: &'a serde_json::Value,
    keys: &[&str],
) -> Option<&'a serde_json::Value> {
    let object = value.as_object()?;
    keys.iter().find_map(|key| object.get(*key))
}

async fn connect_admin_pool(dsn: &str) -> cdc_rs::Result<sqlx::MySqlPool> {
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

    Err(cdc_rs::Error::SourceError(format!(
        "failed to connect mysql admin pool: {}",
        last_error
            .map(|error| error.to_string())
            .unwrap_or_else(|| "unknown error".to_string())
    )))
}

/// Test complete snapshot-to-stream handoff cycle
/// Validates: snapshot completion → stream start → no gaps or duplicates
#[tokio::test]
async fn mysql_snapshot_stream_handoff_full_cycle() -> cdc_rs::Result<()> {
    if std::env::var("CDC_RS_RUN_DOCKER_TESTS").as_deref() != Ok("1") {
        eprintln!("skipping mysql handoff test (set CDC_RS_RUN_DOCKER_TESTS=1)");
        return Ok(());
    }

    let container = GenericImage::new("mysql", "8.0")
        .with_exposed_port(3306.tcp())
        .with_wait_for(WaitFor::message_on_stderr("ready for connections"))
        .with_env_var("MYSQL_ROOT_PASSWORD", "rootpass")
        .with_env_var("MYSQL_DATABASE", "cdc")
        .start()
        .await
        .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;

    let host = container
        .get_host()
        .await
        .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;
    let port = container
        .get_host_port_ipv4(3306.tcp())
        .await
        .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;

    let admin_dsn = format!("mysql://root:rootpass@{host}:{port}/cdc");
    let admin_pool = connect_admin_pool(&admin_dsn).await?;

    // Setup
    sqlx::query("DROP TABLE IF EXISTS handoff_test")
        .execute(&admin_pool)
        .await
        .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;

    sqlx::query(
        "CREATE TABLE handoff_test (
            id BIGINT PRIMARY KEY AUTO_INCREMENT,
            value VARCHAR(255)
        ) ENGINE=InnoDB",
    )
    .execute(&admin_pool)
    .await
    .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;

    // Insert initial snapshot data (1K rows)
    for batch_start in (1..=1000).step_by(100) {
        let mut query = String::from("INSERT INTO handoff_test (value) VALUES ");
        let mut first = true;
        for i in batch_start..std::cmp::min(batch_start + 100, 1001) {
            if !first {
                query.push(',');
            }
            query.push_str(&format!("('initial-{}')", i));
            first = false;
        }
        sqlx::query(&query)
            .execute(&admin_pool)
            .await
            .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;
    }

    let _checkpoint_dir = tempfile::tempdir().map_err(cdc_rs::Error::IoError)?;
    let config = MysqlSourceConfig {
        host: host.to_string(),
        port,
        user: "root".to_string(),
        password: "rootpass".to_string().into(),
        database: "cdc".to_string(),
        server_id: 300,
        gtid_mode_enabled: false,
        binlog_format_check: true,
        transport: TransportConfig::tls(),
        conn_timeout_secs: 30,
        stream_poll_interval_ms: 50,
        max_events_per_poll: 1_000,
    };

    // Phase 1: Snapshot capture
    let mut connection = MysqlConnection::new(config.clone());
    connection.connect().await?;
    let mut snapshot_handle = connection.start_snapshot(&["handoff_test"]).await?;

    // Capture all snapshot events
    let mut snapshot_events = Vec::new();
    loop {
        let chunk = snapshot_handle.next_chunk(5000).await?;
        if chunk.is_empty() {
            break;
        }
        snapshot_events.extend(chunk);
        if snapshot_events.len() >= 1000 {
            break;
        }
    }

    let _snapshot_end = snapshot_handle.finish().await?;
    let snapshot_count = snapshot_events.len();
    println!("Phase 1 (Snapshot): Captured {} events", snapshot_count);
    assert!(
        snapshot_count >= 1000,
        "expected at least 1K snapshot events, got {}",
        snapshot_count
    );

    drop(connection);

    // Phase 2: Stream after snapshot (new changes)
    // Resume runtime (handoff → stream) first, then produce post-handoff writes.
    let mut resumed = MysqlConnection::new(config);
    resumed.connect().await?;
    let mut stream_handle = resumed.start_stream(None).await?;

    for batch_start in (1001..=1100).step_by(50) {
        let mut query = String::from("INSERT INTO handoff_test (value) VALUES ");
        let mut first = true;
        for i in batch_start..std::cmp::min(batch_start + 50, 1101) {
            if !first {
                query.push(',');
            }
            query.push_str(&format!("('post-handoff-{}')", i));
            first = false;
        }
        sqlx::query(&query)
            .execute(&admin_pool)
            .await
            .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;
    }

    // Capture stream events (post-handoff)
    let mut stream_events = Vec::new();
    for _ in 0..100 {
        let mut events = stream_handle.next_events(500).await?;
        if events.is_empty() {
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            events = stream_handle.next_events(500).await?;
            if events.is_empty() {
                break;
            }
        }
        stream_events.extend(events);
        if stream_events.len() >= 100 {
            break;
        }
    }

    let stream_count = stream_events.len();
    println!(
        "Phase 2 (Stream): Captured {} events (expected ~100 inserts)",
        stream_count
    );

    // Validate handoff invariants: no overlap and no missing post-handoff inserts.
    let snapshot_values: HashSet<String> = snapshot_events
        .iter()
        .filter_map(|event| event.after.as_ref())
        .filter_map(|after| json_object_get(after, &["value", "@1"]))
        .filter_map(|value| value.as_str().map(ToString::to_string))
        .collect();

    let stream_values: HashSet<String> = stream_events
        .iter()
        .filter_map(|event| event.after.as_ref())
        .filter_map(|after| json_object_get(after, &["value", "@1"]))
        .filter_map(|value| value.as_str().map(ToString::to_string))
        .collect();

    assert_eq!(
        stream_values.len(),
        100,
        "expected 100 unique stream values from post-handoff inserts, got {}",
        stream_values.len()
    );

    assert!(
        snapshot_values
            .iter()
            .all(|value| value.starts_with("initial-")),
        "snapshot should contain only pre-handoff value payloads"
    );
    assert!(
        stream_values
            .iter()
            .all(|value| value.starts_with("post-handoff-")),
        "stream should contain only post-handoff value payloads"
    );

    let overlap_count = snapshot_values.intersection(&stream_values).count();
    assert_eq!(
        overlap_count, 0,
        "snapshot/stream overlap detected: {overlap_count} duplicate values across handoff"
    );

    let expected_post_handoff: HashSet<String> = (1001..=1100)
        .map(|id| format!("post-handoff-{id}"))
        .collect();
    let missing_post_handoff: Vec<String> = expected_post_handoff
        .difference(&stream_values)
        .cloned()
        .collect();
    assert!(
        missing_post_handoff.is_empty(),
        "stream missed post-handoff values: {:?}",
        missing_post_handoff
    );

    println!(
        "✓ Handoff test: snapshot {} events → stream {} events (total {})",
        snapshot_count,
        stream_count,
        snapshot_count + stream_count
    );

    resumed.close().await;
    Ok(())
}
