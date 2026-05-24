#![cfg(feature = "mysql")]

use cdc_rs::{
    checkpoint::{Checkpoint, FileCheckpoint},
    source::Source,
    MysqlConnection, MysqlSourceConfig, TransportConfig,
};
use std::collections::HashSet;
use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    GenericImage, ImageExt,
};
use tokio::time::{sleep, Duration};

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
        "failed to connect mariadb admin pool: {}",
        last_error
            .map(|error| error.to_string())
            .unwrap_or_else(|| "unknown error".to_string())
    )))
}

fn json_object_get<'a>(
    value: &'a serde_json::Value,
    keys: &[&str],
) -> Option<&'a serde_json::Value> {
    let object = value.as_object()?;
    keys.iter().find_map(|key| object.get(*key))
}

fn json_i64_field(value: &serde_json::Value, keys: &[&str]) -> Option<i64> {
    let field = json_object_get(value, keys)?;
    field
        .as_i64()
        .or_else(|| field.as_str()?.parse::<i64>().ok())
}

fn docker_tests_enabled() -> bool {
    std::env::var("CDC_RS_RUN_DOCKER_TESTS").as_deref() == Ok("1")
}

fn skip_mariadb_e2e_case(case_label: &str) -> bool {
    if docker_tests_enabled() {
        return false;
    }

    eprintln!("skipping {case_label} (set CDC_RS_RUN_DOCKER_TESTS=1)",);
    true
}

macro_rules! mariadb_e2e_test {
    ($name:ident, $version:literal, $server_id:expr, $runner:ident, $label:literal) => {
        #[tokio::test]
        async fn $name() -> cdc_rs::Result<()> {
            if skip_mariadb_e2e_case($label) {
                return Ok(());
            }
            $runner($version, $server_id).await
        }
    };
}

async fn run_mariadb_snapshot_resume_from_checkpoint(
    version: &str,
    server_id: u32,
) -> cdc_rs::Result<()> {
    let container = GenericImage::new("mariadb", version)
        .with_exposed_port(3306.tcp())
        .with_wait_for(WaitFor::message_on_stderr("ready for connections"))
        .with_cmd(vec![
            "--log-bin=mysql-bin",
            "--binlog-format=ROW",
            "--server-id=1",
        ])
        .with_env_var("MYSQL_ROOT_PASSWORD", "rootpass")
        .with_env_var("MYSQL_DATABASE", "cdc")
        .start()
        .await
        .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;

    let host = container
        .get_host()
        .await
        .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?
        .to_string();
    let port = container
        .get_host_port_ipv4(3306.tcp())
        .await
        .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;

    let admin_dsn = format!("mysql://root:rootpass@{host}:{port}/cdc");
    let admin_pool = connect_admin_pool(&admin_dsn).await?;

    sqlx::query("DROP TABLE IF EXISTS mariadb_resumption_test")
        .execute(&admin_pool)
        .await
        .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;

    sqlx::query(
        "CREATE TABLE mariadb_resumption_test (
            id BIGINT PRIMARY KEY AUTO_INCREMENT,
            value VARCHAR(255)
        ) ENGINE=InnoDB",
    )
    .execute(&admin_pool)
    .await
    .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;

    for batch_start in (1..=5000).step_by(500) {
        let mut query = String::from("INSERT INTO mariadb_resumption_test (value) VALUES ");
        let mut first = true;
        for i in batch_start..std::cmp::min(batch_start + 500, 5001) {
            if !first {
                query.push(',');
            }
            query.push_str(&format!("('row-{i}')"));
            first = false;
        }
        sqlx::query(&query)
            .execute(&admin_pool)
            .await
            .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;
    }

    let checkpoint_dir = tempfile::tempdir().map_err(cdc_rs::Error::IoError)?;
    let mut checkpoint = FileCheckpoint::new(checkpoint_dir.path());

    let config = MysqlSourceConfig {
        host: host.clone(),
        port,
        user: "root".to_string(),
        password: "rootpass".to_string().into(),
        database: "cdc".to_string(),
        server_id,
        gtid_mode_enabled: false,
        binlog_format_check: true,
        transport: TransportConfig::tls(),
        conn_timeout_secs: 30,
        stream_poll_interval_ms: 50,
        max_events_per_poll: 1_000,
    };

    let mut connection_1 = MysqlConnection::new(config.clone());
    connection_1.connect().await?;
    let mut snapshot_1 = connection_1
        .start_snapshot(&["mariadb_resumption_test"])
        .await?;

    let first_batch = snapshot_1.next_chunk(1000).await?;
    assert!(!first_batch.is_empty(), "expected initial snapshot batch");
    snapshot_1
        .checkpoint(&mut checkpoint, first_batch.len() as u64)
        .await?;
    let resume_offset = checkpoint.load().await?.ok_or_else(|| {
        cdc_rs::Error::CheckpointError("expected checkpoint offset for mariadb snapshot".into())
    })?;

    drop(snapshot_1);
    connection_1.close().await;

    let mut connection_2 = MysqlConnection::new(config);
    connection_2.connect().await?;
    let mut resumed_snapshot = connection_2
        .start_snapshot_from_checkpoint(&["mariadb_resumption_test"], Some(resume_offset.as_ref()))
        .await?;

    let mut resumed_events = Vec::new();
    loop {
        let chunk = resumed_snapshot.next_chunk(1000).await?;
        if chunk.is_empty() {
            break;
        }
        resumed_events.extend(chunk);
    }

    let _snapshot_end = resumed_snapshot.finish().await?;

    let mut ids = HashSet::new();
    for event in first_batch.iter().chain(resumed_events.iter()) {
        let after = event.after.as_ref().ok_or_else(|| {
            cdc_rs::Error::SourceError("snapshot row missing after payload".into())
        })?;
        let id = json_i64_field(after, &["id", "@0", "@1"])
            .ok_or_else(|| cdc_rs::Error::SourceError("snapshot row missing id".into()))?;
        assert!(ids.insert(id), "duplicate id across resumed snapshot: {id}");
    }

    assert_eq!(
        ids.len(),
        5000,
        "expected 5K unique rows after resumed snapshot"
    );

    connection_2.close().await;
    Ok(())
}

async fn run_mariadb_stream_capture_insert_update_delete(
    version: &str,
    server_id: u32,
) -> cdc_rs::Result<()> {
    let container = GenericImage::new("mariadb", version)
        .with_exposed_port(3306.tcp())
        .with_wait_for(WaitFor::message_on_stderr("ready for connections"))
        .with_cmd(vec![
            "--log-bin=mysql-bin",
            "--binlog-format=ROW",
            "--server-id=1",
        ])
        .with_env_var("MYSQL_ROOT_PASSWORD", "rootpass")
        .with_env_var("MYSQL_DATABASE", "cdc")
        .start()
        .await
        .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;

    let host = container
        .get_host()
        .await
        .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?
        .to_string();
    let port = container
        .get_host_port_ipv4(3306.tcp())
        .await
        .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;

    let admin_dsn = format!("mysql://root:rootpass@{host}:{port}/cdc");
    let admin_pool = connect_admin_pool(&admin_dsn).await?;

    sqlx::query("DROP TABLE IF EXISTS mariadb_stream_test")
        .execute(&admin_pool)
        .await
        .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;

    sqlx::query(
        "CREATE TABLE mariadb_stream_test (
            id BIGINT PRIMARY KEY AUTO_INCREMENT,
            value VARCHAR(255),
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        ) ENGINE=InnoDB",
    )
    .execute(&admin_pool)
    .await
    .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;

    let config = MysqlSourceConfig {
        host,
        port,
        user: "root".to_string(),
        password: "rootpass".to_string().into(),
        database: "cdc".to_string(),
        server_id,
        gtid_mode_enabled: false,
        binlog_format_check: true,
        transport: TransportConfig::tls(),
        conn_timeout_secs: 30,
        stream_poll_interval_ms: 50,
        max_events_per_poll: 1_000,
    };

    let mut connection = MysqlConnection::new(config);
    connection.connect().await?;
    let mut stream = connection.start_stream(None).await?;

    for i in 1..=40 {
        sqlx::query("INSERT INTO mariadb_stream_test (value) VALUES (?)")
            .bind(format!("insert-{i}"))
            .execute(&admin_pool)
            .await
            .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;
    }
    for i in 1..=15 {
        sqlx::query("UPDATE mariadb_stream_test SET value = ? WHERE id = ?")
            .bind(format!("update-{i}"))
            .bind(i)
            .execute(&admin_pool)
            .await
            .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;
    }
    for i in 31..=40 {
        sqlx::query("DELETE FROM mariadb_stream_test WHERE id = ?")
            .bind(i)
            .execute(&admin_pool)
            .await
            .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;
    }

    let mut events = Vec::new();
    for _ in 0..120 {
        let batch = stream.next_events(400).await?;
        if batch.is_empty() {
            sleep(Duration::from_millis(100)).await;
        } else {
            events.extend(batch);
        }
        if events.len() >= 65 {
            break;
        }
    }

    let mut inserts = 0;
    let mut updates = 0;
    let mut deletes = 0;
    for event in &events {
        match event.op {
            cdc_rs::core::Operation::Insert => inserts += 1,
            cdc_rs::core::Operation::Update => updates += 1,
            cdc_rs::core::Operation::Delete => deletes += 1,
            _ => {}
        }
    }

    assert!(
        inserts >= 40,
        "expected at least 40 insert events, got {inserts}"
    );
    assert!(
        updates >= 15,
        "expected at least 15 update events, got {updates}"
    );
    assert!(
        deletes >= 10,
        "expected at least 10 delete events, got {deletes}"
    );

    connection.close().await;
    Ok(())
}

async fn run_mariadb_snapshot_stream_handoff_full_cycle(
    version: &str,
    server_id: u32,
) -> cdc_rs::Result<()> {
    let container = GenericImage::new("mariadb", version)
        .with_exposed_port(3306.tcp())
        .with_wait_for(WaitFor::message_on_stderr("ready for connections"))
        .with_cmd(vec![
            "--log-bin=mysql-bin",
            "--binlog-format=ROW",
            "--server-id=1",
        ])
        .with_env_var("MYSQL_ROOT_PASSWORD", "rootpass")
        .with_env_var("MYSQL_DATABASE", "cdc")
        .start()
        .await
        .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;

    let host = container
        .get_host()
        .await
        .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?
        .to_string();
    let port = container
        .get_host_port_ipv4(3306.tcp())
        .await
        .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;

    let admin_dsn = format!("mysql://root:rootpass@{host}:{port}/cdc");
    let admin_pool = connect_admin_pool(&admin_dsn).await?;

    sqlx::query("DROP TABLE IF EXISTS mariadb_handoff_test")
        .execute(&admin_pool)
        .await
        .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;

    sqlx::query(
        "CREATE TABLE mariadb_handoff_test (
            id BIGINT PRIMARY KEY AUTO_INCREMENT,
            value VARCHAR(255)
        ) ENGINE=InnoDB",
    )
    .execute(&admin_pool)
    .await
    .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;

    for batch_start in (1..=1000).step_by(100) {
        let mut query = String::from("INSERT INTO mariadb_handoff_test (value) VALUES ");
        let mut first = true;
        for i in batch_start..std::cmp::min(batch_start + 100, 1001) {
            if !first {
                query.push(',');
            }
            query.push_str(&format!("('initial-{i}')"));
            first = false;
        }
        sqlx::query(&query)
            .execute(&admin_pool)
            .await
            .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;
    }

    let config = MysqlSourceConfig {
        host,
        port,
        user: "root".to_string(),
        password: "rootpass".to_string().into(),
        database: "cdc".to_string(),
        server_id,
        gtid_mode_enabled: false,
        binlog_format_check: true,
        transport: TransportConfig::tls(),
        conn_timeout_secs: 30,
        stream_poll_interval_ms: 50,
        max_events_per_poll: 1_000,
    };

    let mut connection = MysqlConnection::new(config.clone());
    connection.connect().await?;
    let mut snapshot = connection.start_snapshot(&["mariadb_handoff_test"]).await?;

    let mut snapshot_events = Vec::new();
    loop {
        let batch = snapshot.next_chunk(5000).await?;
        if batch.is_empty() {
            break;
        }
        snapshot_events.extend(batch);
        if snapshot_events.len() >= 1000 {
            break;
        }
    }
    let _end = snapshot.finish().await?;
    connection.close().await;

    let mut resumed = MysqlConnection::new(config);
    resumed.connect().await?;
    let mut stream = resumed.start_stream(None).await?;

    for batch_start in (1001..=1100).step_by(25) {
        let mut query = String::from("INSERT INTO mariadb_handoff_test (value) VALUES ");
        let mut first = true;
        for i in batch_start..std::cmp::min(batch_start + 25, 1101) {
            if !first {
                query.push(',');
            }
            query.push_str(&format!("('post-handoff-{i}')"));
            first = false;
        }
        sqlx::query(&query)
            .execute(&admin_pool)
            .await
            .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;
    }

    let mut stream_events = Vec::new();
    for _ in 0..120 {
        let batch = stream.next_events(400).await?;
        if batch.is_empty() {
            sleep(Duration::from_millis(100)).await;
        } else {
            stream_events.extend(batch);
        }
        if stream_events.len() >= 100 {
            break;
        }
    }

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

    let expected_post_handoff: HashSet<String> = (1001..=1100)
        .map(|id| format!("post-handoff-{id}"))
        .collect();
    let missing_post_handoff: Vec<String> = expected_post_handoff
        .difference(&stream_values)
        .cloned()
        .collect();

    assert!(
        missing_post_handoff.is_empty(),
        "stream missed post-handoff rows: {:?}",
        missing_post_handoff
    );
    assert_eq!(
        snapshot_values.intersection(&stream_values).count(),
        0,
        "snapshot/stream overlap detected across handoff"
    );

    resumed.close().await;
    Ok(())
}

mariadb_e2e_test!(
    mariadb_10_5_snapshot_resume_from_checkpoint,
    "10.5",
    510,
    run_mariadb_snapshot_resume_from_checkpoint,
    "mariadb 10.5 snapshot resume test"
);
mariadb_e2e_test!(
    mariadb_10_5_stream_capture_insert_update_delete,
    "10.5",
    511,
    run_mariadb_stream_capture_insert_update_delete,
    "mariadb 10.5 stream CDC test"
);
mariadb_e2e_test!(
    mariadb_10_5_snapshot_stream_handoff_full_cycle,
    "10.5",
    512,
    run_mariadb_snapshot_stream_handoff_full_cycle,
    "mariadb 10.5 handoff test"
);
mariadb_e2e_test!(
    mariadb_10_6_snapshot_resume_from_checkpoint,
    "10.6",
    610,
    run_mariadb_snapshot_resume_from_checkpoint,
    "mariadb 10.6 snapshot resume test"
);
mariadb_e2e_test!(
    mariadb_10_6_stream_capture_insert_update_delete,
    "10.6",
    611,
    run_mariadb_stream_capture_insert_update_delete,
    "mariadb 10.6 stream CDC test"
);
mariadb_e2e_test!(
    mariadb_10_6_snapshot_stream_handoff_full_cycle,
    "10.6",
    612,
    run_mariadb_snapshot_stream_handoff_full_cycle,
    "mariadb 10.6 handoff test"
);
