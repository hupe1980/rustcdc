#![cfg(feature = "mariadb")]

#[path = "support/matrix.rs"]
mod matrix;

use rustcdc::{
    MariaDbConnection, MariaDbSourceConfig, MysqlSourceConfig, TransportConfig,
    checkpoint::{Checkpoint, FileCheckpoint},
    source::Source,
};
use std::collections::HashSet;
use testcontainers::{
    GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};
use tokio::time::{Duration, sleep};

#[path = "rustls_provider_common.rs"]
mod rustls_provider_common;
use rustls_provider_common::install_rustls_provider;

async fn connect_admin_pool(dsn: &str) -> rustcdc::Result<sqlx::MySqlPool> {
    install_rustls_provider();
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

async fn run_mariadb_snapshot_resume_from_checkpoint(
    version: &str,
    server_id: u32,
) -> rustcdc::Result<()> {
    install_rustls_provider();
    let container = GenericImage::new("mariadb", version)
        .with_exposed_port(3306.tcp())
        .with_wait_for(WaitFor::message_on_stderr("ready for connections"))
        .with_cmd(vec![
            "--log-bin=mysql-bin",
            "--binlog-format=ROW",
            // MariaDB defaults binlog_row_metadata to NO_LOG, MySQL to MINIMAL —
            // neither carries column names or primary-key flags, so events would be
            // emitted with positional placeholder keys and no key. connect() rejects
            // that, so the test server must match a valid production configuration.
            "--binlog-row-metadata=FULL",
            "--binlog-row-image=FULL",
            "--server-id=1",
        ])
        .with_env_var("MYSQL_ROOT_PASSWORD", "rootpass")
        .with_env_var("MYSQL_DATABASE", "cdc")
        .start()
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

    let host = container
        .get_host()
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?
        .to_string();
    let port = container
        .get_host_port_ipv4(3306.tcp())
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

    let admin_dsn = format!("mysql://root:rootpass@{host}:{port}/cdc");
    let admin_pool = connect_admin_pool(&admin_dsn).await?;

    sqlx::query("DROP TABLE IF EXISTS mariadb_resumption_test")
        .execute(&admin_pool)
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

    sqlx::query(
        "CREATE TABLE mariadb_resumption_test (
            id BIGINT PRIMARY KEY AUTO_INCREMENT,
            value VARCHAR(255)
        ) ENGINE=InnoDB",
    )
    .execute(&admin_pool)
    .await
    .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

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
        sqlx::query(sqlx::AssertSqlSafe(query.as_str()))
            .execute(&admin_pool)
            .await
            .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;
    }

    let checkpoint_dir = tempfile::tempdir().map_err(rustcdc::Error::IoError)?;
    let mut checkpoint = FileCheckpoint::new(checkpoint_dir.path());

    let config = MariaDbSourceConfig::default();
    let config = {
        let mut c = config;
        c.host = host.clone();
        c.port = port;
        c.user = "root".to_string();
        c.password = "rootpass".to_string().into();
        c.database = "cdc".to_string();
        c.server_id = server_id;
        c.gtid_mode_enabled = false;
        c.binlog_format_check = true;
        c.transport = TransportConfig::plaintext();
        c.conn_timeout_secs = 30;
        c.stream_poll_interval_ms = 50;
        c.max_events_per_poll = 1_000;
        c
    };

    let mut connection_1 = MariaDbConnection::new(config.clone().into_inner());
    connection_1.connect().await?;
    let mut snapshot_1 = connection_1
        .start_snapshot(&["mariadb_resumption_test"])
        .await?;

    let first_batch = (snapshot_1.next_chunk(1000).await?)
        .into_iter()
        .filter(|event| !event.op.is_schema_change())
        .collect::<Vec<_>>();
    assert!(!first_batch.is_empty(), "expected initial snapshot batch");
    snapshot_1
        .checkpoint(&mut checkpoint, first_batch.len() as u64)
        .await?;
    let resume_offset = checkpoint.load().await?.ok_or_else(|| {
        rustcdc::Error::CheckpointError("expected checkpoint offset for mariadb snapshot".into())
    })?;

    drop(snapshot_1);
    connection_1.close().await;

    let mut connection_2 = MariaDbConnection::new(config.into_inner());
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
        let chunk = chunk
            .into_iter()
            .filter(|event| !event.op.is_schema_change())
            .collect::<Vec<_>>();
        resumed_events.extend(chunk);
    }

    let _snapshot_end = resumed_snapshot.finish().await?;

    let mut ids = HashSet::new();
    for event in first_batch.iter().chain(resumed_events.iter()) {
        let after = event.after.as_ref().ok_or_else(|| {
            rustcdc::Error::SourceError("snapshot row missing after payload".into())
        })?;
        let id = json_i64_field(after, &["id", "@0", "@1"])
            .ok_or_else(|| rustcdc::Error::SourceError("snapshot row missing id".into()))?;
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
) -> rustcdc::Result<()> {
    install_rustls_provider();
    let container = GenericImage::new("mariadb", version)
        .with_exposed_port(3306.tcp())
        .with_wait_for(WaitFor::message_on_stderr("ready for connections"))
        .with_cmd(vec![
            "--log-bin=mysql-bin",
            "--binlog-format=ROW",
            // MariaDB defaults binlog_row_metadata to NO_LOG, MySQL to MINIMAL —
            // neither carries column names or primary-key flags, so events would be
            // emitted with positional placeholder keys and no key. connect() rejects
            // that, so the test server must match a valid production configuration.
            "--binlog-row-metadata=FULL",
            "--binlog-row-image=FULL",
            "--server-id=1",
        ])
        .with_env_var("MYSQL_ROOT_PASSWORD", "rootpass")
        .with_env_var("MYSQL_DATABASE", "cdc")
        .start()
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

    let host = container
        .get_host()
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?
        .to_string();
    let port = container
        .get_host_port_ipv4(3306.tcp())
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

    let admin_dsn = format!("mysql://root:rootpass@{host}:{port}/cdc");
    let admin_pool = connect_admin_pool(&admin_dsn).await?;

    sqlx::query("DROP TABLE IF EXISTS mariadb_stream_test")
        .execute(&admin_pool)
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

    sqlx::query(
        "CREATE TABLE mariadb_stream_test (
            id BIGINT PRIMARY KEY AUTO_INCREMENT,
            value VARCHAR(255),
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        ) ENGINE=InnoDB",
    )
    .execute(&admin_pool)
    .await
    .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

    let config = MariaDbSourceConfig::new(MysqlSourceConfig {
        host,
        port,
        user: "root".to_string(),
        password: "rootpass".to_string().into(),
        database: "cdc".to_string(),
        server_id,
        gtid_mode_enabled: false,
        binlog_format_check: true,
        transport: TransportConfig::plaintext(),
        conn_timeout_secs: 30,
        stream_poll_interval_ms: 50,
        max_events_per_poll: 1_000,
        ..Default::default()
    });

    let mut connection = MariaDbConnection::new(config.into_inner());
    connection.connect().await?;
    let mut stream = connection.start_stream(None).await?;

    for i in 1..=40 {
        sqlx::query("INSERT INTO mariadb_stream_test (value) VALUES (?)")
            .bind(format!("insert-{i}"))
            .execute(&admin_pool)
            .await
            .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;
    }
    for i in 1..=15 {
        sqlx::query("UPDATE mariadb_stream_test SET value = ? WHERE id = ?")
            .bind(format!("update-{i}"))
            .bind(i)
            .execute(&admin_pool)
            .await
            .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;
    }
    for i in 31..=40 {
        sqlx::query("DELETE FROM mariadb_stream_test WHERE id = ?")
            .bind(i)
            .execute(&admin_pool)
            .await
            .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;
    }

    let mut events = Vec::new();
    for _ in 0..120 {
        let batch = stream.next_events(400).await?;
        if batch.is_empty() {
            sleep(Duration::from_millis(100)).await;
        } else {
            events.extend(
                batch
                    .into_iter()
                    .filter(|event| !event.op.is_schema_change())
                    .collect::<Vec<_>>(),
            );
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
            rustcdc::core::Operation::Insert => inserts += 1,
            rustcdc::core::Operation::Update => updates += 1,
            rustcdc::core::Operation::Delete => deletes += 1,
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

    // Each row carries the catalogue's shape, which agrees with its table map: the server
    // runs with `binlog_row_metadata=FULL`, so the table map names its columns.
    let declared: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT COLUMN_NAME, CAST(COLUMN_TYPE AS CHAR), IS_NULLABLE \
         FROM INFORMATION_SCHEMA.COLUMNS \
         WHERE TABLE_SCHEMA = 'cdc' AND TABLE_NAME = 'mariadb_stream_test' \
         ORDER BY ORDINAL_POSITION",
    )
    .fetch_all(&admin_pool)
    .await
    .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;
    for event in &events {
        let carried: Option<Vec<(String, String, String)>> =
            event.shape.as_ref().and_then(|shape| {
                Some(
                    shape
                        .columns()?
                        .iter()
                        .map(|column| {
                            (
                                column.name.clone(),
                                column.data_type.clone(),
                                if column.nullable { "YES" } else { "NO" }.to_string(),
                            )
                        })
                        .collect(),
                )
            });
        assert_eq!(
            carried.as_ref(),
            Some(&declared),
            "a MariaDB stream row carries the information_schema shape"
        );
    }

    connection.close().await;
    Ok(())
}

async fn run_mariadb_snapshot_stream_handoff_full_cycle(
    version: &str,
    server_id: u32,
) -> rustcdc::Result<()> {
    install_rustls_provider();
    let container = GenericImage::new("mariadb", version)
        .with_exposed_port(3306.tcp())
        .with_wait_for(WaitFor::message_on_stderr("ready for connections"))
        .with_cmd(vec![
            "--log-bin=mysql-bin",
            "--binlog-format=ROW",
            // MariaDB defaults binlog_row_metadata to NO_LOG, MySQL to MINIMAL —
            // neither carries column names or primary-key flags, so events would be
            // emitted with positional placeholder keys and no key. connect() rejects
            // that, so the test server must match a valid production configuration.
            "--binlog-row-metadata=FULL",
            "--binlog-row-image=FULL",
            "--server-id=1",
        ])
        .with_env_var("MYSQL_ROOT_PASSWORD", "rootpass")
        .with_env_var("MYSQL_DATABASE", "cdc")
        .start()
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

    let host = container
        .get_host()
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?
        .to_string();
    let port = container
        .get_host_port_ipv4(3306.tcp())
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

    let admin_dsn = format!("mysql://root:rootpass@{host}:{port}/cdc");
    let admin_pool = connect_admin_pool(&admin_dsn).await?;

    sqlx::query("DROP TABLE IF EXISTS mariadb_handoff_test")
        .execute(&admin_pool)
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

    sqlx::query(
        "CREATE TABLE mariadb_handoff_test (
            id BIGINT PRIMARY KEY AUTO_INCREMENT,
            value VARCHAR(255)
        ) ENGINE=InnoDB",
    )
    .execute(&admin_pool)
    .await
    .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

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
        sqlx::query(sqlx::AssertSqlSafe(query.as_str()))
            .execute(&admin_pool)
            .await
            .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;
    }

    let config = MariaDbSourceConfig::new(MysqlSourceConfig {
        host,
        port,
        user: "root".to_string(),
        password: "rootpass".to_string().into(),
        database: "cdc".to_string(),
        server_id,
        gtid_mode_enabled: false,
        binlog_format_check: true,
        transport: TransportConfig::plaintext(),
        conn_timeout_secs: 30,
        stream_poll_interval_ms: 50,
        max_events_per_poll: 1_000,
        ..Default::default()
    });

    let mut connection = MariaDbConnection::new(config.clone().into_inner());
    connection.connect().await?;
    let mut snapshot = connection.start_snapshot(&["mariadb_handoff_test"]).await?;

    let mut snapshot_events = Vec::new();
    loop {
        let batch = snapshot.next_chunk(5000).await?;
        if batch.is_empty() {
            break;
        }
        let batch = batch
            .into_iter()
            .filter(|event| !event.op.is_schema_change())
            .collect::<Vec<_>>();
        snapshot_events.extend(batch);
        if snapshot_events.len() >= 1000 {
            break;
        }
    }
    let _end = snapshot.finish().await?;
    connection.close().await;

    let mut resumed = MariaDbConnection::new(config.into_inner());
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
        sqlx::query(sqlx::AssertSqlSafe(query.as_str()))
            .execute(&admin_pool)
            .await
            .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;
    }

    let mut stream_events = Vec::new();
    for _ in 0..120 {
        let batch = stream.next_events(400).await?;
        if batch.is_empty() {
            sleep(Duration::from_millis(100)).await;
        } else {
            stream_events.extend(
                batch
                    .into_iter()
                    .filter(|event| !event.op.is_schema_change())
                    .collect::<Vec<_>>(),
            );
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

/// Each runner against every tested MariaDB version, with server ids unique per run.
macro_rules! every_tested_mariadb {
    ($name:ident, $runner:ident, $base_server_id:expr, $label:literal) => {
        #[tokio::test]
        async fn $name() -> rustcdc::Result<()> {
            if skip_mariadb_e2e_case($label) {
                return Ok(());
            }
            for (server_id, tag) in ($base_server_id..).zip(matrix::tested(matrix::Engine::Mariadb))
            {
                $runner(tag, server_id).await.map_err(|error| {
                    rustcdc::Error::SourceError(format!("mariadb {tag}: {error}"))
                })?;
            }
            Ok(())
        }
    };
}

every_tested_mariadb!(
    mariadb_snapshot_resume_from_checkpoint,
    run_mariadb_snapshot_resume_from_checkpoint,
    510,
    "mariadb snapshot resume test"
);
every_tested_mariadb!(
    mariadb_stream_capture_insert_update_delete,
    run_mariadb_stream_capture_insert_update_delete,
    530,
    "mariadb stream CDC test"
);
every_tested_mariadb!(
    mariadb_snapshot_stream_handoff_full_cycle,
    run_mariadb_snapshot_stream_handoff_full_cycle,
    550,
    "mariadb handoff test"
);

/// An incremental snapshot on a MariaDB that has written anything at all.
///
/// MariaDB assigns every transaction a GTID, and from 12.3 `SHOW MASTER STATUS` reports the
/// position as a fifth column, `Gtid_Binlog_Pos` (`0-1-3`), where MySQL reports
/// `Executed_Gtid_Set`. The watermark is read from that column on both.
#[tokio::test]
async fn mariadb_incremental_snapshot_reads_every_row_after_a_write() -> rustcdc::Result<()> {
    install_rustls_provider();
    if std::env::var("CDC_RS_RUN_DOCKER_TESTS").as_deref() != Ok("1") {
        eprintln!("skipping mariadb incremental snapshot test (set CDC_RS_RUN_DOCKER_TESTS=1)");
        return Ok(());
    }
    let container = GenericImage::new("mariadb", matrix::full(matrix::Engine::Mariadb))
        .with_exposed_port(3306.tcp())
        .with_wait_for(WaitFor::message_on_stderr("ready for connections"))
        .with_cmd(vec![
            "--log-bin=mysql-bin",
            "--binlog-format=ROW",
            "--binlog-row-metadata=FULL",
            "--binlog-row-image=FULL",
            "--server-id=1",
        ])
        .with_env_var("MYSQL_ROOT_PASSWORD", "rootpass")
        .with_env_var("MYSQL_DATABASE", "cdc")
        .start()
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;
    let host = container
        .get_host()
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?
        .to_string();
    let port = container
        .get_host_port_ipv4(3306.tcp())
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;
    let admin_pool =
        connect_admin_pool(&format!("mysql://root:rootpass@{host}:{port}/cdc")).await?;
    for statement in [
        "CREATE TABLE inc (id BIGINT PRIMARY KEY, note VARCHAR(10)) ENGINE=InnoDB",
        "INSERT INTO inc VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')",
    ] {
        sqlx::query(statement)
            .execute(&admin_pool)
            .await
            .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;
    }

    let config = MariaDbSourceConfig::new(MysqlSourceConfig {
        host,
        port,
        user: "root".to_string(),
        password: "rootpass".to_string().into(),
        database: "cdc".to_string(),
        server_id: 310,
        gtid_mode_enabled: false,
        binlog_format_check: true,
        transport: TransportConfig::plaintext(),
        conn_timeout_secs: 30,
        stream_poll_interval_ms: 50,
        max_events_per_poll: 1_000,
        ..Default::default()
    });
    let mut connection = MariaDbConnection::new(config.into_inner());
    connection.connect().await?;
    let mut handle = connection
        .start_incremental_snapshot(
            rustcdc::IncrementalSnapshotConfig::new(vec!["cdc.inc".to_string()]).with_chunk_size(2),
            None,
        )
        .await?;

    let mut ids = HashSet::new();
    for _ in 0..100 {
        for event in handle.next_events(250).await? {
            if event.op == rustcdc::Operation::Read
                && let Some(id) = event.after.as_ref().and_then(|row| row.get("id"))
            {
                ids.insert(id.to_string().trim_matches('"').to_string());
            }
        }
        if ids.len() == 5 {
            break;
        }
    }
    connection.close().await;
    assert_eq!(ids.len(), 5, "every row must be read: {ids:?}");
    Ok(())
}
