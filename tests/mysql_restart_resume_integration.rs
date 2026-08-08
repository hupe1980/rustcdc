//! A clean restart with no new writes must deliver nothing — MySQL and MariaDB.
//!
//! The PostgreSQL sibling of this suite (`postgres_restart_resume_integration.rs`) exists
//! because logical decoding there filters at *transaction* granularity, so checkpointing a
//! change's own LSN replayed the whole transaction on every restart.
//!
//! MySQL is claimed not to need the same treatment: the binlog event header's `log_pos` is
//! the position of the **next** event, so a checkpointed coordinate is already a boundary
//! and `StreamHandle::resume_offset_for` can keep its default. That claim was asserted in
//! the docs before it was measured. This measures it.
#![cfg(feature = "mysql")]

use rustcdc::source::Source;
use rustcdc::{checkpoint::MysqlOffset, MysqlConnection, MysqlSourceConfig, TransportConfig};
use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    GenericImage, ImageExt,
};
use tokio::time::{sleep, Duration};

async fn connect_admin_pool(dsn: &str) -> rustcdc::Result<sqlx::MySqlPool> {
    for _ in 0..40 {
        if let Ok(pool) = sqlx::mysql::MySqlPoolOptions::new()
            .max_connections(1)
            .connect(dsn)
            .await
        {
            return Ok(pool);
        }
        sleep(Duration::from_millis(500)).await;
    }
    Err(rustcdc::Error::SourceError(
        "failed to connect mysql admin pool".into(),
    ))
}

/// Returns how many events a second session receives after a clean shutdown with no DML.
async fn replayed_after_clean_restart(image: &str, tag: &str) -> rustcdc::Result<usize> {
    let container = GenericImage::new(image, tag)
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

    let pool = connect_admin_pool(&format!("mysql://root:rootpass@{host}:{port}/cdc")).await?;
    sqlx::query("CREATE TABLE t (id BIGINT PRIMARY KEY, data TEXT)")
        .execute(&pool)
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

    let config = MysqlSourceConfig {
        host: host.to_string(),
        port,
        user: "root".into(),
        password: "rootpass".to_string().into(),
        database: "cdc".into(),
        server_id: 4242,
        transport: TransportConfig::plaintext(),
        ..MysqlSourceConfig::default()
    };

    // ── Session 1: capture one insert, then shut down cleanly ────────────────
    let mut connection = MysqlConnection::new(config.clone());
    connection.connect().await?;
    let mut stream = connection.start_stream(None).await?;

    sqlx::query("INSERT INTO t (id, data) VALUES (1, 'one')")
        .execute(&pool)
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

    let mut delivered = Vec::new();
    for _ in 0..60 {
        delivered.extend(stream.next_events(250).await?);
        if !delivered.is_empty() {
            break;
        }
    }
    assert_eq!(delivered.len(), 1, "session 1 must see exactly the insert");

    assert!(
        delivered[0]
            .source
            .offset
            .chars()
            .all(|c| !c.is_control() && c.is_ascii()),
        "the resume coordinate must not carry raw checksum bytes: {:?}",
        delivered[0].source.offset,
    );
    // What the runtime checkpoints for this event.
    let resume = stream
        .resume_offset_for(&delivered[0])
        .unwrap_or_else(|| delivered[0].source.offset.clone());
    let (file, pos) = resume
        .split_once(':')
        .map(|(f, rest)| {
            let pos = rest.split(':').next().unwrap_or(rest);
            (f.to_string(), pos.parse::<u32>().unwrap_or_default())
        })
        .expect("mysql offset is file:pos");
    println!("session 1 resume coordinate: {file}:{pos}");

    // Idle, then clean shutdown.
    sleep(Duration::from_secs(2)).await;
    drop(stream);
    connection.close().await;

    // ── Session 2: resume from that checkpoint, no new DML ───────────────────
    let mut connection = MysqlConnection::new(config);
    connection.connect().await?;
    let offset = MysqlOffset::new("mysql", file, pos, String::new());
    let mut stream = connection.start_stream(Some(&offset)).await?;

    let mut replayed = Vec::new();
    for _ in 0..20 {
        replayed.extend(stream.next_events(250).await?);
    }
    for event in &replayed {
        println!("REPLAYED offset={} op={}", event.source.offset, event.op);
    }
    drop(stream);
    connection.close().await;

    Ok(replayed.len())
}

#[tokio::test(flavor = "multi_thread")]
async fn mysql_restart_delivers_nothing_new() -> rustcdc::Result<()> {
    if std::env::var("CDC_RS_RUN_DOCKER_TESTS").as_deref() != Ok("1") {
        eprintln!("skipping mysql restart resume test (set CDC_RS_RUN_DOCKER_TESTS=1)");
        return Ok(());
    }
    let replayed = replayed_after_clean_restart("mysql", "8.0").await?;
    assert_eq!(
        replayed, 0,
        "a clean restart with no new writes must deliver nothing; the binlog coordinate \
         recorded in the checkpoint must already be a boundary"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn mariadb_restart_delivers_nothing_new() -> rustcdc::Result<()> {
    if std::env::var("CDC_RS_RUN_DOCKER_TESTS").as_deref() != Ok("1") {
        eprintln!("skipping mariadb restart resume test (set CDC_RS_RUN_DOCKER_TESTS=1)");
        return Ok(());
    }
    let replayed = replayed_after_clean_restart("mariadb", "10.6").await?;
    assert_eq!(
        replayed, 0,
        "a clean restart with no new writes must deliver nothing on MariaDB either"
    );
    Ok(())
}
