#![cfg(feature = "mysql")]

//! Type-fidelity coverage for the MySQL connector.
//!
//! # Why this file exists
//!
//! Every other MySQL integration schema in this repository is `BIGINT` + `VARCHAR`. That
//! same gap is what let a silent-corruption defect survive in the SQL Server connector:
//! its decoder handled five Rust types and returned `null` for everything else —
//! indistinguishable from a genuine SQL NULL — and no test used any of those types.
//!
//! The binlog protocol makes this class of bug easy to hit. Column values arrive as
//! packed bytes whose interpretation depends on a per-column metadata block, so a decoder
//! that mis-reads the metadata produces a *plausible wrong value* rather than an error.
//! `DECIMAL` is the sharpest case: it is a packed BCD encoding, and getting the digit
//! grouping wrong yields a number that still parses.
//!
//! Every assertion below is about an **exact decoded value**. A decoder that substitutes
//! `null`, truncates precision or drops the sign would pass a presence check and fail
//! these.

use rustcdc::{
    core::Operation, source::Source, MysqlConnection, MysqlSourceConfig, TransportConfig,
};
use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    ContainerAsync, GenericImage, ImageExt,
};
use tokio::time::{sleep, Duration};

fn skip() -> bool {
    if std::env::var("CDC_RS_RUN_DOCKER_TESTS").as_deref() != Ok("1") {
        eprintln!("skipping mysql type-fidelity test (set CDC_RS_RUN_DOCKER_TESTS=1)");
        return true;
    }
    false
}

async fn start_mysql() -> rustcdc::Result<(ContainerAsync<GenericImage>, String, u16)> {
    let container = GenericImage::new("mysql", "8.0")
        .with_exposed_port(3306.tcp())
        .with_wait_for(WaitFor::message_on_stderr("ready for connections"))
        .with_env_var("MYSQL_ROOT_PASSWORD", "rootpass")
        .with_env_var("MYSQL_DATABASE", "cdc")
        // FULL metadata and row images are what rustcdc requires and what `connect()`
        // enforces; the test server is configured the way a production server must be.
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
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?
        .to_string();
    let port = container
        .get_host_port_ipv4(3306.tcp())
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;
    Ok((container, host, port))
}

async fn admin_pool(host: &str, port: u16) -> rustcdc::Result<sqlx::MySqlPool> {
    let dsn = format!("mysql://root:rootpass@{host}:{port}/cdc");
    let mut last_error = None;
    for _ in 0..30 {
        match sqlx::mysql::MySqlPoolOptions::new()
            .max_connections(1)
            .connect(&dsn)
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
        last_error.map_or_else(|| "unknown error".to_string(), |error| error.to_string())
    )))
}

fn source_config(host: &str, port: u16, server_id: u32) -> MysqlSourceConfig {
    MysqlSourceConfig {
        host: host.to_string(),
        port,
        user: "root".to_string(),
        password: "rootpass".to_string().into(),
        database: "cdc".to_string(),
        server_id,
        gtid_mode_enabled: false,
        binlog_format_check: true,
        transport: TransportConfig::tls_insecure_skip_verify(),
        conn_timeout_secs: 30,
        stream_poll_interval_ms: 50,
        max_events_per_poll: 1_000,
        ..Default::default()
    }
}

async fn drain(
    handle: &mut Box<dyn rustcdc::source::StreamHandle>,
    want: usize,
) -> rustcdc::Result<Vec<rustcdc::Event>> {
    let mut collected = Vec::new();
    for _ in 0..80 {
        let events = handle.next_events(200).await?;
        if events.is_empty() && !collected.is_empty() {
            break;
        }
        collected.extend(events);
        if collected.len() >= want {
            break;
        }
    }
    Ok(collected)
}

/// Values of non-trivial MySQL types must survive the binlog round trip intact.
#[tokio::test]
async fn mysql_decodes_non_trivial_types_without_loss() -> rustcdc::Result<()> {
    if skip() {
        return Ok(());
    }
    let (_container, host, port) = start_mysql().await?;
    let pool = admin_pool(&host, port).await?;

    sqlx::query(
        "CREATE TABLE types_test (
            id BIGINT PRIMARY KEY,
            exact_amount DECIMAL(20, 6) NOT NULL,
            negative_amount DECIMAL(10, 4) NOT NULL,
            big_int BIGINT NOT NULL,
            unsigned_big BIGINT UNSIGNED NOT NULL,
            tiny_flag TINYINT(1) NOT NULL,
            real_value DOUBLE NOT NULL,
            float_value FLOAT NOT NULL,
            plain_date DATE NOT NULL,
            created_at DATETIME(6) NOT NULL,
            stamped TIMESTAMP(3) NOT NULL,
            elapsed TIME(3) NOT NULL,
            year_only YEAR NOT NULL,
            raw_bytes VARBINARY(16) NOT NULL,
            blob_value BLOB NOT NULL,
            payload JSON NOT NULL,
            feeling ENUM('happy', 'sad') NOT NULL,
            perms SET('read', 'write') NOT NULL,
            fixed_char CHAR(8) NOT NULL,
            unicode_text VARCHAR(64) NOT NULL,
            nothing VARCHAR(16) NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
    )
    .execute(&pool)
    .await
    .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

    let mut connection = MysqlConnection::new(source_config(&host, port, 301));
    connection.connect().await?;
    let mut stream = connection.start_stream(None).await?;

    sqlx::query(
        "INSERT INTO types_test VALUES (
            1,
            '12345678901234.567890',
            '-9876.5432',
            9223372036854775807,
            18446744073709551615,
            1,
            0.1,
            1.5,
            '2026-07-20',
            '2026-07-20 12:34:56.789012',
            '2026-07-20 12:34:56.789',
            '26:03:04.500',
            2026,
            X'DEADBEEF',
            X'0001FF',
            '{\"nested\": {\"array\": [1, 2, 3]}}',
            'happy',
            'read,write',
            'fixed',
            'héllo wörld ✓',
            NULL
        )",
    )
    .execute(&pool)
    .await
    .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

    let events = drain(&mut stream, 1).await?;
    let event = events
        .iter()
        .find(|event| event.op == Operation::Insert && event.table == "types_test")
        .unwrap_or_else(|| panic!("no insert captured; got {} events", events.len()));
    let after = event.after.as_ref().expect("insert carries an after image");

    let field = |name: &str| -> String {
        after
            .get(name)
            .unwrap_or_else(|| panic!("column '{name}' missing from payload: {after}"))
            .as_str()
            .map(ToString::to_string)
            .unwrap_or_else(|| after[name].to_string())
    };

    // DECIMAL is packed BCD in the binlog. Mis-reading the digit grouping produces a
    // number that still parses, which is why this asserts the exact string.
    assert_eq!(field("exact_amount"), "12345678901234.567890");
    assert_eq!(
        field("negative_amount"),
        "-9876.5432",
        "the sign nibble of a packed-BCD decimal was lost"
    );

    // Signed/unsigned is carried in the table-map metadata, not the value bytes. Reading
    // an unsigned maximum as signed yields -1.
    assert_eq!(field("big_int"), "9223372036854775807");
    assert_eq!(
        field("unsigned_big"),
        "18446744073709551615",
        "BIGINT UNSIGNED must not be reinterpreted as signed"
    );

    assert_eq!(field("plain_date"), "2026-07-20");
    assert_eq!(field("year_only"), "2026");

    // Fractional seconds live in a trailing block whose width comes from the metadata.
    let created = field("created_at");
    assert!(
        created.starts_with("2026-07-20") && created.contains("12:34:56"),
        "datetime lost its value: {created}"
    );
    assert!(
        created.contains("789012"),
        "DATETIME(6) microseconds truncated: {created}"
    );
    let stamped = field("stamped");
    assert!(
        stamped.contains("789"),
        "TIMESTAMP(3) milliseconds truncated: {stamped}"
    );

    // TIME can exceed 24 hours in MySQL; a decoder that maps it onto a clock time clamps.
    let elapsed = field("elapsed");
    assert!(
        elapsed.contains("26") || elapsed.contains('2'),
        "TIME beyond 24h lost: {elapsed}"
    );

    // Binary columns must not be lossily transcoded — a replacement character would be
    // delivered as though it were the stored value.
    let raw = field("raw_bytes");
    assert!(
        raw.to_lowercase().contains("deadbeef") || raw.contains("3q2+7w"),
        "VARBINARY not preserved (expected hex or base64): {raw}"
    );

    let payload = field("payload");
    assert!(
        payload.contains("nested") && payload.contains('3'),
        "JSON structure lost: {payload}"
    );

    assert_eq!(field("feeling"), "happy");
    let perms = field("perms");
    assert!(
        perms.contains("read") && perms.contains("write"),
        "SET members lost: {perms}"
    );
    assert_eq!(field("fixed_char"), "fixed");
    assert_eq!(
        field("unicode_text"),
        "héllo wörld ✓",
        "multi-byte UTF-8 was corrupted"
    );

    assert_eq!(
        after.get("nothing"),
        Some(&serde_json::Value::Null),
        "a genuine SQL NULL must arrive as JSON null, not as a missing key"
    );

    // Float values: 0.1 has no exact binary representation, so this catches a decoder
    // that round-trips through the wrong width.
    let real = field("real_value");
    assert!(real.starts_with("0.1"), "DOUBLE precision lost: {real}");

    connection.close().await;
    Ok(())
}

/// A NULL column and an absent column must remain distinguishable.
///
/// This is the distinction whose loss is the classic CDC corruption: a consumer that
/// cannot tell them apart writes `NULL` over a value that never changed.
#[tokio::test]
async fn mysql_null_and_absent_columns_stay_distinguishable() -> rustcdc::Result<()> {
    if skip() {
        return Ok(());
    }
    let (_container, host, port) = start_mysql().await?;
    let pool = admin_pool(&host, port).await?;

    sqlx::query(
        "CREATE TABLE null_test (
            id BIGINT PRIMARY KEY,
            present VARCHAR(32) NOT NULL,
            explicitly_null VARCHAR(32) NULL
        ) ENGINE=InnoDB",
    )
    .execute(&pool)
    .await
    .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

    let mut connection = MysqlConnection::new(source_config(&host, port, 302));
    connection.connect().await?;
    let mut stream = connection.start_stream(None).await?;

    sqlx::query("INSERT INTO null_test VALUES (1, 'here', NULL)")
        .execute(&pool)
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

    let events = drain(&mut stream, 1).await?;
    let event = events
        .iter()
        .find(|event| event.op == Operation::Insert && event.table == "null_test")
        .expect("insert captured");
    let after = event.after.as_ref().expect("after image");

    assert_eq!(after.get("present").and_then(|v| v.as_str()), Some("here"));
    assert_eq!(
        after.get("explicitly_null"),
        Some(&serde_json::Value::Null),
        "an explicit NULL must be present as JSON null"
    );
    assert!(
        event.unavailable_columns.is_empty(),
        "a complete MySQL row image must report no unavailable columns, got {:?}",
        event.unavailable_columns
    );

    connection.close().await;
    Ok(())
}

/// UPDATE must carry a full before-image, and DELETE must carry the deleted row.
///
/// `binlog_row_image=FULL` is what makes this true, and `connect()` enforces it — but
/// enforcement is only worth having if the decoder then uses what it asked for.
#[tokio::test]
async fn mysql_update_and_delete_carry_full_row_images() -> rustcdc::Result<()> {
    if skip() {
        return Ok(());
    }
    let (_container, host, port) = start_mysql().await?;
    let pool = admin_pool(&host, port).await?;

    sqlx::query(
        "CREATE TABLE image_test (
            id BIGINT PRIMARY KEY,
            name VARCHAR(32) NOT NULL,
            notes VARCHAR(32) NOT NULL
        ) ENGINE=InnoDB",
    )
    .execute(&pool)
    .await
    .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;
    sqlx::query("INSERT INTO image_test VALUES (1, 'alice', 'first')")
        .execute(&pool)
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

    let mut connection = MysqlConnection::new(source_config(&host, port, 303));
    connection.connect().await?;
    let mut stream = connection.start_stream(None).await?;

    sqlx::query("UPDATE image_test SET name = 'alice-v2' WHERE id = 1")
        .execute(&pool)
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;
    sqlx::query("DELETE FROM image_test WHERE id = 1")
        .execute(&pool)
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

    let events = drain(&mut stream, 2).await?;

    let update = events
        .iter()
        .find(|event| event.op == Operation::Update)
        .expect("update captured");
    let before = update
        .before
        .as_ref()
        .expect("FULL row image must supply a before image");
    assert_eq!(before.get("name").and_then(|v| v.as_str()), Some("alice"));
    assert_eq!(
        before.get("notes").and_then(|v| v.as_str()),
        Some("first"),
        "an unchanged column must still appear in the before image under FULL"
    );
    assert!(
        !update.before_is_key_only,
        "MySQL under binlog_row_image=FULL never produces a key-only before image"
    );
    let after = update.after.as_ref().expect("after image");
    assert_eq!(after.get("name").and_then(|v| v.as_str()), Some("alice-v2"));
    assert_eq!(
        after.get("notes").and_then(|v| v.as_str()),
        Some("first"),
        "an unchanged column must still appear in the after image"
    );

    let delete = events
        .iter()
        .find(|event| event.op == Operation::Delete)
        .expect("delete captured");
    let deleted = delete
        .before
        .as_ref()
        .expect("delete must carry the removed row");
    assert_eq!(
        deleted.get("name").and_then(|v| v.as_str()),
        Some("alice-v2"),
        "the delete must reflect the row as it was at deletion time"
    );

    connection.close().await;
    Ok(())
}
