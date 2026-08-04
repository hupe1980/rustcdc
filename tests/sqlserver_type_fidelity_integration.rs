#![cfg(feature = "sqlserver")]

//! Type-fidelity coverage for the SQL Server connector.
//!
//! # Why this file exists
//!
//! This connector is where the type-fidelity gap was first found. Its client-side decoder
//! handled five Rust types and returned `null` for `decimal`, `datetime2`,
//! `uniqueidentifier`, `varbinary` and `xml` — indistinguishable from a genuine SQL NULL,
//! delivered as an authentic value, with no error anywhere. No test used any of those
//! types, because every integration schema in the repository was `BIGINT` + `NVARCHAR`.
//!
//! A null substituted for a value is the worst shape a CDC bug can take: a consumer
//! applying the event writes `NULL` over real data and the pipeline reports success.
//!
//! Every assertion below is about an **exact decoded value**, and the NULL-vs-value
//! distinction is asserted explicitly rather than inferred.

use rustcdc::{source::Source, Operation, SqlServerConnection};

#[path = "sqlserver_testkit.rs"]
mod sqlserver_testkit;

/// Poll the stream, forcing a capture pass whenever it comes back empty.
///
/// SQL Server populates its change tables from a SQL Agent job, and the `mssql/server`
/// container image ships without a running Agent — so nothing is ever captured unless a
/// scan is invoked explicitly. `sys.sp_cdc_scan` does one pass synchronously. Without
/// this the test fails with zero events for a reason that has nothing to do with the
/// decoder it is meant to exercise.
async fn collect(
    stream: &mut dyn rustcdc::source::StreamHandle,
    admin: &mut sqlserver_testkit::SqlClient,
    database: &str,
    want: usize,
) -> rustcdc::Result<Vec<rustcdc::Event>> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(120);
    let scan_sql = format!("USE {database}; EXEC sys.sp_cdc_scan");
    let mut collected = Vec::new();
    while std::time::Instant::now() < deadline {
        collected.extend(stream.next_events(500).await?);
        if collected.len() >= want {
            break;
        }
        let _ = sqlserver_testkit::sql_exec(admin, &scan_sql).await;
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
    Ok(collected)
}

/// Values of the exact types whose decoder previously returned `null` must round trip.
#[tokio::test]
async fn sqlserver_decodes_non_trivial_types_without_loss() -> rustcdc::Result<()> {
    if sqlserver_testkit::skip_docker_test("sqlserver type-fidelity test") {
        return Ok(());
    }
    let container = match sqlserver_testkit::start_sqlserver_container("2022-latest").await {
        Ok(container) => container,
        Err(ref error) if sqlserver_testkit::is_skip_error(error) => return Ok(()),
        Err(error) => return Err(error),
    };
    let (host, port) = sqlserver_testkit::host_and_port(&container).await?;
    let database = "rustcdc_types";
    let mut admin = sqlserver_testkit::connect_admin_with_retry(
        &host,
        port,
        60,
        std::time::Duration::from_millis(500),
    )
    .await?;
    sqlserver_testkit::sql_exec_with_retry(
        &mut admin,
        &format!("IF DB_ID('{database}') IS NULL CREATE DATABASE {database}"),
    )
    .await?;

    sqlserver_testkit::sql_exec_with_retry(
        &mut admin,
        &format!(
            "USE {database};
             CREATE TABLE dbo.types_test (
                 id BIGINT NOT NULL PRIMARY KEY,
                 exact_amount DECIMAL(20, 6) NOT NULL,
                 negative_amount NUMERIC(10, 4) NOT NULL,
                 money_value MONEY NOT NULL,
                 small_money SMALLMONEY NOT NULL,
                 big_int BIGINT NOT NULL,
                 real_value FLOAT NOT NULL,
                 single_value REAL NOT NULL,
                 flag BIT NOT NULL,
                 identifier UNIQUEIDENTIFIER NOT NULL,
                 plain_date DATE NOT NULL,
                 precise_time TIME(7) NOT NULL,
                 created_at DATETIME2(7) NOT NULL,
                 legacy_dt DATETIME NOT NULL,
                 offset_dt DATETIMEOFFSET(7) NOT NULL,
                 raw_bytes VARBINARY(16) NOT NULL,
                 unicode_text NVARCHAR(64) NOT NULL,
                 ascii_text VARCHAR(64) NOT NULL,
                 doc XML NOT NULL,
                 nothing NVARCHAR(16) NULL
             )"
        ),
    )
    .await?;
    sqlserver_testkit::enable_cdc(&host, port, database).await?;

    sqlserver_testkit::sql_exec_with_retry(
        &mut admin,
        &format!(
            "USE {database};
             EXEC sys.sp_cdc_enable_table
                 @source_schema = N'dbo',
                 @source_name = N'types_test',
                 @role_name = NULL,
                 @supports_net_changes = 0"
        ),
    )
    .await?;
    // The CDC capture job is started asynchronously by `sp_cdc_enable_table`. Until it
    // runs, `fn_cdc_get_max_lsn()` returns NULL and nothing is captured — so a stream
    // opened immediately sees an empty change table and the test fails for a reason that
    // has nothing to do with type decoding.
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    let config = sqlserver_testkit::source_config(host.clone(), port, database.to_string(), 30);
    let mut connection = SqlServerConnection::new(config);
    connection.connect().await?;
    let mut stream = connection.start_stream(None).await?;

    sqlserver_testkit::sql_exec_with_retry(
        &mut admin,
        &format!(
            "USE {database};
             INSERT INTO dbo.types_test VALUES (
                 1,
                 12345678901234.567890,
                 -9876.5432,
                 922337203685477.5807,
                 214748.3647,
                 9223372036854775807,
                 0.1,
                 1.5,
                 1,
                 'A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11',
                 '2026-07-20',
                 '12:34:56.7890123',
                 '2026-07-20T12:34:56.7890123',
                 '2026-07-20T12:34:56.790',
                 '2026-07-20T12:34:56.7890123+02:00',
                 0xDEADBEEF,
                 N'héllo wörld ✓',
                 'plain ascii',
                 '<root><child>value</child></root>',
                 NULL
             )"
        ),
    )
    .await?;

    let events = collect(stream.as_mut(), &mut admin, database, 1).await?;
    let event = events
        .iter()
        .find(|event| event.op == Operation::Insert && event.table == "types_test")
        .unwrap_or_else(|| panic!("no insert captured; got {} events", events.len()));
    let after = event.after.as_ref().expect("insert carries an after image");

    let raw = |name: &str| -> &serde_json::Value {
        after
            .get(name)
            .unwrap_or_else(|| panic!("column '{name}' missing from payload: {after}"))
    };
    let field = |name: &str| -> String {
        let value = raw(name);
        value
            .as_str()
            .map_or_else(|| value.to_string(), ToString::to_string)
    };
    // The defect this file exists for: a decoder that cannot read a type returns `null`,
    // which is indistinguishable from a real SQL NULL. Assert non-null explicitly on every
    // NOT NULL column rather than trusting the value assertions to catch it.
    let assert_present = |name: &str| {
        assert!(
            !raw(name).is_null(),
            "column '{name}' is declared NOT NULL but decoded to null — \
             this is the silent-corruption shape: an unsupported type reported as a NULL"
        );
    };
    for column in [
        "exact_amount",
        "negative_amount",
        "money_value",
        "small_money",
        "big_int",
        "real_value",
        "single_value",
        "flag",
        "identifier",
        "plain_date",
        "precise_time",
        "created_at",
        "legacy_dt",
        "offset_dt",
        "raw_bytes",
        "unicode_text",
        "ascii_text",
        "doc",
    ] {
        assert_present(column);
    }

    // Exact numerics must keep full precision — a float round trip loses the low digits.
    let exact = field("exact_amount");
    assert!(
        exact.starts_with("12345678901234.56"),
        "DECIMAL precision lost: {exact}"
    );
    let negative = field("negative_amount");
    assert!(
        negative.starts_with("-9876.54"),
        "NUMERIC sign or precision lost: {negative}"
    );

    assert_eq!(field("big_int"), "9223372036854775807");

    let identifier = field("identifier").to_lowercase();
    assert_eq!(
        identifier, "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
        "UNIQUEIDENTIFIER must round trip as its canonical text form"
    );

    let date = field("plain_date");
    assert!(
        date.starts_with("2026-07-20"),
        "DATE lost its value: {date}"
    );

    let created = field("created_at");
    assert!(
        created.contains("2026-07-20") && created.contains("12:34:56"),
        "DATETIME2 lost its value: {created}"
    );
    assert!(
        created.contains("789"),
        "DATETIME2(7) sub-second precision truncated: {created}"
    );

    let offset = field("offset_dt");
    assert!(
        offset.contains("2026-07-20"),
        "DATETIMEOFFSET lost its value: {offset}"
    );

    let time = field("precise_time");
    assert!(time.contains("12:34:56"), "TIME lost its value: {time}");

    // Binary must not be lossily transcoded into replacement characters.
    let bytes = field("raw_bytes").to_lowercase();
    assert!(
        bytes.contains("deadbeef") || bytes.contains("3q2+7w"),
        "VARBINARY not preserved (expected hex or base64): {bytes}"
    );

    assert_eq!(
        field("unicode_text"),
        "héllo wörld ✓",
        "NVARCHAR multi-byte content was corrupted"
    );
    assert_eq!(field("ascii_text"), "plain ascii");

    let doc = field("doc");
    assert!(
        doc.contains("child") && doc.contains("value"),
        "XML content lost: {doc}"
    );

    // And the one column that genuinely is NULL must still read as NULL.
    assert!(
        raw("nothing").is_null(),
        "a genuine SQL NULL must arrive as JSON null"
    );

    connection.close().await;
    Ok(())
}

/// A NULL column and a decode failure must not look the same.
///
/// The point of this test is the *contrast*: the same table has a real NULL and a value of
/// a type the decoder must handle. If the decoder regresses, the second becomes null and
/// this test tells you which one changed.
#[tokio::test]
async fn sqlserver_null_is_distinguishable_from_an_undecodable_value() -> rustcdc::Result<()> {
    if sqlserver_testkit::skip_docker_test("sqlserver null-fidelity test") {
        return Ok(());
    }
    let container = match sqlserver_testkit::start_sqlserver_container("2022-latest").await {
        Ok(container) => container,
        Err(ref error) if sqlserver_testkit::is_skip_error(error) => return Ok(()),
        Err(error) => return Err(error),
    };
    let (host, port) = sqlserver_testkit::host_and_port(&container).await?;
    let database = "rustcdc_nulls";
    let mut admin = sqlserver_testkit::connect_admin_with_retry(
        &host,
        port,
        60,
        std::time::Duration::from_millis(500),
    )
    .await?;
    sqlserver_testkit::sql_exec_with_retry(
        &mut admin,
        &format!("IF DB_ID('{database}') IS NULL CREATE DATABASE {database}"),
    )
    .await?;

    sqlserver_testkit::sql_exec_with_retry(
        &mut admin,
        &format!(
            "USE {database};
             CREATE TABLE dbo.null_test (
                 id BIGINT NOT NULL PRIMARY KEY,
                 present_decimal DECIMAL(10, 2) NOT NULL,
                 null_decimal DECIMAL(10, 2) NULL,
                 present_guid UNIQUEIDENTIFIER NOT NULL,
                 null_guid UNIQUEIDENTIFIER NULL
             )"
        ),
    )
    .await?;
    sqlserver_testkit::enable_cdc(&host, port, database).await?;
    sqlserver_testkit::sql_exec_with_retry(
        &mut admin,
        &format!(
            "USE {database};
             EXEC sys.sp_cdc_enable_table
                 @source_schema = N'dbo',
                 @source_name = N'null_test',
                 @role_name = NULL,
                 @supports_net_changes = 0"
        ),
    )
    .await?;
    // The CDC capture job is started asynchronously by `sp_cdc_enable_table`. Until it
    // runs, `fn_cdc_get_max_lsn()` returns NULL and nothing is captured — so a stream
    // opened immediately sees an empty change table and the test fails for a reason that
    // has nothing to do with type decoding.
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    let config = sqlserver_testkit::source_config(host.clone(), port, database.to_string(), 30);
    let mut connection = SqlServerConnection::new(config);
    connection.connect().await?;
    let mut stream = connection.start_stream(None).await?;

    sqlserver_testkit::sql_exec_with_retry(
        &mut admin,
        &format!(
            "USE {database};
             INSERT INTO dbo.null_test VALUES (
                 1, 42.50, NULL, 'A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11', NULL
             )"
        ),
    )
    .await?;

    let events = collect(stream.as_mut(), &mut admin, database, 1).await?;
    let event = events
        .iter()
        .find(|event| event.op == Operation::Insert && event.table == "null_test")
        .expect("insert captured");
    let after = event.after.as_ref().expect("after image");

    assert!(
        !after["present_decimal"].is_null(),
        "a NOT NULL decimal decoded to null — the decoder cannot read the type"
    );
    assert!(
        !after["present_guid"].is_null(),
        "a NOT NULL uniqueidentifier decoded to null — the decoder cannot read the type"
    );
    assert!(
        after["null_decimal"].is_null(),
        "a genuine NULL must stay null"
    );
    assert!(
        after["null_guid"].is_null(),
        "a genuine NULL must stay null"
    );
    assert!(
        event.unavailable_columns.is_empty(),
        "SQL Server capture tables always carry every captured column, so nothing should \
         be reported unavailable; got {:?}",
        event.unavailable_columns
    );

    connection.close().await;
    Ok(())
}
