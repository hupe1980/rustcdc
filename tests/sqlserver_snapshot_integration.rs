#![cfg(feature = "sqlserver")]

use rustcdc::checkpoint::{Checkpoint, InMemoryCheckpoint};
use rustcdc::{source::Source, SqlServerConnection};

#[path = "sqlserver_testkit.rs"]
mod sqlserver_testkit;

async fn sql_count(
    client: &mut tiberius::Client<tokio_util::compat::Compat<tokio::net::TcpStream>>,
    sql: &str,
) -> rustcdc::Result<u64> {
    let rows = client
        .query(sql, &[])
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?
        .into_first_result()
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

    let count = rows
        .into_iter()
        .next()
        .and_then(|row| row.get::<i64, _>(0))
        .ok_or_else(|| rustcdc::Error::SourceError("missing count row".into()))?;
    u64::try_from(count).map_err(|_| rustcdc::Error::SourceError("negative row count".into()))
}

async fn sql_rows(
    client: &mut tiberius::Client<tokio_util::compat::Compat<tokio::net::TcpStream>>,
    sql: &str,
) -> rustcdc::Result<Vec<(i32, String)>> {
    let rows = client
        .query(sql, &[])
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?
        .into_first_result()
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let id = row
            .get::<i32, _>(0)
            .ok_or_else(|| rustcdc::Error::SourceError("missing id column".into()))?;
        let name = row
            .get::<&str, _>(1)
            .ok_or_else(|| rustcdc::Error::SourceError("missing name column".into()))?
            .to_string();
        out.push((id, name));
    }
    Ok(out)
}

fn json_i32_field(row: &serde_json::Value, field: &str) -> rustcdc::Result<i32> {
    let value = row
        .get(field)
        .ok_or_else(|| rustcdc::Error::SourceError(format!("snapshot row missing {field}")))?;

    if let Some(number) = value.as_i64() {
        return i32::try_from(number).map_err(|_| {
            rustcdc::Error::SourceError(format!("snapshot row {field} out of i32 range"))
        });
    }

    if let Some(text) = value.as_str() {
        return text.parse::<i32>().map_err(|error| {
            rustcdc::Error::SourceError(format!("invalid {field} in snapshot row: {error}"))
        });
    }

    Err(rustcdc::Error::SourceError(format!(
        "snapshot row {field} has unsupported type"
    )))
}

#[tokio::test]
async fn sqlserver_snapshot_chunking_matches_table_count() -> rustcdc::Result<()> {
    if sqlserver_testkit::skip_docker_test("sqlserver snapshot integration test") {
        return Ok(());
    }

    let container = sqlserver_testkit::start_sqlserver_container("2019-latest").await?;
    let (host, port) = sqlserver_testkit::host_and_port(&container).await?;

    let mut admin = sqlserver_testkit::connect_admin_with_retry(
        &host,
        port,
        40,
        std::time::Duration::from_secs(2),
    )
    .await?;

    let database = "rustcdc_snapshot_chunking";

    sqlserver_testkit::sql_exec_with_retry(
        &mut admin,
        &format!("IF DB_ID('{database}') IS NULL CREATE DATABASE {database}"),
    )
    .await?;
    sqlserver_testkit::enable_cdc(&host, port, database).await?;
    sqlserver_testkit::sql_exec_with_retry(
        &mut admin,
        &format!(
            "USE {database}; IF OBJECT_ID('dbo.users', 'U') IS NULL CREATE TABLE dbo.users (id INT NOT NULL PRIMARY KEY, name NVARCHAR(100) NOT NULL)"
        ),
    )
    .await?;
    sqlserver_testkit::sql_exec_with_retry(
        &mut admin,
        &format!("USE {database}; DELETE FROM dbo.users"),
    )
    .await?;

    // Seed enough rows to force multiple chunks.
    for start in (1..=1000).step_by(100) {
        let end = start + 99;
        let mut values = String::new();
        for id in start..=end {
            if !values.is_empty() {
                values.push_str(", ");
            }
            values.push_str(&format!("({id}, 'user_{id}')"));
        }
        sqlserver_testkit::sql_exec_with_retry(
            &mut admin,
            &format!("USE {database}; INSERT INTO dbo.users (id, name) VALUES {values}"),
        )
        .await?;
    }

    let expected_count = sql_count(
        &mut admin,
        &format!("USE {database}; SELECT COUNT_BIG(1) FROM dbo.users"),
    )
    .await?;

    let mut source = SqlServerConnection::new(sqlserver_testkit::source_config(
        host.clone(),
        port,
        database.into(),
        30,
    ));

    source.connect().await?;
    let mut snapshot = source.start_snapshot(&["dbo.users"]).await?;

    let mut read_count = 0_u64;
    let mut chunk_count = 0_u32;
    loop {
        let chunk = snapshot.next_chunk(128).await?;
        if chunk.is_empty() {
            break;
        }

        chunk_count += 1;
        for event in chunk {
            assert_eq!(event.op, rustcdc::Operation::Read);
            assert_eq!(event.table, "users");
            assert_eq!(event.schema.as_deref(), Some("dbo"));
            assert!(event.snapshot.is_some());
            assert!(event.after.is_some());
            read_count = read_count.saturating_add(1);
        }
    }

    assert!(chunk_count > 1, "snapshot should emit more than one chunk");
    assert_eq!(read_count, expected_count);

    let end = snapshot.finish().await?;
    assert!(end.snapshot_end_ts > 0);

    source.close().await;
    Ok(())
}

#[tokio::test]
async fn sqlserver_snapshot_resume_has_no_duplicates_and_matches_select_content(
) -> rustcdc::Result<()> {
    if sqlserver_testkit::skip_docker_test("sqlserver snapshot resume integration test") {
        return Ok(());
    }

    let container = sqlserver_testkit::start_sqlserver_container("2019-latest").await?;
    let (host, port) = sqlserver_testkit::host_and_port(&container).await?;

    let mut admin = sqlserver_testkit::connect_admin_with_retry(
        &host,
        port,
        40,
        std::time::Duration::from_secs(2),
    )
    .await?;
    let database = "rustcdc_snapshot_resume";

    sqlserver_testkit::sql_exec_with_retry(
        &mut admin,
        &format!("IF DB_ID('{database}') IS NULL CREATE DATABASE {database}"),
    )
    .await?;
    sqlserver_testkit::enable_cdc(&host, port, database).await?;
    sqlserver_testkit::sql_exec_with_retry(
        &mut admin,
        &format!(
            "USE {database}; IF OBJECT_ID('dbo.users', 'U') IS NULL CREATE TABLE dbo.users (id INT NOT NULL PRIMARY KEY, name NVARCHAR(100) NOT NULL)"
        ),
    )
    .await?;
    sqlserver_testkit::sql_exec_with_retry(
        &mut admin,
        &format!("USE {database}; DELETE FROM dbo.users"),
    )
    .await?;

    // Insert 100K rows in deterministic order.
    for chunk_start in (1..=100000).step_by(1000) {
        let chunk_end = (chunk_start + 999).min(100000);
        let mut values = String::new();
        for id in chunk_start..=chunk_end {
            if !values.is_empty() {
                values.push_str(", ");
            }
            values.push_str(&format!("({id}, 'user_{id}')"));
        }
        sqlserver_testkit::sql_exec_with_retry(
            &mut admin,
            &format!("USE {database}; INSERT INTO dbo.users (id, name) VALUES {values}"),
        )
        .await?;
    }

    let expected_rows = sql_rows(
        &mut admin,
        &format!("USE {database}; SELECT id, name FROM dbo.users ORDER BY id"),
    )
    .await?;
    let expected_count = expected_rows.len() as u64;
    assert_eq!(expected_count, 100000);

    let mut source = SqlServerConnection::new(sqlserver_testkit::source_config(
        host.clone(),
        port,
        database.into(),
        30,
    ));

    source.connect().await?;
    let mut snapshot = source.start_snapshot(&["dbo.users"]).await?;

    let mut checkpoint = InMemoryCheckpoint::default();
    let mut captured_rows: Vec<(i32, String)> = Vec::new();
    let mut chunks = 0_u32;

    loop {
        let batch = snapshot.next_chunk(2000).await?;
        if batch.is_empty() {
            break;
        }

        chunks = chunks.saturating_add(1);
        for event in batch {
            let after = event.after.ok_or_else(|| {
                rustcdc::Error::SourceError("snapshot row missing after payload".into())
            })?;
            let id = json_i32_field(&after, "id")?;
            let name = after
                .get("name")
                .and_then(|value| value.as_str())
                .ok_or_else(|| rustcdc::Error::SourceError("snapshot row missing name".into()))?
                .to_string();
            captured_rows.push((id, name));
        }

        if chunks == 5 {
            snapshot
                .checkpoint(&mut checkpoint, captured_rows.len() as u64)
                .await?;
            break;
        }
    }

    let resume_offset = checkpoint.load().await?.ok_or_else(|| {
        rustcdc::Error::CheckpointError("expected snapshot checkpoint after chunk 5".into())
    })?;
    source.close().await;

    let mut resumed_source = SqlServerConnection::new(sqlserver_testkit::source_config(
        host,
        port,
        database.into(),
        30,
    ));
    resumed_source.connect().await?;

    let mut resumed_snapshot = resumed_source
        .start_snapshot_from_checkpoint(&["dbo.users"], Some(resume_offset.as_ref()))
        .await?;

    loop {
        let batch = resumed_snapshot.next_chunk(2000).await?;
        if batch.is_empty() {
            break;
        }

        for event in batch {
            let after = event.after.ok_or_else(|| {
                rustcdc::Error::SourceError("resumed snapshot row missing after payload".into())
            })?;
            let id = json_i32_field(&after, "id")?;
            let name = after
                .get("name")
                .and_then(|value| value.as_str())
                .ok_or_else(|| {
                    rustcdc::Error::SourceError("resumed snapshot row missing name".into())
                })?
                .to_string();
            captured_rows.push((id, name));
        }
    }

    let snapshot_end = resumed_snapshot.finish().await?;
    assert!(snapshot_end.snapshot_end_ts > 0);

    let mut unique_rows = std::collections::BTreeMap::new();
    for (id, name) in &captured_rows {
        unique_rows.insert(*id, name.clone());
    }

    assert_eq!(captured_rows.len() as u64, expected_count);
    assert_eq!(unique_rows.len() as u64, expected_count);

    let expected_map: std::collections::BTreeMap<i32, String> = expected_rows.into_iter().collect();
    assert_eq!(unique_rows, expected_map);

    resumed_source.close().await;
    Ok(())
}
