#![cfg(feature = "sqlserver")]

use std::{
    io,
    process::{Command, Stdio},
    time::Duration,
};

use serde_json::Value;
use testcontainers::{core::IntoContainerPort, runners::AsyncRunner, GenericImage, ImageExt};

#[path = "sqlserver_testkit.rs"]
mod sqlserver_testkit;

async fn sql_exec(
    client: &mut tiberius::Client<tokio_util::compat::Compat<tokio::net::TcpStream>>,
    sql: &str,
) -> rustcdc::Result<()> {
    client
        .execute(sql, &[])
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;
    Ok(())
}

async fn sql_exec_with_retry(
    client: &mut tiberius::Client<tokio_util::compat::Compat<tokio::net::TcpStream>>,
    sql: &str,
) -> rustcdc::Result<()> {
    const MAX_ATTEMPTS: usize = 8;

    for attempt in 1..=MAX_ATTEMPTS {
        match sql_exec(client, sql).await {
            Ok(()) => return Ok(()),
            Err(error) => {
                let message = error.to_string().to_ascii_lowercase();
                let is_deadlock =
                    message.contains("deadlock victim") || message.contains("code: 1205");
                if is_deadlock && attempt < MAX_ATTEMPTS {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    continue;
                }
                return Err(error);
            }
        }
    }

    Err(rustcdc::Error::StateError(
        "sql_exec_with_retry exhausted attempts unexpectedly".to_string(),
    ))
}

#[tokio::test]
async fn sqlserver_to_otel_example_runs_and_emits_logs_and_traces() -> rustcdc::Result<()> {
    if sqlserver_testkit::skip_docker_test("sqlserver_to_otel example integration test") {
        return Ok(());
    }

    let jaeger = GenericImage::new("jaegertracing/all-in-one", "1.57")
        .with_exposed_port(4317.tcp())
        .with_exposed_port(16686.tcp())
        .with_env_var("COLLECTOR_OTLP_ENABLED", "true")
        .start()
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

    let jaeger_host = jaeger
        .get_host()
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?
        .to_string();
    let jaeger_otlp_port = jaeger
        .get_host_port_ipv4(4317.tcp())
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;
    let jaeger_ui_port = jaeger
        .get_host_port_ipv4(16686.tcp())
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

    let sqlserver = sqlserver_testkit::start_sqlserver_container("2019-latest").await?;
    let (sql_host, sql_port) = sqlserver_testkit::host_and_port(&sqlserver).await?;

    let mut admin = sqlserver_testkit::connect_admin_with_retry(
        &sql_host,
        sql_port,
        30,
        Duration::from_secs(1),
    )
    .await?;

    sql_exec(
        &mut admin,
        "IF DB_ID('rustcdc_example') IS NULL CREATE DATABASE rustcdc_example",
    )
    .await?;
    sql_exec(
        &mut admin,
        "USE rustcdc_example; IF OBJECT_ID('dbo.orders', 'U') IS NULL CREATE TABLE dbo.orders (id INT NOT NULL PRIMARY KEY, amount INT NOT NULL)",
    )
    .await?;
    sql_exec(&mut admin, "USE rustcdc_example; DELETE FROM dbo.orders").await?;
    sql_exec_with_retry(
        &mut admin,
        "USE rustcdc_example; IF (SELECT is_cdc_enabled FROM sys.databases WHERE name = DB_NAME()) = 0 EXEC sys.sp_cdc_enable_db",
    )
    .await?;
    sql_exec_with_retry(
        &mut admin,
        "USE rustcdc_example; IF NOT EXISTS (SELECT 1 FROM cdc.change_tables WHERE source_object_id = OBJECT_ID('dbo.orders')) EXEC sys.sp_cdc_enable_table @source_schema='dbo', @source_name='orders', @role_name=NULL, @supports_net_changes=0",
    )
    .await?;

    sql_exec(
        &mut admin,
        "USE rustcdc_example; INSERT INTO dbo.orders (id, amount) VALUES (1,100), (2,200), (3,300), (4,400), (5,500)",
    )
    .await?;

    let status = Command::new("cargo")
        .args([
            "build",
            "--example",
            "sqlserver_to_otel",
            "--features",
            "sqlserver,metrics",
        ])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .status()
        .map_err(|error| {
            rustcdc::Error::SourceError(format!("failed to build example: {error}"))
        })?;
    if !status.success() {
        return Err(rustcdc::Error::SourceError(
            "example build failed".to_string(),
        ));
    }

    let checkpoint_dir = tempfile::tempdir().map_err(rustcdc::Error::IoError)?;
    let service_name = format!("sqlserver-to-otel-example-{}", std::process::id());
    let binary = format!(
        "{}/target/debug/examples/sqlserver_to_otel",
        env!("CARGO_MANIFEST_DIR")
    );

    let child = Command::new(binary)
        .env("CDC_RS_SQLSERVER_HOST", &sql_host)
        .env("CDC_RS_SQLSERVER_PORT", sql_port.to_string())
        .env("CDC_RS_SQLSERVER_USER", "sa")
        .env(
            "CDC_RS_SQLSERVER_PASSWORD",
            sqlserver_testkit::SQLSERVER_SA_PASSWORD,
        )
        .env("CDC_RS_SQLSERVER_DB", "rustcdc_example")
        .env("CDC_RS_SNAPSHOT_TABLES", "dbo.orders")
        .env("CDC_RS_CHECKPOINT_DIR", checkpoint_dir.path())
        .env("CDC_RS_MAX_EVENTS", "5")
        .env("CDC_RS_MAX_RUNTIME_SECS", "12")
        .env("CDC_RS_POLL_WAIT_MS", "200")
        .env(
            "CDC_RS_OTLP_ENDPOINT",
            format!("http://{jaeger_host}:{jaeger_otlp_port}"),
        )
        .env("CDC_RS_SERVICE_NAME", &service_name)
        .env("CDC_RS_SERVICE_VERSION", "test")
        .env("CDC_RS_ENVIRONMENT", "integration")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|error| rustcdc::Error::SourceError(format!("failed to run example: {error}")))?;

    let output = tokio::time::timeout(Duration::from_secs(25), async move {
        tokio::task::spawn_blocking(move || child.wait_with_output()).await
    })
    .await
    .map_err(|_| rustcdc::Error::TimeoutError("example timed out".to_string()))
    .and_then(|join_result| {
        join_result
            .map_err(|error| rustcdc::Error::SourceError(error.to_string()))
            .and_then(|wait_result| {
                wait_result
                    .map_err(|error: io::Error| rustcdc::Error::SourceError(error.to_string()))
            })
    })?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(rustcdc::Error::SourceError(format!(
            "example exited with status {:?}: {}",
            output.status.code(),
            stderr
        )));
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut saw_log_source_connected = false;
    let mut saw_log_stream_started = false;
    let mut saw_log_checkpoint = false;
    let mut saw_read = false;

    for line in stdout.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        let parsed: Value = serde_json::from_str(trimmed).map_err(|error| {
            rustcdc::Error::SerializationError(format!(
                "example stdout line is not valid JSON: {error}"
            ))
        })?;

        if parsed.get("kind").and_then(Value::as_str) == Some("log") {
            assert_eq!(
                parsed.get("source_type").and_then(Value::as_str),
                Some("sqlserver")
            );
            match parsed.get("event").and_then(Value::as_str) {
                Some("source_connected") => saw_log_source_connected = true,
                Some("stream_started") => saw_log_stream_started = true,
                Some("checkpoint_saved") => saw_log_checkpoint = true,
                _ => {}
            }
            continue;
        }

        if let Some(op) = parsed.get("op").and_then(Value::as_str) {
            if op == "read" {
                saw_read = true;
            }
        }
    }

    assert!(
        saw_log_source_connected,
        "expected source_connected structured log"
    );
    assert!(
        saw_log_stream_started,
        "expected stream_started structured log"
    );
    assert!(
        saw_log_checkpoint,
        "expected checkpoint_saved structured log"
    );
    assert!(saw_read, "expected snapshot read events in example output");

    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

    let mut saw_traces = false;
    for _ in 0..12 {
        let response = http
            .get(format!(
                "http://{jaeger_host}:{jaeger_ui_port}/api/traces?service={service_name}&limit=20"
            ))
            .send()
            .await
            .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

        if !response.status().is_success() {
            tokio::time::sleep(Duration::from_millis(250)).await;
            continue;
        }

        let payload: Value = response
            .json()
            .await
            .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

        let has_data = payload
            .get("data")
            .and_then(Value::as_array)
            .is_some_and(|items| !items.is_empty());

        if has_data {
            saw_traces = true;
            break;
        }

        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    assert!(
        saw_traces,
        "expected example traces to be queryable in Jaeger"
    );

    Ok(())
}
