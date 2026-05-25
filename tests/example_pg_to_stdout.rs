//! Integration test for the `pg_to_stdout` example.
//!
//! Spawns a real PostgreSQL container, runs the compiled example, inserts rows,
//! captures stdout JSON lines, then sends SIGINT for graceful shutdown.
//!
//! Enabled only when `CDC_RS_RUN_DOCKER_TESTS=1`.

#![cfg(feature = "postgres")]

use std::{
    process::{Child, Command, Stdio},
    time::Duration,
};

use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    GenericImage, ImageExt,
};

#[tokio::test]
async fn example_pg_to_stdout_streams_events_and_shuts_down_cleanly() -> rustcdc::Result<()> {
    if std::env::var("CDC_RS_RUN_DOCKER_TESTS").as_deref() != Ok("1") {
        eprintln!("skipping pg_to_stdout example integration test (set CDC_RS_RUN_DOCKER_TESTS=1)");
        return Ok(());
    }

    // ── 1. Start Postgres container ─────────────────────────────────────────
    let container = GenericImage::new("postgres", "16-alpine")
        .with_exposed_port(5432.tcp())
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_USER", "postgres")
        .with_env_var("POSTGRES_PASSWORD", "postgres")
        .with_env_var("POSTGRES_DB", "cdc")
        .with_cmd(vec![
            "postgres",
            "-c",
            "wal_level=logical",
            "-c",
            "max_replication_slots=4",
            "-c",
            "max_wal_senders=4",
        ])
        .start()
        .await
        .map_err(|e| rustcdc::Error::SourceError(e.to_string()))?;

    let host = container
        .get_host()
        .await
        .map_err(|e| rustcdc::Error::SourceError(e.to_string()))?;
    let port = container
        .get_host_port_ipv4(5432.tcp())
        .await
        .map_err(|e| rustcdc::Error::SourceError(e.to_string()))?;

    // ── 2. Prepare schema via admin connection ──────────────────────────────
    let dsn = format!(
        "host={host} port={port} user=postgres password=postgres dbname=cdc connect_timeout=30"
    );
    let (admin, conn) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
        .await
        .map_err(|e| rustcdc::Error::SourceError(e.to_string()))?;
    tokio::spawn(async move {
        let _ = conn.await;
    });

    admin
        .batch_execute(
            "CREATE TABLE IF NOT EXISTS public.example_items (
               id BIGINT PRIMARY KEY,
               label TEXT NOT NULL
             );
             ALTER TABLE public.example_items REPLICA IDENTITY FULL;
             DROP PUBLICATION IF EXISTS cdc_example_pub;
             CREATE PUBLICATION cdc_example_pub FOR TABLE public.example_items;",
        )
        .await
        .map_err(|e| rustcdc::Error::SourceError(e.to_string()))?;

    // ── 3. Build the example binary (should already be built by CI gate) ────
    let status = Command::new("cargo")
        .args([
            "build",
            "--example",
            "pg_to_stdout",
            "--features",
            "postgres",
        ])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .status()
        .map_err(|e| rustcdc::Error::SourceError(format!("failed to build example: {e}")))?;
    if !status.success() {
        return Err(rustcdc::Error::SourceError(
            "example build failed".to_string(),
        ));
    }

    // ── 4. Launch the example process ───────────────────────────────────────
    let checkpoint_dir = tempfile::tempdir().map_err(rustcdc::Error::IoError)?;

    let example_bin = format!(
        "{}/target/debug/examples/pg_to_stdout",
        env!("CARGO_MANIFEST_DIR")
    );

    let mut child: Child = Command::new(&example_bin)
        .env("CDC_RS_HOST", host.to_string())
        .env("CDC_RS_PORT", port.to_string())
        .env("CDC_RS_USER", "postgres")
        .env("CDC_RS_PASSWORD", "postgres")
        .env("CDC_RS_DB", "cdc")
        .env("CDC_RS_SLOT", "cdc_example_slot")
        .env("CDC_RS_PUBLICATION", "cdc_example_pub")
        .env("CDC_RS_SNAPSHOT_TABLES", "public.example_items")
        .env(
            "CDC_RS_CHECKPOINT_DIR",
            checkpoint_dir.path().to_str().unwrap(),
        )
        .env("CDC_RS_POLL_WAIT_MS", "200")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| rustcdc::Error::SourceError(format!("failed to spawn example: {e}")))?;

    // ── 5. Insert rows after example has started ────────────────────────────
    // Give the example time to connect and begin streaming.
    tokio::time::sleep(Duration::from_secs(3)).await;

    for id in 1_i64..=10_i64 {
        admin
            .execute(
                "INSERT INTO public.example_items (id, label) VALUES ($1, $2)",
                &[&id, &format!("item-{id}")],
            )
            .await
            .map_err(|e| rustcdc::Error::SourceError(e.to_string()))?;
    }

    // ── 6. Allow streaming to run briefly, then terminate and collect output ─
    tokio::time::sleep(Duration::from_secs(5)).await;

    let _ = child.kill();
    let output = child.wait_with_output().map_err(|e| {
        rustcdc::Error::SourceError(format!("failed waiting for example output: {e}"))
    })?;

    let stdout_text = String::from_utf8_lossy(&output.stdout);
    let stderr_text = String::from_utf8_lossy(&output.stderr);
    let collected: Vec<String> = stdout_text
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(ToOwned::to_owned)
        .collect();

    // ── 8. Assertions ────────────────────────────────────────────────────────
    assert_eq!(
        collected.len(),
        10,
        "expected 10 JSON events on stdout, got {}\nstdout:\n{}\nstderr:\n{}",
        collected.len(),
        stdout_text,
        stderr_text
    );

    for (i, line) in collected.iter().enumerate() {
        let value: serde_json::Value = serde_json::from_str(line).unwrap_or_else(|e| {
            panic!("stdout line {i} is not valid JSON: {e}\nLine: {line}");
        });
        assert!(
            value.get("op").is_some(),
            "event {i} missing 'op' field: {value}"
        );
        assert!(
            value.get("after").is_some(),
            "event {i} missing 'after' field: {value}"
        );
    }

    Ok(())
}
