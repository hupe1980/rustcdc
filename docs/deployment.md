# Deployment Guide

This guide documents local container deployment, runtime configuration, and production deployment baselines for cdc-rs.

## Deployment Models

cdc-rs can be deployed as:

- embedded runtime in an application binary
- example container runtime for local validation
- production service managed by your platform scheduler

## Reference Container Deployment

The repository includes a Docker-based reference path for the PostgreSQL example runtime.

### Build Image

```bash
docker build -t cdc-rs:postgres-example .
```

### Start Stack

```bash
docker compose up --build
```

The compose topology starts:

- PostgreSQL with logical replication enabled
- cdc-rs example runtime configured with `CDC_RS_*` variables

## Runtime Configuration Variables

Common environment variables for the PostgreSQL example:

- `CDC_RS_HOST`
- `CDC_RS_PORT`
- `CDC_RS_USER`
- `CDC_RS_PASSWORD`
- `CDC_RS_DB`
- `CDC_RS_SLOT`
- `CDC_RS_PUBLICATION`
- `CDC_RS_SNAPSHOT_TABLES`
- `CDC_RS_CHECKPOINT_DIR`
- `CDC_RS_MAX_BUFFER_SIZE`
- `CDC_RS_POLL_WAIT_MS`
- `CDC_RS_CONN_TIMEOUT_SECS`

## Persistent Storage Requirements

At minimum, production deployments must persist:

- checkpoint storage
- runtime logs
- optional schema history storage (backend-dependent)

If checkpoint storage is ephemeral, restart correctness is not guaranteed.

## Production Baseline

Recommended baseline controls:

1. run with explicit resource limits (CPU/memory)
2. persist checkpoints on durable storage
3. configure source credentials via managed secret systems
4. expose runtime health and metrics endpoints through your control plane
5. configure restart policy with backoff
6. validate replay behavior before production cutover

## Rollout Checklist

1. validate source connector permissions in target environment
2. verify snapshot and streaming behavior in staging
3. verify checkpoint advancement under steady load
4. execute restart and failover tests
5. confirm downstream sink idempotency policy

## Non-PostgreSQL Sources

For MySQL and SQL Server deployments, use the same runtime pattern with source-specific config and operational prerequisites from [config_reference.md](config_reference.md) and [runbook.md](runbook.md).

## Related Documentation

- [getting_started.md](getting_started.md)
- [config_reference.md](config_reference.md)
- [runbook.md](runbook.md)
- [troubleshooting.md](troubleshooting.md)

---

## HTTP Health and Admin Endpoint

cdc-rs is an embeddable library and does not start an HTTP server. `CdcRuntime` exposes
`admin_snapshot_json()` which returns a `RuntimeAdminSnapshot` payload as JSON.
Wire it to any HTTP server of your choice.

**Minimal axum example** (add `axum = "0.7"` and `tokio` to your Cargo.toml):

```rust
use std::sync::Arc;
use axum::{extract::State, response::IntoResponse, routing::get, Router};
use cdc_rs::{CdcRuntime, RuntimeConfig, RuntimeSourceConfig, InMemoryCheckpoint, InMemorySchemaHistory};

type SharedRuntime = Arc<tokio::sync::Mutex<CdcRuntime<InMemoryCheckpoint, InMemorySchemaHistory>>>;

async fn health(State(rt): State<SharedRuntime>) -> impl IntoResponse {
    let json = match rt.lock().await.admin_snapshot_json() {
        Ok(payload) => payload,
        Err(error) => {
            return (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                [("content-type", "application/json")],
                format!(r#"{{"error":"{error}"}}"),
            );
        }
    };
    (axum::http::StatusCode::OK, [("content-type", "application/json")], json)
}

#[tokio::main]
async fn main() {
    let checkpoint = InMemoryCheckpoint::default();
    let schema_history = InMemorySchemaHistory::default();
    let config = RuntimeConfig::new(RuntimeSourceConfig::Disabled, checkpoint, schema_history);
    let runtime = CdcRuntime::new(config).expect("runtime config should be valid");
    let state: SharedRuntime = Arc::new(tokio::sync::Mutex::new(runtime));

    let app = Router::new()
        .route("/health", get(health))
        .with_state(state.clone());

    // Spawn the HTTP server alongside the CDC runtime.
    tokio::spawn(async move {
        let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await.unwrap();
        axum::serve(listener, app).await.unwrap();
    });

    // Run the CDC runtime loop in the foreground.
    {
        state.lock().await.start().await.unwrap();
        loop {
            let token = {
                let mut runtime = state.lock().await;
                let batch = runtime.poll_event_batch().await.unwrap();
                batch.ack_token()
            };
            if let Some(token) = token {
                state.lock().await.commit_ack(token).await.unwrap();
            }
        }
    }
}
```

The `/health` endpoint returns a JSON object such as:

```json
{
    "source_type": "postgres",
    "state": "running",
    "readiness": true,
    "liveness": true,
    "capabilities": {
        "snapshot": true,
        "snapshot_checkpoint_resume": true,
        "handoff": true,
        "ddl_capture": true,
        "heartbeat": true,
        "tls": true,
        "schema_introspection": true
    },
    "buffer_depth": 0,
    "in_flight_events": 0,
    "snapshot_active": false,
    "stream_active": true,
    "handoff_complete": true,
    "total_events_polled": 42817,
    "total_events_committed": 42817,
    "total_events_deduplicated": 0,
    "started_at_ms": 1716400000000,
    "last_poll_at_ms": 1716400000421,
    "last_commit_at_ms": 1716400000440,
    "checkpoint_age_ms": 12,
    "replication_lag_ms": 4
}
```

Liveness probes should check for a `200` response and `"liveness": true`.
Readiness probes should additionally assert `"readiness": true` and may enforce
`"state": "running"` for stricter rollout policies.
