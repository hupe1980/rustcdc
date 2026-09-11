# rustcdc demo

Stream PostgreSQL changes to your terminal in under two minutes — no Rust toolchain needed.

## Prerequisites

- [Docker Engine ≥ 23](https://docs.docker.com/engine/install/) with Compose v2
  ```bash
  docker compose version   # must print v2.x
  ```

## Quick start

```bash
cd demo
docker compose up
```

rustcdc performs an **initial snapshot** first — one `"read"` event per existing
row — then switches to **streaming**. The seeder service generates new events every
2 s, so output is continuous.

### What the events look like

Each line is a full rustcdc event envelope (abridged here — real events also
carry `source`, `ts`, `transaction`, and `envelope_version`):

```jsonc
// Initial snapshot — one "read" event per existing row.
// `customers` passes through the masking transform in cdc.toml: `email` is
// redacted outright and `name` becomes a keyed HMAC, so rows stay joinable on
// the pseudonym without being re-identifiable.
{"op":"read","schema":"public","table":"customers","before":null,"after":{"id":"1","name":"9f2c…","email":"***@redacted","tier":"pro","created_at":"..."},"snapshot":{...},"before_is_key_only":false}

// Ongoing changes from the seeder
{"op":"insert","schema":"public","table":"orders","before":null,"after":{"id":"4","customer_id":"3","sku":"WGT-001","quantity":"2","total_cents":"1998","status":"pending","created_at":"..."},"before_is_key_only":false}
{"op":"update","schema":"public","table":"orders","before":{"id":"4","status":"pending",...},"after":{"id":"4","status":"shipped",...},"before_is_key_only":false}
{"op":"delete","schema":"public","table":"orders","before":{"id":"1",...},"after":null,"before_is_key_only":false}
```

Updates carry a **full `before` image** because `init.sql` sets
`REPLICA IDENTITY FULL` on the demo tables. Events with partial payloads
(PostgreSQL unchanged-TOAST) would additionally list the missing columns in
`unavailable_columns` / `before_unavailable_columns` — see
[Core concepts](https://hupe1980.github.io/rustcdc-server/docs/concepts/#partial-row-images-unchanged-toast).

## Inspect the admin API

Open a second terminal while the demo is running. The admin API binds a
non-loopback interface (so Docker can port-map it), which the server only
allows with token auth **and** TLS — the demo ships a read token in
`compose.yml` and a self-signed cert in the image (hence `-k`):

```bash
TOKEN="Authorization: Bearer rustcdc-demo-read-token"

# Liveness / readiness probes
curl -sk -H "$TOKEN" https://localhost:8080/livez
curl -sk -H "$TOKEN" https://localhost:8080/readyz

# Pipeline status and counters
curl -sk -H "$TOKEN" https://localhost:8080/status | jq

# Prometheus metrics — runtime health verdict, throughput, slot lag
curl -sk -H "$TOKEN" https://localhost:8080/metrics \
  | grep -E "rustcdc_runtime_health|rustcdc_runtime_events"
```

## Connect with psql directly

```bash
psql -h localhost -U postgres -d demo
# password: postgres

SELECT COUNT(*) FROM orders WHERE status = 'pending';
INSERT INTO customers (name, email) VALUES ('Demo User', 'demo@example.com');
```

## Stop and clean up

```bash
# Pause — state is preserved on the cdc-state volume; `up` resumes streaming
docker compose stop

# Full reset — removes all volumes including the replication slot
docker compose down -v
```

## What's in the demo

| File | Purpose |
|---|---|
| `compose.yml` | Service definitions (postgres, rustcdc, seeder) |
| `cdc.toml` | rustcdc configuration (PostgreSQL → stdout) |
| `postgres/init.sql` | Schema, replica identity, CDC user, publication, seed data |
| `seed.sh` | Continuous change generator (INSERTs, UPDATEs, DELETEs every 2 s) |

## Next steps

| Goal | How |
|---|---|
| Stream to Kafka | Replace `[sink]` in `cdc.toml` with `type = "kafka"` and add a [Redpanda](https://hub.docker.com/r/redpandadata/redpanda) service to `compose.yml` |
| Encode as Avro / Protobuf against a registry | Add `[sink.codec]` with `type = "avro_confluent"` and a `[registries.<name>]` block — see [the configuration reference](https://hupe1980.github.io/rustcdc-server/docs/configuration/#codecs-sink-codec) |
| Tune or extend the masking rules | The `redact_customer_pii` rule in `cdc.toml`; all rule types are in [the configuration reference](https://hupe1980.github.io/rustcdc-server/docs/configuration/#mask-redact-hash-or-encrypt-fields) |
| Add a WASM transform | See [the WASM transforms guide](https://hupe1980.github.io/rustcdc-server/docs/transforms/) |
| Use MySQL instead of PostgreSQL | See [the MySQL connector guide](https://hupe1980.github.io/rustcdc-server/docs/connectors/mysql/) |
| Deploy to Kubernetes | See [the operations guide](https://hupe1980.github.io/rustcdc-server/docs/operations/#11-kubernetes-deployment) |
| All configuration options | See [the configuration reference](https://hupe1980.github.io/rustcdc-server/docs/configuration/) |
