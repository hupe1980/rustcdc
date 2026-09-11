+++
title = "Change data capture in Rust"
description = "Open-source change data capture in Rust. Stream row-level changes from PostgreSQL, MySQL, MariaDB and SQL Server to Kafka, Iceberg, HTTP or files."
template = "index.html"
+++

```toml
api_version       = "v1"
delivery_contract = "at_least_once"

[source.postgres]
host                  = "db.internal"
port                  = 5432
user                  = "cdc_user"
password              = { env = "POSTGRES_PASSWORD" }   # a literal here is rejected at load
database              = "app"
publication_name      = "cdc_pub"
replication_slot_name = "cdc_slot"
table_include_list    = ["public.orders", "public.customers"]
table_exclude_list    = []
conn_timeout_secs     = 10
max_events_per_poll   = 1000
stream_poll_interval_ms = 100

[source.postgres.transport]
mode = "tls"

[sink]
type    = "kafka"
brokers = "broker1:9092,broker2:9092"
topic   = "cdc.orders"

# Mask before the data ever leaves the process.
[[pipeline.transforms]]
name = "redact_pii"

  [[pipeline.transforms.actions]]
  type = "mask"

    [pipeline.transforms.actions.rules]
    email = { type = "hmac_sha256", key = { env = "PII_HMAC_KEY" } }

[state.backend.kafka_topic]
brokers            = "broker1:9092,broker2:9092"
topic              = "__rustcdc_state"
durability_profile = "production"

[admin]
bind            = "127.0.0.1:8080"
probe_auth_mode = "allow_unauthenticated_loopback"
```
