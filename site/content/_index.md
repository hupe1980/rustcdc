+++
title = "Change data capture in Rust"
description = "Open-source change data capture in Rust. Stream row-level changes from PostgreSQL, MySQL, MariaDB and SQL Server to Kafka, Iceberg, HTTP or files."
template = "index.html"
+++

```toml
api_version       = "v1"
delivery_contract = "at_least_once"

[source]
type = "postgres"

  [source.postgres]
  host     = "db.internal"
  user     = "cdc_user"
  password = { env = "POSTGRES_PASSWORD" }   # a literal here is rejected at load
  database = "app"
  publication_name      = "cdc_pub"
  replication_slot_name = "cdc_slot"
  table_include_list    = ["public.orders", "public.customers"]

[sink]
type    = "kafka"
brokers = "broker1:9092,broker2:9092"
topic   = "cdc.orders"
delivery_mode       = "at_least_once_idempotent"
max_pipelined_sends = 128

  [sink.codec]
  type         = "avro_confluent"
  registry_ref = "prod"

# Mask before the data ever leaves the process.
[[pipeline.transforms]]
name = "redact_pii"

  [[pipeline.transforms.actions]]
  type = "mask"

    [pipeline.transforms.actions.rules]
    email = { type = "hmac_sha256", key = { env = "PII_HMAC_KEY" } }

[state]
backend = "kafka_topic"

[admin]
enabled = true
bind    = "127.0.0.1:8080"
```
