//! Startup preflight for the topics schema events are published to.
//!
//! Every connector publishes schema events through `CapturedDdl::to_event`, under the
//! synthetic table `<table>__ddl_events`, so a topic template renders them to a topic of
//! their own. These tests go through the real `load()`, `build_router`
//! and router preflight against a fake broker, because the question is what an operator's
//! pipeline demands at startup, not what one helper returns.

use std::path::{Path, PathBuf};

const TEMPLATE: &str = "cdc.${schema}.${table}";

/// One PostgreSQL source naming `public.orders`, plus the sink section and extras given.
fn pipeline_config(dir: &Path, sinks: &str, extra: &str) -> PathBuf {
    let config_path = dir.join("cdc.toml");
    std::fs::write(
        &config_path,
        format!(
            r#"
api_version = "v1"

[source.postgres]
host = "localhost"
user = "cdc_user"
password = {{ env = "CDC_TEST_SOURCE_PASSWORD" }}
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
table_include_list = ["public.orders"]

[source.postgres.transport]
mode = "plaintext"

{sinks}

{extra}

[state]
dir = "{}"
"#,
            dir.join("state").display()
        ),
    )
    .expect("write config");
    config_path
}

fn kafka_sink(header: &str, brokers: &str) -> String {
    format!("{header}\ntype = \"kafka\"\nbrokers = \"{brokers}\"\ntopic = \"{TEMPLATE}\"\n")
}

async fn preflight(config_path: &Path) -> Result<(), String> {
    let cfg = crate::config::load(config_path).expect("config loads");
    let mut built = super::build_router(&cfg).await.expect("router builds");
    crate::pipeline::router::preflight_check(&mut built.router)
        .await
        .map_err(|error| error.to_string())
}

/// The defect: preflight checked `cdc.public.orders` and passed, the first event for the
/// table was its schema announcement, and that went to `cdc.public.orders__ddl_events`,
/// which did not exist. The producer never created it, so the batch held every table
/// behind it until the delivery timeout, and the pipeline then exited.
#[tokio::test]
async fn preflight_demands_the_schema_event_topic_of_each_configured_table() {
    let _env = crate::test_env::EnvGuard::set(&[("CDC_TEST_SOURCE_PASSWORD", "pg-secret")]);
    let broker = krafka::testing::FakeBroker::start()
        .await
        .expect("fake broker");
    broker.create_topic("cdc.public.orders", 1);

    let dir = tempfile::tempdir().expect("tempdir");
    let config_path = pipeline_config(
        dir.path(),
        &kafka_sink("[sink]", &broker.bootstrap_servers()),
        "",
    );

    let message = preflight(&config_path)
        .await
        .expect_err("the schema-event topic was never created");
    assert!(
        message.contains("cdc.public.orders__ddl_events"),
        "the missing topic must be named: {message}"
    );

    broker.create_topic("cdc.public.orders__ddl_events", 1);
    preflight(&config_path)
        .await
        .expect("with both topics present, preflight passes");
}

/// A pipeline that filters schema events out never publishes them, so it must not be
/// failed over their topic. The decision is the pipeline's own transform rules, run
/// against a schema event, not a second reading of the configuration.
#[tokio::test]
async fn a_pipeline_that_drops_schema_events_is_not_asked_for_their_topic() {
    let _env = crate::test_env::EnvGuard::set(&[("CDC_TEST_SOURCE_PASSWORD", "pg-secret")]);
    let broker = krafka::testing::FakeBroker::start()
        .await
        .expect("fake broker");
    broker.create_topic("cdc.public.orders", 1);

    let dir = tempfile::tempdir().expect("tempdir");
    let config_path = pipeline_config(
        dir.path(),
        &kafka_sink("[sink]", &broker.bootstrap_servers()),
        r#"
[[pipeline.transforms]]
name = "drop-schema-events"

  [[pipeline.transforms.actions]]
  type        = "filter"
  exclude_ops = ["schema_change"]
"#,
    );

    preflight(&config_path)
        .await
        .expect("schema events are filtered, so their topic is never written");
}

/// Schema events are routed by their own name, `public.orders__ddl_events`, exactly as
/// the router routes them at runtime. A route for `public.orders` does not claim them, so
/// its sink must not be asked for their topic; they fall through to the default sink.
#[tokio::test]
async fn schema_event_topics_follow_the_routes_that_would_carry_them() {
    let _env = crate::test_env::EnvGuard::set(&[("CDC_TEST_SOURCE_PASSWORD", "pg-secret")]);
    let broker = krafka::testing::FakeBroker::start()
        .await
        .expect("fake broker");
    broker.create_topic("cdc.public.orders", 1);

    let dir = tempfile::tempdir().expect("tempdir");
    let sinks = format!(
        "[sink]\ntype = \"stdout\"\n\n{}\n[[pipeline.routes]]\ntable_pattern = \"public.orders\"\nsink = \"orders\"\n",
        kafka_sink("[[sinks]]\nname = \"orders\"", &broker.bootstrap_servers())
    );
    let config_path = pipeline_config(dir.path(), &sinks, "");
    preflight(&config_path)
        .await
        .expect("the route claims only the data table; schema events go to stdout");

    // Widen the route so it claims the schema events too, and the topic is demanded.
    let widened = sinks.replace(
        "table_pattern = \"public.orders\"",
        "table_pattern = \"public.orders*\"",
    );
    let config_path = pipeline_config(dir.path(), &widened, "");
    let message = preflight(&config_path)
        .await
        .expect_err("the widened route now carries the schema events");
    assert!(
        message.contains("cdc.public.orders__ddl_events"),
        "{message}"
    );
}

/// A `route` action can rename a schema event's table, and that is the name its topic is
/// rendered from. This is the case that makes running the rules necessary rather than just
/// looking for `exclude_ops`.
#[tokio::test]
async fn a_route_action_that_renames_schema_events_moves_the_topic_checked() {
    let _env = crate::test_env::EnvGuard::set(&[("CDC_TEST_SOURCE_PASSWORD", "pg-secret")]);
    let broker = krafka::testing::FakeBroker::start()
        .await
        .expect("fake broker");
    broker.create_topic("cdc.public.orders", 1);

    let dir = tempfile::tempdir().expect("tempdir");
    let config_path = pipeline_config(
        dir.path(),
        &kafka_sink("[sink]", &broker.bootstrap_servers()),
        r#"
[[pipeline.transforms]]
name = "one-topic-for-schema-events"

  [pipeline.transforms.when]
  ops = ["schema_change"]

  [[pipeline.transforms.actions]]
  type  = "route"
  table = "schema_events"
"#,
    );

    let message = preflight(&config_path)
        .await
        .expect_err("the renamed topic does not exist yet");
    assert!(message.contains("cdc.public.schema_events"), "{message}");
    assert!(!message.contains("orders__ddl_events"), "{message}");

    broker.create_topic("cdc.public.schema_events", 1);
    preflight(&config_path)
        .await
        .expect("the topic the renamed events go to exists");
}

/// A rule that reads the schema event's payload must see the payload a connector sends.
/// `unwrap` on `result_schema` succeeds on a real announcement, so the event is published
/// and its topic has to be checked. A probe with an empty payload made this rule fail,
/// counted the event as dropped, and left the topic unchecked: the stall from #21 again.
#[tokio::test]
async fn a_rule_that_reads_the_schema_payload_still_leaves_the_topic_checked() {
    let _env = crate::test_env::EnvGuard::set(&[("CDC_TEST_SOURCE_PASSWORD", "pg-secret")]);
    let broker = krafka::testing::FakeBroker::start()
        .await
        .expect("fake broker");
    broker.create_topic("cdc.public.orders", 1);

    let dir = tempfile::tempdir().expect("tempdir");
    let config_path = pipeline_config(
        dir.path(),
        &kafka_sink("[sink]", &broker.bootstrap_servers()),
        r#"
[[pipeline.transforms]]
name = "columns-only"

  [pipeline.transforms.when]
  ops = ["schema_change"]

  [[pipeline.transforms.actions]]
  type  = "unwrap"
  field = "result_schema"
"#,
    );

    let message = preflight(&config_path)
        .await
        .expect_err("the rule keeps the event, so its topic is required");
    assert!(
        message.contains("cdc.public.orders__ddl_events"),
        "{message}"
    );
}
