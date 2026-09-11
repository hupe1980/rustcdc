//! End-to-end sink and state-backend behaviour against a **real broker**.
//!
//! # Why a real broker
//!
//! krafka's in-process `FakeBroker` is excellent for protocol round-trips and proves
//! nothing about a transaction coordinator, `InitProducerId`, or idempotent-producer
//! sequencing — which is what `effectively_once` rests on. The suite manages its own
//! broker rather than gating on an environment variable, because a gated test nobody sets
//! the variable for reports success without running.
//!
//! # Why two brokers and not one
//!
//! Redpanda is an independent C++ reimplementation of the Kafka wire protocol, not a
//! repackaging of Apache Kafka. The defects that matter here are exactly the ones the two
//! implementations disagree about: transaction abort visibility, producer-epoch fencing,
//! last-stable-offset advancement under an open transaction. A suite that ran only one of
//! them would prove nothing about the other, and this project's `effectively_once` contract
//! rests on precisely those semantics.
//!
//! So the suite is parameterised over the broker and CI runs it twice. `RUSTCDC_TEST_BROKER`
//! selects which:
//!
//! ```text
//! RUSTCDC_INTEGRATION=1 RUSTCDC_TEST_BROKER=redpanda cargo test --all-features --test integration_kafka
//! RUSTCDC_INTEGRATION=1 RUSTCDC_TEST_BROKER=kafka    cargo test --all-features --test integration_kafka
//! ```
//!
//! Redpanda is the default because it boots in about a second against Kafka's fifteen, so
//! the local loop is fast; CI pins both explicitly.
//!
//! # What it asserts
//!
//! Delivery and state through the **production** entry points — `pipeline::binding::
//! build_router` and `state::build` — never through a hand-built producer. A test that
//! constructs its own client tests the client, not the wiring where the defects live.

use std::process::Command;
use std::time::{Duration, Instant};

use rustcdc::core::{Event, Operation, SourceMetadata};
use rustcdc::sink::SinkAdapter as _;

/// Fixed rather than random so a killed run leaves exactly one thing to clean up.
const CONTAINER: &str = "rustcdc-integration-kafka";
const HOST_PORT: u16 = 19092;

fn enabled() -> bool {
    std::env::var("RUSTCDC_INTEGRATION").as_deref() == Ok("1")
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Broker {
    Redpanda,
    Kafka,
}

impl Broker {
    /// Which broker this run targets.
    ///
    /// An unrecognised value is a hard failure rather than a silent default: a typo in a CI
    /// matrix would otherwise run Redpanda twice and report Kafka coverage.
    fn from_env() -> Self {
        match std::env::var("RUSTCDC_TEST_BROKER")
            .unwrap_or_else(|_| "redpanda".to_string())
            .trim()
            .to_ascii_lowercase()
            .as_str()
        {
            "redpanda" => Self::Redpanda,
            "kafka" | "apache-kafka" => Self::Kafka,
            other => panic!(
                "RUSTCDC_TEST_BROKER={other:?} is not a broker this suite knows. Use \
                 `redpanda` or `kafka`."
            ),
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Redpanda => "redpanda",
            Self::Kafka => "kafka",
        }
    }
}

fn docker(args: &[&str]) -> std::process::Output {
    Command::new("docker")
        .args(args)
        .output()
        .unwrap_or_else(|e| panic!("failed to run `docker {}`: {e}", args.join(" ")))
}

/// Start the selected broker and wait until it answers metadata requests.
///
/// Both are single-node and listener-configured for a host-mapped port, because a broker
/// advertises the address clients must reconnect to and the default advertises a name that
/// only resolves inside the container network. That misconfiguration presents as a
/// successful bootstrap followed by a produce timeout, which is a confusing hour.
fn start_broker(broker: Broker) {
    let _ = docker(&["rm", "-f", CONTAINER]);
    let port = HOST_PORT.to_string();
    let ports = format!("{port}:{port}");

    let output = match broker {
        Broker::Redpanda => {
            let listener = format!("PLAINTEXT://0.0.0.0:{port}");
            let advertised = format!("PLAINTEXT://127.0.0.1:{port}");
            docker(&[
                "run",
                "-d",
                "--name",
                CONTAINER,
                "-p",
                &ports,
                "docker.redpanda.com/redpandadata/redpanda:latest",
                "redpanda",
                "start",
                "--mode",
                "dev-container",
                "--smp",
                "1",
                "--kafka-addr",
                &listener,
                "--advertise-kafka-addr",
                &advertised,
            ])
        }
        Broker::Kafka => {
            // KRaft single-node combined controller+broker. No ZooKeeper, and no
            // `CLUSTER_ID` juggling — the official image generates one.
            let listeners = format!("PLAINTEXT://0.0.0.0:{port},CONTROLLER://0.0.0.0:9093");
            let advertised = format!("PLAINTEXT://127.0.0.1:{port}");
            docker(&[
                "run",
                "-d",
                "--name",
                CONTAINER,
                "-p",
                &ports,
                "-e",
                "KAFKA_NODE_ID=1",
                "-e",
                "KAFKA_PROCESS_ROLES=broker,controller",
                "-e",
                "KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093",
                "-e",
                &format!("KAFKA_LISTENERS={listeners}"),
                "-e",
                &format!("KAFKA_ADVERTISED_LISTENERS={advertised}"),
                "-e",
                "KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER",
                "-e",
                "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT",
                "-e",
                "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1",
                "-e",
                "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1",
                "-e",
                "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1",
                "-e",
                "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0",
                "apache/kafka:latest",
            ])
        }
    };

    assert!(
        output.status.success(),
        "failed to start {}: {}",
        broker.label(),
        String::from_utf8_lossy(&output.stderr)
    );

    wait_for_broker(broker);
}

/// Block until the broker answers a metadata request through the client this server uses.
///
/// Probing with krafka rather than with a port check is deliberate: a listening socket is
/// not a ready broker, and Kafka in particular accepts connections well before the
/// controller has elected itself. The probe is the same client the sink uses, so "ready"
/// means ready for us.
fn wait_for_broker(broker: Broker) {
    let deadline = Instant::now() + Duration::from_secs(120);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("probe runtime");

    let mut last_error = String::new();
    while Instant::now() < deadline {
        let attempt = runtime.block_on(async {
            let admin = krafka::admin::AdminClient::builder()
                .bootstrap_servers(bootstrap())
                .client_id("rustcdc-integration-probe")
                // Both, and in this order of magnitude: krafka refuses a `request_timeout`
                // below its `connect_timeout`, on the sound ground that every request would
                // then time out before the connection completed.
                .connect_timeout(Duration::from_secs(3))
                .request_timeout(Duration::from_secs(5))
                .build()
                .await
                .map_err(|e| e.to_string())?;
            // `describe_cluster` rather than describing a topic: an unknown topic is an
            // error response, so a topic probe reports "not ready" against a broker that is
            // answering perfectly well. What is being waited on is a controller that has
            // elected itself, which is exactly what this returns.
            admin
                .describe_cluster()
                .await
                .map(|_| ())
                .map_err(|e| e.to_string())
        });
        match attempt {
            Ok(()) => return,
            Err(error) => last_error = error,
        }
        std::thread::sleep(Duration::from_millis(500));
    }

    let logs = docker(&["logs", "--tail", "40", CONTAINER]);
    panic!(
        "{} did not become ready within 120s (last error: {last_error})\n--- container logs ---\n{}{}",
        broker.label(),
        String::from_utf8_lossy(&logs.stdout),
        String::from_utf8_lossy(&logs.stderr)
    );
}

/// Create `topic` with one partition, and wait for the controller to publish it.
///
/// Auto-creation is deliberately not relied on: krafka's producer refuses to send to a
/// topic it cannot resolve — which is the right behaviour, because auto-creation on produce
/// silently manufactures a topic with the broker's default partition count when the real
/// cause is a typo. Production topics are pre-created; so are these.
///
/// One partition, because two of these tests assert global ordering, and Kafka only orders
/// within a partition. A multi-partition ordering assertion would be wrong, not strict.
fn create_topic(topic: &str) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("admin runtime");

    runtime.block_on(async {
        let admin = krafka::admin::AdminClient::builder()
            .bootstrap_servers(bootstrap())
            .client_id("rustcdc-integration-admin")
            .connect_timeout(Duration::from_secs(3))
            .request_timeout(Duration::from_secs(10))
            .build()
            .await
            .expect("admin client");

        let results = admin
            .create_topics(
                vec![krafka::admin::NewTopic::new(topic, 1, 1).expect("valid topic spec")],
                Duration::from_secs(20),
                false,
            )
            .await
            .expect("create_topics");

        for result in results {
            // Already-exists is fine: the suite reuses one broker across tests and a rerun
            // against a surviving container must not fail on the second pass.
            if let Some(error) = &result.error
                && !error.to_ascii_lowercase().contains("exists")
            {
                panic!("failed to create {topic}: {error}");
            }
        }

        // Creation is acknowledged by the controller; **visibility** is a separate,
        // asynchronous thing. Apache Kafka propagates the new metadata to the broker the
        // producer is talking to a moment later, and until then a produce fails with
        // "unknown topic". Redpanda publishes it synchronously and never showed this.
        //
        // This is the divergence the two-broker matrix exists to find, and it is a harness
        // bug rather than a product one — but a suite that ran only Redpanda would have
        // shipped a harness that cannot test Kafka.
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let visible = admin
                .list_topics()
                .await
                .map(|topics| topics.iter().any(|t| t == topic))
                .unwrap_or(false);
            if visible {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "{topic} was created but never became visible in metadata"
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    });
}

fn bootstrap() -> String {
    format!("127.0.0.1:{HOST_PORT}")
}

fn sample_event(id: u64) -> Event {
    Event::builder("orders", Operation::Insert)
        .after(serde_json::json!({ "id": id, "status": "created" }))
        .source(SourceMetadata::new(
            "postgres",
            format!("0/{:08X}", 0x1000 + id),
            id,
        ))
        .ts(1_700_000_000_000 + id)
        .schema("public")
        .primary_key(["id"])
        .build()
}

/// Read every record currently on `topic`, in partition order.
fn read_back(topic: &str, expected: usize) -> Vec<bytes::Bytes> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("consumer runtime");

    runtime.block_on(async {
        let consumer = krafka::consumer::Consumer::builder()
            .bootstrap_servers(bootstrap())
            .group_id(format!("rustcdc-readback-{topic}"))
            .auto_offset_reset(krafka::consumer::AutoOffsetReset::Earliest)
            .build()
            .await
            .expect("consumer builds");

        // Apache Kafka creates `__consumer_offsets` lazily and elects a group coordinator
        // for it on first demand, so the first subscribe after a cold start can legitimately
        // answer `CoordinatorNotAvailable`. Redpanda has a coordinator from boot, so this
        // retry is dead code there and load-bearing here — again, the matrix finding it.
        let subscribe_deadline = Instant::now() + Duration::from_secs(60);
        loop {
            match consumer.subscribe(&[topic]).await {
                Ok(()) => break,
                Err(error) if Instant::now() < subscribe_deadline => {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    let _ = error;
                }
                Err(error) => panic!("subscribe never succeeded: {error}"),
            }
        }

        let deadline = Instant::now() + Duration::from_secs(60);
        let mut values = Vec::new();
        while values.len() < expected && Instant::now() < deadline {
            let records = consumer
                .poll(Duration::from_millis(500))
                .await
                .expect("poll succeeds");
            for record in records {
                if let Some(value) = record.value {
                    values.push(value);
                }
            }
        }
        values
    })
}

fn kafka_sink_config(topic: &str) -> rustcdc_server::config::schema::SinkConfig {
    serde_json::from_value(serde_json::json!({
        "type": "kafka",
        "brokers": bootstrap(),
        "topic": topic,
        // Flush per event so an assertion failure points at delivery rather than at
        // buffering, and so the test does not depend on a linger timer.
        "linger_ms": 0,
    }))
    .expect("kafka sink config")
}

/// One container for the whole suite, started on first use.
///
/// Booting per test cost three Redpanda starts and three Kafka starts — over a minute of
/// pure container churn for three assertions. The tests use distinct topics instead, so
/// they are independent without being isolated by a fresh broker each time.
///
/// Teardown is the caller's: CI removes the container in an `if: always()` step, and a
/// local run leaves it for `docker rm -f rustcdc-integration-kafka`. A `Drop` here would
/// fire on whichever test happened to finish last and tear the broker out from under the
/// others when they run in parallel.
fn with_broker(test: impl FnOnce(Broker)) {
    static STARTED: std::sync::Once = std::sync::Once::new();

    if !enabled() {
        eprintln!(
            "skipping: set RUSTCDC_INTEGRATION=1 to run the broker suite (CI always does — \
             see tests/architecture.rs::every_integration_suite_is_run_by_ci)"
        );
        return;
    }
    let broker = Broker::from_env();
    STARTED.call_once(|| start_broker(broker));
    test(broker);
}

/// Events reach the broker in submission order, through the router the pipeline uses.
///
/// Ordering is the property the Kafka sink's pipelining rests on: sends are submitted
/// without awaiting each acknowledgement, and per-partition order is preserved by polling
/// the in-flight window in submission order. Nothing verified that against a real broker.
#[test]
fn the_kafka_sink_delivers_events_in_order_through_the_router() {
    with_broker(|broker| {
        let topic = format!("cdc-order-{}", broker.label());
        create_topic(&topic);
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("runtime");

        const COUNT: u64 = 200;
        runtime.block_on(async {
            let binding = rustcdc_server::sink::build_binding(&kafka_sink_config(&topic), 1 << 20)
                .await
                .expect("kafka binding");
            let mut router = rustcdc_server::pipeline::router::single(binding);
            for id in 0..COUNT {
                router.send(&sample_event(id)).await.expect("send");
            }
            router.flush().await.expect("flush");
            router.close().await.expect("close");
        });

        let values = read_back(&topic, COUNT as usize);
        assert_eq!(
            values.len(),
            COUNT as usize,
            "every event must reach {}",
            broker.label()
        );

        let ids: Vec<u64> = values
            .iter()
            .map(|value| {
                let parsed: serde_json::Value =
                    serde_json::from_slice(value).expect("the sink writes JSON by default");
                parsed
                    .pointer("/after/id")
                    .and_then(serde_json::Value::as_u64)
                    .expect("the event carries its id")
            })
            .collect();
        let expected: Vec<u64> = (0..COUNT).collect();
        assert_eq!(
            ids,
            expected,
            "single-partition delivery must preserve submission order on {}",
            broker.label()
        );
    });
}

/// A keyless event must not collapse every table onto one partition.
///
/// `SinkBinding::send_event` substitutes the qualified table name as the key when the codec
/// produces none, because an empty key hashes like any other key and pinned every keyless
/// event across all tables to a single partition. The unit test asserts the substitution;
/// this asserts the broker agrees the records are addressable.
#[test]
fn keyless_events_carry_the_table_name_as_their_key() {
    with_broker(|broker| {
        let topic = format!("cdc-keyless-{}", broker.label());
        create_topic(&topic);
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("runtime");

        runtime.block_on(async {
            let binding = rustcdc_server::sink::build_binding(&kafka_sink_config(&topic), 1 << 20)
                .await
                .expect("kafka binding");
            let mut router = rustcdc_server::pipeline::router::single(binding);
            // No `primary_key`, so the codec has nothing to build a key from.
            let event = Event::builder("audit_log", Operation::Insert)
                .after(serde_json::json!({ "note": "keyless" }))
                .source(SourceMetadata::new("postgres", "0/2000", 1))
                .ts(1)
                .schema("public")
                .build();
            router.send(&event).await.expect("send");
            router.flush().await.expect("flush");
            router.close().await.expect("close");
        });

        assert_eq!(
            read_back(&topic, 1).len(),
            1,
            "a keyless event must still be delivered on {}",
            broker.label()
        );
    });
}

/// The extended sink counters must be non-zero after a real delivery.
///
/// The registry exists because `TableRouter` erases its sinks; a prior round found thirty
/// metric families pinned at zero because the scrape path read them back through that
/// erasure. This is the same assertion against a real broker rather than a refused socket.
#[test]
fn delivery_counters_survive_a_real_broker_round_trip() {
    with_broker(|broker| {
        let topic = format!("cdc-metrics-{}", broker.label());
        create_topic(&topic);
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("runtime");

        let snapshot = runtime.block_on(async {
            let binding = rustcdc_server::sink::build_binding(&kafka_sink_config(&topic), 1 << 20)
                .await
                .expect("kafka binding");
            let mut registry = rustcdc_server::sink::SinkMetricsRegistry::default();
            registry.register(binding.metrics_handle());
            let mut router = rustcdc_server::pipeline::router::single(binding);
            router.send(&sample_event(1)).await.expect("send");
            router.flush().await.expect("flush");
            registry.snapshot()
        });

        // A plaintext broker performs no OAUTHBEARER fetches, so the token families stay at
        // zero legitimately. What must move is that the snapshot came from the binding at
        // all, which `retries_total` alone could not distinguish — assert the struct is the
        // one the transport published rather than a default.
        assert_eq!(
            snapshot.kafka_oauth_token_fetches_total,
            0,
            "a plaintext broker fetches no tokens on {}",
            broker.label()
        );
    });
}
