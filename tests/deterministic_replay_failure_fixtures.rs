use std::path::PathBuf;

use cdc_rs::deterministic_replay::{Fixture, ReplaySession};

fn fixture_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("fixtures/deterministic_replay")
}

fn load_fixture(name: &str) -> Fixture {
    let path = fixture_root().join(name);
    Fixture::from_path(&path)
        .unwrap_or_else(|error| panic!("failed loading fixture '{}': {error}", path.display()))
}

#[test]
fn postgres_crash_interrupted_transaction_fixture_fails_closed() {
    let fixture = load_fixture("postgres_crash_interrupted_transaction_v1.fixture.json");
    let mut replay = ReplaySession::new(fixture).expect("replay session creation");
    let result = replay.replay();

    assert!(
        !result.success,
        "replay should fail for interrupted transaction"
    );
    assert!(result
        .errors
        .iter()
        .any(|error| error.contains("not committed")));
    assert_eq!(replay.events().len(), 4);
    assert_eq!(
        replay.events()[0].event.table,
        "__marker__transaction_begin"
    );
    assert_eq!(replay.events()[1].event.table, "users");
    assert_eq!(
        replay.events()[1]
            .event
            .transaction
            .as_ref()
            .expect("committed event should carry transaction metadata")
            .total_events,
        1
    );
    assert_eq!(replay.events()[2].event.table, "__marker__transaction_end");
    assert_eq!(
        replay.events()[3].event.table,
        "__marker__transaction_begin"
    );
}

#[test]
fn mysql_crash_interrupted_transaction_fixture_fails_closed() {
    let fixture = load_fixture("mysql_crash_interrupted_transaction_v1.fixture.json");
    let mut replay = ReplaySession::new(fixture).expect("replay session creation");
    let result = replay.replay();

    assert!(
        !result.success,
        "replay should fail for interrupted transaction"
    );
    assert!(result
        .errors
        .iter()
        .any(|error| error.contains("not committed")));
    assert_eq!(replay.events().len(), 4);
    assert_eq!(
        replay.events()[0].event.table,
        "__marker__transaction_begin"
    );
    assert_eq!(replay.events()[1].event.table, "products");
    assert_eq!(
        replay.events()[1]
            .event
            .transaction
            .as_ref()
            .expect("committed event should carry transaction metadata")
            .total_events,
        1
    );
    assert_eq!(replay.events()[2].event.table, "__marker__transaction_end");
    assert_eq!(
        replay.events()[3].event.table,
        "__marker__transaction_begin"
    );
}

#[test]
fn sqlserver_crash_interrupted_transaction_fixture_fails_closed() {
    let fixture = load_fixture("sqlserver_crash_interrupted_transaction_v1.fixture.json");
    let mut replay = ReplaySession::new(fixture).expect("replay session creation");
    let result = replay.replay();

    assert!(
        !result.success,
        "replay should fail for interrupted transaction"
    );
    assert!(result
        .errors
        .iter()
        .any(|error| error.contains("not committed")));
    assert_eq!(replay.events().len(), 1);
    assert_eq!(
        replay.events()[0].event.table,
        "__marker__transaction_begin"
    );
}
