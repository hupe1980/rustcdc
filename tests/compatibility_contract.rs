use std::{fs, path::PathBuf, time::Duration};

use rustcdc::{
    checkpoint::{Checkpoint, FileCheckpoint, MysqlOffset},
    Event, EVENT_ENVELOPE_VERSION,
};
use tempfile::tempdir;

fn fixture(path: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(path)
}

#[cfg(unix)]
fn secure_file_permissions(path: &std::path::Path) {
    use std::os::unix::fs::PermissionsExt;

    let mut perms = fs::metadata(path)
        .expect("copied checkpoint fixture metadata should load")
        .permissions();
    perms.set_mode(0o600);
    fs::set_permissions(path, perms)
        .expect("copied checkpoint fixture permissions should be set to 0600");
}

#[cfg(not(unix))]
fn secure_file_permissions(_path: &std::path::Path) {}

fn copy_checkpoint_fixture(src: PathBuf, dst: PathBuf) {
    fs::copy(src, &dst).expect("compat checkpoint fixture copy should succeed");
    secure_file_permissions(&dst);
}

#[test]
fn event_envelope_fixture_decodes_and_validates() {
    let raw = fs::read_to_string(fixture("fixtures/compatibility/event_envelope.json"))
        .expect("compat event fixture should exist");
    let event = Event::from_json(&raw).expect("compat event fixture should decode");

    assert_eq!(event.envelope_version, EVENT_ENVELOPE_VERSION);
    assert!(
        event.validate().is_ok(),
        "compat event should pass validation"
    );
}

#[tokio::test]
async fn checkpoint_fixture_round_trips_via_file_checkpoint_backend() {
    let dir = tempdir().expect("tempdir should be created");
    let checkpoint_dir = dir.path();

    copy_checkpoint_fixture(
        fixture("fixtures/compatibility/checkpoint_postgres.json"),
        checkpoint_dir.join("checkpoint_postgres.json"),
    );

    // Ensure deterministic mtime ordering for latest-record selection.
    std::thread::sleep(Duration::from_millis(2));

    copy_checkpoint_fixture(
        fixture("fixtures/compatibility/checkpoint_postgres_snapshot.json"),
        checkpoint_dir.join("checkpoint_postgres_snapshot.json"),
    );

    let checkpoint = FileCheckpoint::new(checkpoint_dir);
    let loaded = checkpoint
        .load()
        .await
        .expect("compat checkpoint fixtures should load")
        .expect("compat checkpoint fixtures should produce an offset");

    assert!(
        matches!(loaded.source_type(), "postgres" | "postgres_snapshot"),
        "compat loader should accept stream/snapshot source family variants"
    );

    let committed = checkpoint
        .get_committed_count()
        .await
        .expect("compat checkpoint committed count should load");
    assert!(
        matches!(committed, 21 | 42),
        "compat checkpoint committed count should match compatibility fixtures"
    );
}

#[tokio::test]
async fn checkpoint_fixtures_cover_mysql_and_sqlserver_source_families() {
    struct CheckpointFixtureCase {
        fixture_name: &'static str,
        checkpoint_file: &'static str,
        expected_source_type: &'static str,
        expected_committed_count: u64,
    }

    let cases = [
        CheckpointFixtureCase {
            fixture_name: "checkpoint_mysql.json",
            checkpoint_file: "checkpoint_mysql.json",
            expected_source_type: "mysql",
            expected_committed_count: 84,
        },
        CheckpointFixtureCase {
            fixture_name: "checkpoint_mysql_snapshot.json",
            checkpoint_file: "checkpoint_mysql_snapshot.json",
            expected_source_type: "mysql_snapshot",
            expected_committed_count: 33,
        },
        CheckpointFixtureCase {
            fixture_name: "checkpoint_sqlserver.json",
            checkpoint_file: "checkpoint_sqlserver.json",
            expected_source_type: "sqlserver",
            expected_committed_count: 65,
        },
        CheckpointFixtureCase {
            fixture_name: "checkpoint_sqlserver_snapshot.json",
            checkpoint_file: "checkpoint_sqlserver_snapshot.json",
            expected_source_type: "sqlserver_snapshot",
            expected_committed_count: 28,
        },
    ];

    for case in cases {
        let dir = tempdir().expect("tempdir should be created");
        let checkpoint_dir = dir.path();

        copy_checkpoint_fixture(
            fixture(&format!("fixtures/compatibility/{}", case.fixture_name)),
            checkpoint_dir.join(case.checkpoint_file),
        );

        let checkpoint = FileCheckpoint::new(checkpoint_dir);
        let loaded = checkpoint
            .load()
            .await
            .expect("compat checkpoint fixture should load")
            .expect("compat checkpoint fixture should produce an offset");

        assert_eq!(
            loaded.source_type(),
            case.expected_source_type,
            "compat loader should preserve source type for fixture '{}'",
            case.fixture_name
        );

        if loaded.source_type() == "mysql" {
            let decoded =
                MysqlOffset::from_bytes(&loaded.encode().expect("mysql offset should encode"))
                    .expect("mysql stream checkpoint fixture should decode as typed mysql offset");
            assert_eq!(decoded.binlog_file, "mysql-bin.000123");
            assert_eq!(decoded.binlog_pos, 987654);
            assert_eq!(decoded.gtid, "24BC7856-9A30-11EE-8B4B-0242AC120002:1-9");
        }

        let committed = checkpoint
            .get_committed_count()
            .await
            .expect("compat checkpoint committed count should load");
        assert_eq!(
            committed, case.expected_committed_count,
            "compat checkpoint committed count should match fixture '{}'",
            case.fixture_name
        );
    }
}
