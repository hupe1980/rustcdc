/// Architecture invariant tests.
///
/// These source-level assertions enforce the structural boundaries defined in
/// REFACTOR.md so that breaking changes are caught at compile/test time rather
/// than at code review.
use std::fs;
use std::path::Path; // used by glob_src / read_src

fn read_src(relative: &str) -> String {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join(relative);
    fs::read_to_string(&path).unwrap_or_else(|e| panic!("failed to read {relative}: {e}"))
}

fn glob_src(dir: &str) -> Vec<(String, String)> {
    let base = Path::new(env!("CARGO_MANIFEST_DIR")).join(dir);
    let mut result = Vec::new();
    if let Ok(entries) = fs::read_dir(&base) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().map(|e| e == "rs").unwrap_or(false) {
                let name = path.to_string_lossy().into_owned();
                let src = fs::read_to_string(&path)
                    .unwrap_or_else(|e| panic!("failed to read {name}: {e}"));
                result.push((name, src));
            }
        }
    }
    result
}

// ─────────────────────────────────────────────────────────────────────────────
// Sink config invariants
// ─────────────────────────────────────────────────────────────────────────────

fn read_schema_src() -> String {
    let mut combined = String::new();
    for (_, src) in glob_src("src/config") {
        combined.push_str(&src);
        combined.push('\n');
    }
    combined
}

/// `SinkConfig` must not have an `Avro` variant — Avro is a codec, not a
/// transport. Use `KafkaSinkConfig.codec` with `type = "avro_confluent"`.
#[test]
fn sink_config_has_no_avro_variant() {
    let src = read_schema_src();
    assert!(
        !src.contains("Avro("),
        "SinkConfig::Avro must not exist — encode via KafkaSinkConfig.codec instead"
    );
}

/// `SinkConfig` must not have an `Otel` variant — OTEL is not a delivery
/// target. CDC events reach OTEL through the telemetry stack in telemetry.rs.
#[test]
fn sink_config_has_no_otel_variant() {
    let src = read_schema_src();
    assert!(
        !src.contains("Otel("),
        "SinkConfig::Otel must not exist — OTEL is telemetry infrastructure, not a sink"
    );
}

/// Sink config structs must not embed registry credentials inline; credentials
/// live in `[registries.<name>]` or inside `CodecConfig`.
#[test]
fn sink_config_has_no_inline_registry_password() {
    let src = read_schema_src();
    // The HttpSinkConfig.bearer_token is acceptable (it's a transport auth, not a
    // schema registry credential). Guard only against raw `password` or
    // `schema_registry_password` style fields on sink structs.
    assert!(
        !src.contains("registry_password"),
        "Sink config must not contain inline registry_password fields — use [registries.*]"
    );
    assert!(
        !src.contains("registry_url"),
        "Sink config must not contain inline registry_url fields — use [registries.*]"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Codec / transport separation invariants
// ─────────────────────────────────────────────────────────────────────────────

/// `src/sink/` transport modules must not import the `schemreg` crate
/// directly. Codec logic lives in `src/codec/`.
#[test]
fn sink_modules_do_not_import_schemreg() {
    for (path, src) in glob_src("src/sink") {
        assert!(
            !src.contains("schemreg"),
            "{path} must not import schemreg — schema registry logic belongs in src/codec/"
        );
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Config versioning invariants
// ─────────────────────────────────────────────────────────────────────────────

/// The supported API version must be `"v1"`.
#[test]
fn supported_api_version_is_v1() {
    let src = read_schema_src();
    assert!(
        src.contains(r#"SUPPORTED_API_VERSION: &'static str = "v1""#),
        "SUPPORTED_API_VERSION must be \"v1\""
    );
}

/// `config/migrations.rs` must not contain cascading multi-hop version chains.
/// Only v1alpha1 → v1 legacy alias is permitted.
#[test]
fn migrations_has_no_v2_or_v3_hop() {
    let src = read_src("src/config/migrations.rs");
    assert!(
        !src.contains("\"v2\""),
        "migrations.rs must not reference v2 — only v1 is the target version"
    );
    assert!(
        !src.contains("\"v3\""),
        "migrations.rs must not reference v3 — only v1 is the target version"
    );
}
