/// Architecture invariant tests.
///
/// These source-level assertions enforce the structural boundaries this crate relies on,
/// so that breaking one is caught by the test suite rather than at code review — or not
/// at all.
///
/// Each test states the invariant and why it exists; a bare grep with no rationale is
/// indistinguishable from a lint nobody can safely delete.
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

/// The body of the `SinkConfig` enum — the *transport* enum specifically.
///
/// The codec/transport guards below have to look here rather than at all of
/// `src/config/`: a substring search over the whole directory also matches codec
/// variants, and `CodecConfig::GlueAvro(..)` is a perfectly legitimate codec that a
/// naive `contains("Avro(")` reported as a forbidden sink transport.
fn read_sink_config_enum_body() -> String {
    let src = read_schema_src();
    let start = src
        .find("pub enum SinkConfig {")
        .expect("SinkConfig enum must exist in src/config/");
    let rest = &src[start..];
    let end = rest
        .find("\n}")
        .expect("SinkConfig enum must be brace-terminated");
    rest[..end].to_string()
}

/// `SinkConfig` must not have an `Avro` variant — Avro is a codec, not a
/// transport. Use `KafkaSinkConfig.codec` with `type = "avro_confluent"`.
#[test]
fn sink_config_has_no_avro_variant() {
    let body = read_sink_config_enum_body();
    assert!(
        !body.contains("Avro"),
        "SinkConfig must have no Avro variant — encode via the sink's `codec` instead:\n{body}"
    );
}

/// The guard above must actually be looking at the enum, not the whole file.
///
/// A `contains` over all of `src/config/` passes trivially once the string moves
/// somewhere harmless, which is how a structural guard silently stops guarding.
#[test]
fn sink_config_enum_body_is_extracted_not_the_whole_file() {
    let body = read_sink_config_enum_body();
    assert!(
        body.contains("Kafka(") && body.contains("Iceberg("),
        "the extracted body must be the real SinkConfig enum:\n{body}"
    );
    assert!(
        !body.contains("pub enum CodecConfig"),
        "extraction must stop at the enum's closing brace, not run into the next item"
    );
    // The codec enum legitimately has an Avro-named variant; the guard must not see it.
    assert!(
        read_schema_src().contains("GlueAvro("),
        "this test is only meaningful while a codec variant contains \"Avro(\""
    );
}

/// `SinkConfig` must not have an `Otel` variant — OTEL is not a delivery
/// target. CDC events reach OTEL through the telemetry stack in telemetry.rs.
#[test]
fn sink_config_has_no_otel_variant() {
    let body = read_sink_config_enum_body();
    assert!(
        !body.contains("Otel"),
        "SinkConfig must have no Otel variant — OTEL is telemetry infrastructure, not a \
         sink:\n{body}"
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

// ─────────────────────────────────────────────────────────────────────────────
// Worker lifecycle invariants
// ─────────────────────────────────────────────────────────────────────────────

/// Background work belongs on the ambient tokio runtime, not on detached OS threads
/// running private ones.
///
/// The admin state used to start four workers with `std::thread::spawn`, each building a
/// `current_thread` runtime and looping unconditionally. That is four extra OS threads and
/// four extra timer/IO drivers per process, none of which could be stopped: no handle was
/// kept and no loop had an exit condition. Shutdown was therefore not orderly — the
/// signal-action worker went on mutating admin state while the pipeline finalised — and
/// every test that built an `AdminState` leaked its four threads for the lifetime of the
/// test binary.
///
/// The needle is assembled with `concat!` so this assertion does not match its own source.
#[test]
fn no_module_spawns_a_detached_thread_with_its_own_runtime() {
    let needle = concat!("thread::", "spawn(move");
    let mut offenders = Vec::new();

    for dir in ["src", "src/admin", "src/commands", "src/sink", "src/state"] {
        for (name, src) in glob_src(dir) {
            if src.contains(needle) {
                offenders.push(name);
            }
        }
    }

    assert!(
        offenders.is_empty(),
        "detached threads cannot be shut down and each carries a private tokio runtime; \
         use tokio::spawn with a shutdown watch instead. Offenders: {offenders:?}"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Module size
// ─────────────────────────────────────────────────────────────────────────────

/// No source file may exceed a size at which it stops being navigable.
///
/// This exists because the two largest modules grew for five consecutive review cycles
/// while "split them" sat on the plan. `admin/mod.rs` reached 7 927 lines and
/// `config/loader.rs` 4 318, and each round that deferred the split also added to them —
/// the cost rose monotonically with the delay and nothing made that visible until someone
/// counted.
///
/// The limit is deliberately generous. It is not a style preference about ideal file
/// length; it is a ratchet that stops the specific failure of a module doubling while
/// everyone agrees it should shrink. Splitting a file is mechanical and the compiler
/// verifies it, so hitting this is a prompt to do fifteen minutes of work, not to raise
/// the number.
#[test]
fn no_source_file_grows_past_the_point_of_navigability() {
    const MAX_LINES: usize = 4_000;

    let mut oversized = Vec::new();
    let mut stack = vec![Path::new(env!("CARGO_MANIFEST_DIR")).join("src")];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().map(|e| e == "rs").unwrap_or(false) {
                let lines = fs::read_to_string(&path)
                    .map(|s| s.lines().count())
                    .unwrap_or(0);
                if lines > MAX_LINES {
                    oversized.push((path.display().to_string(), lines));
                }
            }
        }
    }

    oversized.sort_by_key(|(_, lines)| std::cmp::Reverse(*lines));
    assert!(
        oversized.is_empty(),
        "these files exceed {MAX_LINES} lines and should be split by concern \
         (tests move to a sibling `*_tests.rs` cheaply): {oversized:?}"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Minimum supported Rust version
// ─────────────────────────────────────────────────────────────────────────────

/// Extract the MSRV from `Cargo.toml`.
fn declared_msrv() -> String {
    read_src("Cargo.toml")
        .lines()
        .find_map(|line| line.strip_prefix("rust-version"))
        .and_then(|rest| rest.split('"').nth(1).map(str::to_owned))
        .expect("Cargo.toml must declare rust-version")
}

/// Every place that restates the MSRV must agree with `Cargo.toml`.
///
/// The number appears in four files — the manifest, the Dockerfile's build stage, the
/// documentation site, and (derived, not hardcoded) the CI job. They were hand-synchronised
/// and they drifted: the manifest said `1.94` while CI pinned `1.94.0`, so when a
/// dependency raised its own requirement to `1.94.1` the build broke with no indication
/// which of the two numbers was wrong.
///
/// The README badge is deliberately included. A badge is the first MSRV statement most
/// people read, and a stale one is worse than none.
#[test]
fn every_statement_of_the_msrv_matches_cargo_toml() {
    let msrv = declared_msrv();
    let mut wrong = Vec::new();

    let dockerfile = read_src("Dockerfile");
    if !dockerfile.contains(&format!("ARG RUST_VERSION={msrv}")) {
        wrong.push(format!("Dockerfile: expected `ARG RUST_VERSION={msrv}`"));
    }

    let site_config = read_src("site/zola.toml");
    if !site_config.contains(&format!(r#"rust_version = "{msrv}""#)) {
        wrong.push(format!("site/zola.toml: expected `rust_version = \"{msrv}\"`"));
    }

    let readme = read_src("README.md");
    if !readme.contains(&format!("Rust {msrv}+")) {
        wrong.push(format!("README.md: badge/text should say `Rust {msrv}+`"));
    }

    // The CI job must *derive* the toolchain rather than restate it, or this test would
    // have to be updated in lockstep with a value it is supposed to be policing.
    let ci = read_src(".github/workflows/ci.yml");
    if !ci.contains("steps.msrv.outputs.version") {
        wrong.push(
            ".github/workflows/ci.yml: the MSRV job must read rust-version from Cargo.toml \
             rather than pin a literal toolchain"
                .to_owned(),
        );
    }

    assert!(
        wrong.is_empty(),
        "MSRV is declared as {msrv} in Cargo.toml but these disagree: {wrong:?}"
    );
}
