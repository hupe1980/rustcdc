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
///
/// **A ratchet only ratchets if it is tightened.** At a flat 4 000 this had never fired,
/// and a review found `admin/mod.rs` at 3 982 lines carrying five unrelated concerns —
/// HTTP handlers, state, workers, Prometheus rendering and a Kafka publisher — with a
/// worker-lifecycle defect sitting three thousand lines from the state it mutates. A guard
/// calibrated above the worst case in the tree is not doing work.
///
/// It fired at 4 144 during that remediation; `prometheus.rs` and `notify.rs` came out and
/// `admin/mod.rs` is now 3 599.
///
/// # Two budgets, because the harm is different
///
/// **Production modules get the tighter number.** Length there costs review quality
/// directly: the defect above was invisible precisely because its two halves could not be
/// held on screen together.
///
/// **Test files get the looser one.** A long test file is a navigation annoyance, not a
/// correctness risk — tests are read one function at a time and each states its own
/// premise. `admin/tests.rs` is the current offender at ~3 990 lines and does want
/// splitting along the command/observation seam, but that is a hand job: an automated
/// split by brace-matching is defeated by `{}` inside format strings, and corrupting a
/// 4 000-line test file to satisfy a style guard is a bad trade.
///
/// Lower both numbers after the next split. They should trail the largest file, not lead
/// it.
#[test]
fn no_source_file_grows_past_the_point_of_navigability() {
    /// Production modules: tight, because length here costs review quality.
    const MAX_LINES: usize = 3_700;
    /// Test modules: looser, and tracked separately — see the docs above.
    const MAX_TEST_LINES: usize = 4_000;

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
                let is_test_file = path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.contains("tests"));
                let budget = if is_test_file {
                    MAX_TEST_LINES
                } else {
                    MAX_LINES
                };
                if lines > budget {
                    oversized.push((path.display().to_string(), lines));
                }
            }
        }
    }

    oversized.sort_by_key(|(_, lines)| std::cmp::Reverse(*lines));
    assert!(
        oversized.is_empty(),
        "these files exceed their budget ({MAX_LINES} for a module, {MAX_TEST_LINES} for a \
         test file) and should be split by concern \
         (tests move to a sibling `*_tests.rs` cheaply): {oversized:?}"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Minimum supported Rust version
// ─────────────────────────────────────────────────────────────────────────────

/// Every place that restates the release version must agree with `Cargo.toml`.
///
/// `site/zola.toml` carries `version`, shown in the site header and in the page's
/// structured data, with a comment telling a human to "bump with the release". That is
/// exactly the arrangement the MSRV was in before it drifted — the manifest said `1.94`
/// while CI pinned `1.94.0`, and when a dependency raised its requirement the build broke
/// with no indication which number was wrong.
///
/// A stale version in the site header is less dangerous than a stale MSRV, but it is the
/// number a reader trusts to tell them what they are looking at, and nothing was checking
/// it. Now something is.
///
/// The OpenAPI document is deliberately **not** checked here: it reads
/// `env!("CARGO_PKG_VERSION")` at compile time, so it cannot drift, and its tests pass an
/// obviously-fake `0.0.0-test` rather than a literal that would invite a reflex bump.
#[test]
fn every_statement_of_the_release_version_matches_cargo_toml() {
    let version = read_src("Cargo.toml")
        .lines()
        .find_map(|line| line.strip_prefix("version = "))
        .and_then(|rest| rest.split('"').nth(1).map(str::to_owned))
        .expect("Cargo.toml must declare a version");

    let site_config = read_src("site/zola.toml");
    assert!(
        site_config.contains(&format!(r#"version = "{version}""#)),
        "site/zola.toml: expected `version = \"{version}\"` to match Cargo.toml. It is \
         rendered in the site header and structured data, so a stale value misreports which \
         release the documentation describes."
    );
}

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
        wrong.push(format!(
            "site/zola.toml: expected `rust_version = \"{msrv}\"`"
        ));
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

// ─────────────────────────────────────────────────────────────────────────────
// Settings must reach code
// ─────────────────────────────────────────────────────────────────────────────

/// Every `.rs` file in the crate, concatenated — the "does anything read it" corpus.
///
/// Deliberately includes the declaring modules and the tests. The signal this test uses is
/// **field access** (`.field`), which a declaration (`pub field: Type`) and a struct
/// literal (`field: value`) do not produce, so including a module cannot make its own
/// declarations look consumed.
///
/// It does mean a field read only by its own `validate()` counts as consumed. That is the
/// known weakness and it is worth accepting: all four defects this test exists to catch
/// (see below) had **no** reader at all, validate or otherwise, and excluding the config
/// module instead produced false positives on `SqlServerProfileConfig`, whose legitimate
/// consumer is the `to_runtime_config()` mapping that necessarily lives beside it.
fn consumer_corpus() -> Vec<(String, String)> {
    fn walk(dir: &Path, out: &mut Vec<(String, String)>) {
        let Ok(entries) = fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk(&path, out);
                continue;
            }
            if path.extension().is_some_and(|e| e == "rs") {
                if let Ok(src) = fs::read_to_string(&path) {
                    out.push((path.to_string_lossy().into_owned(), src));
                }
            }
        }
    }

    let mut corpus = Vec::new();
    for dir in ["src", "tests", "benches"] {
        walk(
            &Path::new(env!("CARGO_MANIFEST_DIR")).join(dir),
            &mut corpus,
        );
    }
    corpus
}

/// Public field names of **deserialized** structs in `src/config/**` and `src/cli.rs`.
///
/// "A configuration setting" means a field something outside this program can populate:
/// serde from a config file, or clap from the command line. The derive is what makes that
/// true, so the derive is the filter.
///
/// This used to collect every `pub <name>:` under `src/config/**`, which was blunt in a
/// way that eventually mattered: `KNOWN_SOURCE_DRIVERS`' `SourceDriverEntry` is a static
/// lookup table, not a setting, and its `aliases` field was reported as inert because the
/// only reader is the entry's own `matches()` — good encapsulation, punished. The fix is
/// not an allowlist entry (that list is for serde and macros, and diluting it is how it
/// stops meaning anything) but a scanner that knows what it is looking for.
///
/// The six defects this check was built to catch were all serde or clap fields, so nothing
/// is given up.
fn declared_settings() -> Vec<(String, String)> {
    /// Derives that mean "something outside this program fills this in".
    const SETTING_DERIVES: &[&str] = &["Deserialize", "Parser", "Args", "Subcommand"];

    let mut declared = Vec::new();

    let mut sources = glob_src("src/config");
    sources.push(("src/cli.rs".to_string(), read_src("src/cli.rs")));

    for (file, src) in sources {
        // Attributes can span lines (`#[derive(\n    Debug,\n    Deserialize,\n)]`), so
        // accumulate until the closing `)]` before deciding.
        let mut derive_buffer = String::new();
        let mut in_derive = false;
        let mut is_setting_struct = false;
        let mut depth = 0usize;

        for line in src.lines() {
            let trimmed = line.trim();

            if in_derive || trimmed.starts_with("#[derive(") {
                in_derive = true;
                derive_buffer.push_str(trimmed);
                if trimmed.contains(")]") {
                    in_derive = false;
                }
                continue;
            }

            if depth == 0 && (trimmed.starts_with("pub struct ") || trimmed.starts_with("struct "))
            {
                is_setting_struct = SETTING_DERIVES
                    .iter()
                    .any(|derive| derive_buffer.contains(derive));
                derive_buffer.clear();
                // A tuple struct or a `;`-terminated unit struct has no field block.
                if !trimmed.contains('{') {
                    is_setting_struct = false;
                    continue;
                }
                depth = 1;
                continue;
            }

            // Any other item resets the pending derive, so `#[derive(Deserialize)] enum`
            // followed by a plain struct does not leak the derive across.
            if depth == 0 && !trimmed.is_empty() && !trimmed.starts_with("//") {
                derive_buffer.clear();
            }

            if depth > 0 {
                depth += trimmed.matches('{').count();
                depth = depth.saturating_sub(trimmed.matches('}').count());
                if depth == 0 {
                    is_setting_struct = false;
                    continue;
                }
            }

            if !is_setting_struct {
                continue;
            }

            let Some(rest) = trimmed.strip_prefix("pub ") else {
                continue;
            };
            let Some((name, _)) = rest.split_once(':') else {
                continue;
            };
            let name = name.trim();
            if name.is_empty()
                || !name
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
            {
                continue;
            }
            declared.push((name.to_string(), file.clone()));
        }
    }

    declared.sort();
    declared.dedup();
    declared
}

/// A configuration setting that reaches no code is worse than a missing one.
///
/// **This repository has shipped four of them.** `admin.audit_signing_key_env` named the
/// variable to read while the loader read a hardcoded one, so operators who configured the
/// documented key got unsigned audit trails. `observability.otlp_protocol` was documented
/// as `grpc | http` while both exporters were built with a hardcoded `.with_tonic()`, so
/// `"http"` produced a gRPC exporter aimed at a collector's HTTP port — no telemetry, no
/// error. `state.backend.postgresql.schema_history_table` was defaulted to a distinct name
/// and never read, so both state artifacts shared the checkpoint's table. And the CLI's
/// `--admin-write-token[-env]` were parsed by clap and consumed by nothing.
///
/// All four parsed. All four validated. All four appeared in the reference documentation.
/// A config round-trip test cannot catch any of them — the value parses either way — and a
/// reviewer cannot either, because the declaration and the consumer are in different files.
///
/// So the check is mechanical: every `pub <field>:` declared under `src/config/**` or in
/// `src/cli.rs` must be read as `.field` somewhere that is not its own declaring module.
///
/// The allowlist below is the honest part. An entry there is a claim that the field is
/// consumed by something this scan cannot see — serde, or a macro — and each one names
/// which.
#[test]
fn every_configuration_setting_is_read_by_something() {
    /// Fields consumed by a mechanism a `.field` grep cannot observe.
    ///
    /// Keep this list short and justified. "It is used somewhere, trust me" is exactly the
    /// belief that let the four defects above ship.
    //
    // Every entry is verified to still be *needed*: see the staleness check below, which
    // deleted the three clap entries this list opened with (`command`, `admin_tls`,
    // `admin_auth`). All three had ordinary external readers — `cli.command` in
    // `main.rs`, `args.admin_tls` in `commands/status.rs` — and had simply never been
    // re-examined, because an allowlist that is only ever read past is never wrong.
    const CONSUMED_INDIRECTLY: &[(&str, &str)] = &[
        // ── SqlServerProfileConfig::to_runtime_config, src/config/source.rs ─────
        // This is the one config type this crate owns that mirrors a rustcdc struct
        // field-for-field, so its mapper necessarily lives beside the declaration.
        // These three joined the list when the replication-slot lag side-channel was
        // deleted (rustcdc 0.11 samples slot lag itself). That connection read
        // `pg.host/port/user/database/conn_timeout_secs`, which is what had been keeping
        // them externally referenced; `to_runtime_config` is now their only reader, like
        // every other field of this struct.
        ("conn_timeout_secs", "source.rs: to_runtime_config"),
        ("database", "source.rs: to_runtime_config"),
        ("port", "source.rs: to_runtime_config"),
        ("capture_truncate_events", "source.rs: to_runtime_config"),
        ("cdc_enabled", "source.rs: to_runtime_config"),
        ("cdc_schema", "source.rs: to_runtime_config"),
        ("instance_name", "source.rs: to_runtime_config"),
        ("max_events_per_poll", "source.rs: to_runtime_config"),
        ("prereq_pool_size", "source.rs: to_runtime_config"),
        ("stream_poll_interval_ms", "source.rs: to_runtime_config"),
        ("table_exclude_list", "source.rs: to_runtime_config"),
        ("table_include_list", "source.rs: to_runtime_config"),
        // ── krafka builder mapping, src/config/sink.rs ─────────────────────────
        (
            "client_secret",
            "sink.rs: to_auth_config / OidcTokenProvider builder",
        ),
        ("connections_max_idle_ms", "sink.rs: krafka client builder"),
        // Moved here from `KafkaSinkConfig` when krafka 0.18 deleted the producer's own
        // `max_in_flight` — it is a per-connection transport setting, so it maps in
        // `KafkaTransportConfig::to_krafka` alongside every other field of that struct.
        ("max_in_flight", "sink.rs: to_krafka (TransportConfig)"),
        ("form_parameters", "sink.rs: OidcTokenProvider builder"),
        ("max_connections", "sink.rs: krafka client builder"),
        ("mechanism", "sink.rs: to_auth_config"),
        ("ssl_certificate_location", "sink.rs: to_auth_config (mTLS)"),
        ("ssl_key_location", "sink.rs: to_auth_config (mTLS)"),
        ("tcp_keepalive_ms", "sink.rs: krafka client builder"),
        ("token_endpoint", "sink.rs: OidcTokenProvider builder"),
        // Read by `KafkaTransportConfig::to_krafka_proxy`, which `src/sink/kafka.rs` calls
        // on both producer builders. It is a separate accessor from `to_krafka` because
        // krafka carries the proxy on the client builder, not on `TransportConfig` — which
        // is exactly why this setting used to be validated and then dropped, and why this
        // entry has to name its consumer rather than simply silencing the check.
        (
            "socks5_proxy",
            "sink.rs: to_krafka_proxy, applied in sink/kafka.rs",
        ),
        // ── schemreg / validation-only, same file ──────────────────────────────
        (
            "allow_insecure",
            "registry.rs: guards the http:// URL check",
        ),
        (
            "password_env",
            "registry.rs: resolved into the registry client",
        ),
        (
            "token_env",
            "registry.rs: resolved into the registry client",
        ),
        (
            "username_env",
            "registry.rs: resolved into the registry client",
        ),
        (
            "durability_profile",
            "state.rs: cross-checks replication factor and ISR",
        ),
    ];

    let corpus = consumer_corpus();
    let declared = declared_settings();
    let mut inert = Vec::new();

    // An allowlist entry for a field that no longer exists, or that now has an ordinary
    // external reader, is a standing exemption nobody asked for. The next field to take
    // that name inherits it silently — which is precisely how a check like this stops
    // catching anything. Each entry must still be doing work.
    let mut stale: Vec<&str> = Vec::new();
    for (field, why) in CONSUMED_INDIRECTLY {
        let Some((_, file)) = declared.iter().find(|(name, _)| name == field) else {
            stale.push(field);
            continue;
        };
        let access = format!(".{field}");
        if corpus
            .iter()
            .any(|(path, src)| path != file && src.contains(&access))
        {
            let _ = why;
            stale.push(field);
        }
    }
    assert!(
        stale.is_empty(),
        "these CONSUMED_INDIRECTLY entries no longer earn their place — the field was \
         deleted, or it now has a plain external reader:\n  {}\n\nRemove them. A stale \
         exemption is inherited by the next field to take the name.",
        stale.join("\n  ")
    );

    for (field, file) in declared {
        if CONSUMED_INDIRECTLY.iter().any(|(name, _)| *name == field) {
            continue;
        }
        let access = format!(".{field}");
        let read_elsewhere = corpus
            .iter()
            .any(|(path, src)| *path != file && src.contains(&access));
        if read_elsewhere {
            continue;
        }
        inert.push(format!("{field} (declared in {file})"));
    }

    assert!(
        inert.is_empty(),
        "these configuration settings are declared, parse, and are read by nothing:\n  {}\n\n\
         A setting that reaches no code is accepted, documented and silently ignored — the \
         operator believes it applied. Either wire it to its consumer, delete it, or add it \
         to CONSUMED_INDIRECTLY with the mechanism that reads it.",
        inert.join("\n  ")
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// CI must actually run the integration suite
// ─────────────────────────────────────────────────────────────────────────────

/// **Every** integration suite must be run by CI, not just the first one written.
///
/// These suites skip unless `RUSTCDC_INTEGRATION=1`, so CI setting that variable *and*
/// naming the suite is the whole guarantee that any of them ever run. An env-gated test
/// nobody runs is strictly worse than no test: the suite is green, the coverage is zero,
/// and the absence of failures reads as health.
///
/// This used to assert only that `integration_postgres` was referenced, which is exactly
/// how the gap it was written to prevent reappeared: a review found three of four shipped
/// connectors had no end-to-end test at all, while this guard passed. A check that names
/// one file cannot notice a second file going unrun.
///
/// So it enumerates `tests/integration_*.rs` from disk and requires each to appear in the
/// workflow. Adding a suite and forgetting to wire it up now fails the unit suite.
#[test]
fn every_integration_suite_is_run_by_ci() {
    let workflow = read_src(".github/workflows/ci.yml");

    let tests_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests");
    let mut suites: Vec<String> = fs::read_dir(&tests_dir)
        .expect("tests/ must be readable")
        .flatten()
        .filter_map(|entry| {
            let name = entry.file_name().to_string_lossy().into_owned();
            name.strip_prefix("integration_")
                .and_then(|rest| rest.strip_suffix(".rs"))
                .map(|stem| format!("integration_{stem}"))
        })
        .collect();
    suites.sort();

    assert!(
        !suites.is_empty(),
        "no tests/integration_*.rs found; this guard's discovery has drifted from the tree"
    );

    let unreferenced: Vec<&String> = suites
        .iter()
        .filter(|suite| !workflow.contains(suite.as_str()))
        .collect();
    assert!(
        unreferenced.is_empty(),
        "these integration suites exist and CI never runs them: {unreferenced:?}\n\nThey are \
         env-gated, so an unreferenced suite is not merely unrun — it reports success. Add \
         a `--test <suite>` step to .github/workflows/ci.yml."
    );

    assert!(
        workflow.contains("RUSTCDC_INTEGRATION: \"1\""),
        "the integration job must set RUSTCDC_INTEGRATION=1; without it every test in every \
         suite returns immediately and the job passes having tested nothing"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Connectors are opt-in, and a narrow build must stay narrow
// ─────────────────────────────────────────────────────────────────────────────

/// A default build must link exactly one TLS stack.
///
/// The point of making connectors cargo features was that a PostgreSQL-only deployment
/// stopped shipping `tiberius`, and with it rustls 0.21, a second webpki, a second X.509
/// verifier, and four suppressed RUSTSEC advisories. That property is invisible in the
/// source: it lives in `Cargo.toml`'s feature graph, and one careless
/// `rustcdc = { features = ["sqlserver"] }` or a new dependency that happens to pull
/// tiberius puts it all back with nothing failing.
///
/// So it is asserted against the resolved graph. `cargo tree --edges normal` on the
/// default feature set must contain no rustls 0.21 and no tiberius.
///
/// This is deliberately a *graph* assertion rather than a `deny.toml` one. cargo-deny
/// reports the three webpki advisories as `advisory-not-detected` on a narrow build,
/// which is the same signal — but it is a warning, and CI audits `--all-features` where
/// they legitimately appear, so nothing would fail.
#[test]
fn the_default_build_has_one_tls_stack() {
    let output = std::process::Command::new(env!("CARGO"))
        .args(["tree", "--edges", "normal", "--prefix", "none"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .expect("cargo tree");
    assert!(
        output.status.success(),
        "cargo tree failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let tree = String::from_utf8_lossy(&output.stdout);

    for (crate_name, why) in [
        (
            "rustls v0.21",
            "a second TLS stack, carrying RUSTSEC-2026-0098/-0099/-0104 via rustls-webpki \
             0.101",
        ),
        (
            "rustls-webpki v0.101",
            "a second X.509 verifier with three open advisories",
        ),
        (
            "tiberius v0.12",
            "the SQL Server driver, which is what pins rustls 0.21",
        ),
    ] {
        assert!(
            !tree.contains(crate_name),
            "a default build must not link {crate_name} — it is {why}. Something re-enabled \
             the `sqlserver` feature, or a new dependency pulled it in transitively. Run \
             `cargo tree --edges normal -i {crate_name}` to find the edge."
        );
    }
}

/// Every `SourceDriver` variant must appear in `KNOWN_SOURCE_DRIVERS`.
///
/// The two lists serve different masters: the enum is what serde can parse in *this*
/// build, and the table is what the loader can produce an actionable "rebuild with
/// `--features x`" message for. A driver added to the enum but not the table falls
/// through to serde's `unknown variant` error in builds that lack it — which is the exact
/// unhelpful message the table exists to prevent, and it would only show up for operators
/// on a narrow build, never in CI.
#[test]
fn every_source_driver_is_known_to_the_gate() {
    let source = read_src("src/config/source.rs");

    // Variant names as serde spells them: `#[serde(rename_all = "snake_case")]`, so
    // `Sqlserver` is `sqlserver`. Read from the enum body rather than a hand-kept list.
    let enum_body = source
        .split_once("pub enum SourceDriver {")
        .expect("SourceDriver enum must exist")
        .1
        .split_once("\n}")
        .expect("SourceDriver enum must be closed")
        .0;

    // Variant lines look like `Postgres(PostgresSourceConfig),` — take the identifier
    // before the payload parenthesis, skipping doc comments and `#[cfg]`/`#[serde]`
    // attributes.
    let variants: Vec<String> = enum_body
        .lines()
        .map(str::trim)
        .filter(|line| !line.starts_with("//") && !line.starts_with('#'))
        .filter_map(|line| {
            let (name, _) = line.split_once('(')?;
            let name = name.trim();
            (!name.is_empty() && name.chars().all(|c| c.is_ascii_alphanumeric()))
                .then(|| name.to_ascii_lowercase())
        })
        .collect();

    assert!(
        !variants.is_empty(),
        "failed to parse any variant out of the SourceDriver enum; this test's parser has \
         drifted from the source"
    );

    for variant in &variants {
        assert!(
            source.contains(&format!("name: \"{variant}\"")),
            "SourceDriver::{variant} has no entry in KNOWN_SOURCE_DRIVERS. Without one, a \
             build that lacks its feature rejects the config with serde's `unknown \
             variant` error instead of naming the feature to rebuild with."
        );
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// The OpenAPI document must describe the router that exists
// ─────────────────────────────────────────────────────────────────────────────

/// Every route the admin router serves must appear in the OpenAPI document, and vice versa.
///
/// A specification nobody verifies is a specification that lies, and it lies in the
/// direction that costs most: a consumer generates a client, the client is missing an
/// endpoint that exists (or calls one that does not), and the mismatch surfaces at
/// integration time in someone else's codebase.
///
/// This is why the document is built in `src/admin/openapi.rs` rather than checked in as a
/// hand-written `openapi.yaml` — a file beside the source has exactly this drift problem
/// one commit later, with nothing to catch it.
///
/// Both directions are asserted. An undocumented route is the obvious failure; a documented
/// route that no longer exists is the quieter one, and it is what a spec accumulates as
/// endpoints are removed.
#[test]
fn every_admin_route_appears_in_the_openapi_document() {
    let admin = read_src("src/admin/mod.rs");

    // Routes as declared: `.route("/path", ...)`.
    let router_body = admin
        .split_once("pub fn router(state: AdminState) -> Router {")
        .expect("the admin router must exist")
        .1
        .split_once("\n}")
        .expect("the router must be brace-terminated")
        .0;

    let mut routed: Vec<String> = router_body
        .match_indices(".route(")
        .filter_map(|(index, _)| {
            let rest = &router_body[index..];
            let open = rest.find('"')?;
            let close = rest[open + 1..].find('"')?;
            Some(rest[open + 1..open + 1 + close].to_string())
        })
        .collect();
    routed.sort();
    routed.dedup();

    assert!(
        routed.len() >= 8,
        "failed to parse the router's paths; this test's parser has drifted: {routed:?}"
    );

    // Paths as documented: the `DOCUMENTED_PATHS` const, which the document's own unit
    // test already pins to the JSON it emits.
    let openapi = read_src("src/admin/openapi.rs");
    let const_body = openapi
        .split_once("pub(crate) const DOCUMENTED_PATHS: &[&str] = &[")
        .expect("DOCUMENTED_PATHS must exist")
        .1
        .split_once("];")
        .expect("DOCUMENTED_PATHS must be terminated")
        .0;

    let mut documented: Vec<String> = const_body
        .match_indices('"')
        .step_by(2)
        .filter_map(|(index, _)| {
            let rest = &const_body[index + 1..];
            let close = rest.find('"')?;
            Some(rest[..close].to_string())
        })
        .collect();
    documented.sort();
    documented.dedup();

    let undocumented: Vec<&String> = routed.iter().filter(|p| !documented.contains(p)).collect();
    assert!(
        undocumented.is_empty(),
        "these admin routes are served and undocumented: {undocumented:?}\n\nAdd them to \
         DOCUMENTED_PATHS and to the `paths` object in src/admin/openapi.rs. A consumer \
         generating a client from the document would not know they exist."
    );

    let phantom: Vec<&String> = documented.iter().filter(|p| !routed.contains(p)).collect();
    assert!(
        phantom.is_empty(),
        "these paths are documented and not served: {phantom:?}\n\nRemove them from \
         src/admin/openapi.rs. A generated client would call an endpoint that 404s."
    );
}
