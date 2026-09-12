/// Architecture invariant tests.
///
/// These source-level assertions enforce the structural boundaries this crate relies on,
/// so that breaking one is caught by the test suite rather than at code review — or not
/// at all.
///
/// Each test states the invariant and why it exists; a bare grep with no rationale is
/// indistinguishable from a lint nobody can safely delete.
use std::fs;
use std::path::{Path, PathBuf}; // used by glob_src / read_src

fn read_src(relative: &str) -> String {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join(relative);
    fs::read_to_string(&path).unwrap_or_else(|e| panic!("failed to read {relative}: {e}"))
}

/// Read a file relative to the **workspace root**, not this package.
///
/// The manifest, the Dockerfile, the documentation site, the README and the CI workflows
/// are repository-level artefacts and live one directory up. They used to sit beside this
/// crate, when the server was its own repository; the guards below check the same files
/// in their new home rather than being deleted for being inconvenient.
fn read_repo(relative: &str) -> String {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("the server package sits two levels below the workspace root")
        .join(relative);
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
/// Not a style preference about ideal file length — a ratchet against the specific failure
/// of a module doubling while everyone agrees it should shrink. Splitting is mechanical and
/// the compiler verifies it, so hitting this is a prompt to do fifteen minutes of work, not
/// to raise the number.
///
/// **The budget must trail the largest file, not lead it.** A guard calibrated above the
/// worst case in the tree never fires and does no work, so each split tightens both numbers
/// to just above what survives it.
///
/// Production modules get the tighter budget: length there costs review quality directly —
/// `admin/mod.rs` once carried a worker-lifecycle defect three thousand lines from the state
/// it mutated. Test files get the looser one; they are read a function at a time and each
/// states its own premise, so length is a navigation annoyance rather than a correctness
/// risk.
#[test]
fn no_source_file_grows_past_the_point_of_navigability() {
    /// Production modules: tight, because length here costs review quality.
    const MAX_LINES: usize = 3_600;
    /// Test modules: looser, and tracked separately — see the docs above.
    const MAX_TEST_LINES: usize = 3_850;

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
// Panicking constructs in production code
// ─────────────────────────────────────────────────────────────────────────────

/// Production code may panic only where a comment argues the case, and only on this list.
///
/// The project has claimed a budget of "at most two panicking constructs in production
/// code" since the first review, and nothing enforced it. An audit found **five**: the one
/// documented `unreachable!` in the Kafka sink, three `Mutex::lock().expect(..)` in the
/// signal ledger — where a single poisoning turned into a permanent outage of the whole
/// signal-ingress path — and a `serde_json::to_vec(..).expect(..)` on the disaster-recovery
/// path of `migrate-state`. A KPI with no test behind it is a wish.
///
/// Test modules are excluded: `expect` in a test *is* the assertion. The exclusion is by
/// `#[cfg(test)]` block and by `*_tests.rs` filename, matching how this crate splits them.
#[test]
fn production_code_panics_only_where_the_allowlist_says_it_may() {
    /// Each entry is `(file, snippet)`. A snippet must be specific enough that moving the
    /// construct somewhere else fails this test rather than silently passing.
    const ALLOWED: &[(&str, &str)] = &[
        // Guarded by an observation two lines above: the front of the window was just
        // matched as `Done`, so the arm cannot be reached.
        (
            "src/sink/kafka.rs",
            "unreachable!(\"front was just observed to be Done\")",
        ),
    ];

    let pattern = regex_lite_panics();
    let mut found: Vec<(String, usize, String)> = Vec::new();

    let mut stack = vec![Path::new(env!("CARGO_MANIFEST_DIR")).join("src")];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
                continue;
            }
            if path.extension().map(|e| e != "rs").unwrap_or(true) {
                continue;
            }
            let name = path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or_default();
            if name.contains("tests") {
                continue;
            }
            let Ok(src) = fs::read_to_string(&path) else {
                continue;
            };
            let relative = path
                .strip_prefix(env!("CARGO_MANIFEST_DIR"))
                .unwrap_or(&path)
                .display()
                .to_string();

            for (line_no, line) in production_lines(&src) {
                let trimmed = line.trim();
                // Doc comments and ordinary comments discuss these constructs constantly;
                // a mention is not an occurrence.
                if trimmed.starts_with("//") || trimmed.starts_with("*") {
                    continue;
                }
                if pattern.iter().any(|needle| line.contains(needle)) {
                    found.push((relative.clone(), line_no, trimmed.to_string()));
                }
            }
        }
    }

    let unexpected: Vec<_> = found
        .iter()
        .filter(|(file, _, line)| {
            !ALLOWED
                .iter()
                .any(|(allowed_file, snippet)| file == allowed_file && line.contains(snippet))
        })
        .collect();

    assert!(
        unexpected.is_empty(),
        "production code gained panicking constructs that are not on the allowlist in this \
         test. Either return a `Result`, recover (see `signal_ledger::lock_ledger` for the \
         poisoned-mutex pattern), or add an entry here with the argument for why it cannot \
         fire:\n{unexpected:#?}"
    );

    // The allowlist must not rot: an entry whose construct has been removed should be
    // deleted, not left as a licence for the next one.
    for (file, snippet) in ALLOWED {
        assert!(
            found
                .iter()
                .any(|(f, _, line)| f == file && line.contains(snippet)),
            "the allowlist still exempts `{snippet}` in {file}, but it is no longer there. \
             Remove the entry."
        );
    }
}

/// The panicking constructs this crate forbids outside tests.
fn regex_lite_panics() -> Vec<&'static str> {
    vec![
        ".unwrap()",
        ".expect(",
        "panic!(",
        "unreachable!(",
        "todo!(",
        "unimplemented!(",
    ]
}

/// Lines of `src` outside any `#[cfg(test)]` item, with 1-based line numbers.
///
/// Brace-counted rather than parsed: this crate writes `#[cfg(test)] mod tests {` at
/// column 0 with a matching `}` at column 0, and a full parse would be a dependency for
/// no extra confidence.
fn production_lines(src: &str) -> Vec<(usize, &str)> {
    let mut out = Vec::new();
    let mut skipping = false;
    let mut depth: i32 = 0;
    let mut armed = false;

    for (index, line) in src.lines().enumerate() {
        if !skipping && line.trim_start().starts_with("#[cfg(test)]") {
            armed = true;
            continue;
        }
        if armed {
            // The item's opening brace may be on the attribute's line or the next one.
            depth += line.matches('{').count() as i32 - line.matches('}').count() as i32;
            if depth > 0 {
                armed = false;
                skipping = true;
            }
            continue;
        }
        if skipping {
            depth += line.matches('{').count() as i32 - line.matches('}').count() as i32;
            if depth <= 0 {
                skipping = false;
                depth = 0;
            }
            continue;
        }
        out.push((index + 1, line));
    }
    out
}

// ─────────────────────────────────────────────────────────────────────────────
// `unsafe` in the library
// ─────────────────────────────────────────────────────────────────────────────

/// The library may use `unsafe` only where this list says it may.
///
/// `[workspace.lints.rust] unsafe_code = "forbid"` was declared for the whole life of this
/// project and applied to **nothing**: workspace lints only take effect for a package that
/// opts in with `[lints] workspace = true`, and this package never did. Underneath a README
/// line claiming `#![deny(unsafe_code)]` was "enforced workspace-wide" sat six `unsafe`
/// blocks and one gratuitous `unsafe fn` that wrapped a call to `kill(1)`.
///
/// The lint is real now, at `deny`, with exactly one exemption: `test_env::write_env`, the
/// single place the crate mutates process environment. `src/main.rs` carries `forbid`,
/// which cannot be overridden, so the **binary** genuinely contains none.
///
/// Checked in both directions, like the panic allowlist: an entry whose `unsafe` has been
/// removed also fails, so the list cannot rot into a standing licence.
fn uses_unsafe_keyword(code: &str) -> bool {
    let bytes = code.as_bytes();
    let mut from = 0usize;
    while let Some(offset) = code[from..].find("unsafe") {
        let start = from + offset;
        let end = start + "unsafe".len();
        let preceded = start
            .checked_sub(1)
            .and_then(|i| bytes.get(i))
            .is_some_and(|c| c.is_ascii_alphanumeric() || *c == b'_');
        let continues = bytes
            .get(end)
            .is_some_and(|c| c.is_ascii_alphanumeric() || *c == b'_');
        if !preceded && !continues {
            return true;
        }
        from = end;
    }
    false
}

#[test]
fn unsafe_code_appears_only_where_the_allowlist_says_it_may() {
    /// `(file, why)`. One entry. Adding a second needs an argument, in code review, here.
    const ALLOWED: &[(&str, &str)] = &[(
        "src/test_env.rs",
        "the one place the crate mutates process environment, serialised by EnvGuard",
    )];

    let mut found: Vec<String> = Vec::new();
    let mut stack = vec![Path::new(env!("CARGO_MANIFEST_DIR")).join("src")];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
                continue;
            }
            if path.extension().map(|e| e != "rs").unwrap_or(true) {
                continue;
            }
            let Ok(src) = fs::read_to_string(&path) else {
                continue;
            };
            // Comments discuss `unsafe` constantly in this crate, and the lint attributes
            // themselves are spelled `unsafe_code` — so match the *keyword*, with both
            // boundaries, over code only.
            if !uses_unsafe_keyword(&code_only(&src)) {
                continue;
            }
            found.push(
                path.strip_prefix(env!("CARGO_MANIFEST_DIR"))
                    .unwrap_or(&path)
                    .display()
                    .to_string(),
            );
        }
    }

    let unexpected: Vec<&String> = found
        .iter()
        .filter(|file| !ALLOWED.iter().any(|(allowed, _)| file.as_str() == *allowed))
        .collect();
    assert!(
        unexpected.is_empty(),
        "these files use `unsafe` and are not on the allowlist in this test. The library \
         denies `unsafe_code` and the binary forbids it; if a new site is genuinely \
         unavoidable, add it here with the argument:\n{unexpected:#?}"
    );

    for (file, why) in ALLOWED {
        assert!(
            found.iter().any(|f| f == file),
            "the allowlist still exempts {file} ({why}), but it no longer uses `unsafe`. \
             Remove the entry."
        );
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Secret-named settings must be typed as secrets
// ─────────────────────────────────────────────────────────────────────────────

/// A configuration field whose name says "secret" must be a `SecretString`.
///
/// `GET /status` and `GET /config` serialise the running configuration and hand it to any
/// read-scoped token. `SecretString`'s `Serialize` emits `[REDACTED]`, so a credential with
/// that type never reaches the wire at all — the name-based rules in `src/redaction.rs` are
/// a backstop for strings that arrive from elsewhere, not the primary defence.
///
/// Which makes the dangerous shape a secret-named field typed as a plain `String`: nothing
/// about it is redacted at the type level, and it survives only as long as the name rule
/// happens to match. That is the invariant here, and it is type-driven on purpose — a list
/// of field names only protects the names somebody remembered.
///
/// The allowlist is for names that *look* sensitive and are not. Each entry is a real
/// category, not an exception granted to a field.
#[test]
fn a_secret_named_setting_is_typed_as_a_secret() {
    /// `(field, why)`. `*_env` names are handled by rule below, not listed here.
    const NOT_ACTUALLY_SECRET: &[(&str, &str)] = &[(
        "token_endpoint",
        "an OAuth token endpoint is a URL; credentials inside it are stripped by the \
         value-driven URL rule",
    )];

    let mut plaintext: Vec<String> = Vec::new();

    for (file, src) in glob_src("src/config") {
        // Only serde-deserialised structs: a resolved runtime type like
        // `ResolvedRegistryAuth` legitimately holds plaintext and is never serialised into
        // the snapshot.
        let code = code_only(&src);
        let mut derive_buffer = String::new();
        let mut in_derive = false;
        let mut is_setting_struct = false;

        for (line_no, line) in code.lines().enumerate() {
            let trimmed = line.trim();

            if in_derive || trimmed.starts_with("#[derive(") {
                in_derive = true;
                derive_buffer.push_str(trimmed);
                if trimmed.contains(")]") {
                    in_derive = false;
                }
                continue;
            }
            if trimmed.starts_with("pub struct ")
                || trimmed.starts_with("struct ")
                || trimmed.starts_with("pub enum ")
                || trimmed.starts_with("enum ")
            {
                is_setting_struct = derive_buffer.contains("Deserialize");
                derive_buffer.clear();
                continue;
            }
            if !is_setting_struct {
                continue;
            }

            let Some((name, ty)) = trimmed.trim_start_matches("pub ").split_once(':') else {
                continue;
            };
            let name = name.trim();
            let ty = ty.trim().trim_end_matches(',');
            if name.is_empty() || !name.bytes().all(|b| b.is_ascii_lowercase() || b == b'_') {
                continue;
            }
            if ty != "String" && ty != "Option<String>" {
                continue;
            }
            if !rustcdc_server::redaction::is_sensitive_key_name(name)
                || NOT_ACTUALLY_SECRET
                    .iter()
                    .any(|(allowed, _)| name == *allowed)
            {
                continue;
            }
            plaintext.push(format!(
                "{}:{} — `{name}: {ty}`",
                Path::new(&file)
                    .strip_prefix(env!("CARGO_MANIFEST_DIR"))
                    .unwrap_or(Path::new(&file))
                    .display(),
                line_no + 1
            ));
        }
    }

    assert!(
        plaintext.is_empty(),
        "these settings are named like secrets but typed as plain strings, so nothing \
         redacts them at the type level and `GET /config` protects them only for as long as \
         a substring rule happens to match:\n  {}\n\nUse `SecretString`, or — if the field \
         genuinely is not a secret — add it to `NOT_ACTUALLY_SECRET` with the category.",
        plaintext.join("\n  ")
    );
}

/// The redactor must not blank fields an operator needs in order to *find* a credential.
///
/// Over-redaction is a real cost. `GET /config` is where someone answers "which variable
/// holds my token?"; blanking that pushes them to read the config file off the disk, which
/// is worse for security rather than better.
///
/// The treatment used to be arbitrary rather than chosen — `read_token_env` was redacted and
/// `audit_signing_key_env` was not, the difference being only which substrings happened to
/// be in the token list.
#[test]
fn env_variable_names_and_endpoints_are_not_treated_as_secrets() {
    use rustcdc_server::redaction::is_sensitive_key_name;

    for name in [
        "read_token_env",
        "write_token_env",
        "password_env",
        "token_env",
        "audit_signing_key_env",
        "token_endpoint",
    ] {
        assert!(
            !is_sensitive_key_name(name),
            "`{name}` names a variable or an endpoint, not a secret; redacting it hides the \
             one thing an operator needs to locate the credential"
        );
    }

    // …and the actual secrets must still be caught, or this test is a licence.
    for name in [
        "password",
        "passphrase",
        "client_secret",
        "bearer_token",
        "private_key",
        "sasl_password",
        "x-api-key",
        "access_key_id",
    ] {
        assert!(is_sensitive_key_name(name), "`{name}` must be redacted");
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tool-generated markers
// ─────────────────────────────────────────────────────────────────────────────

/// `cargo fix` leaves `FIXME:` comments behind, and they outlive the thing they asked about.
///
/// The edition-2024 migration inserted an "audit that the environment access only happens
/// in single-threaded code" note above every `env::set_var`. Those calls were then routed
/// through `test_env::EnvGuard`, which *is* that audit — a process-wide lock and restoration
/// on drop — but two of the comments survived the refactor and read as open questions about
/// code that had already been answered.
///
/// A marker a tool wrote and nobody re-read is worse than no marker: it makes the file look
/// unfinished in a place that is finished, and it trains readers to skip the ones that
/// matter.
#[test]
fn no_tool_generated_fixme_survives_in_the_tree() {
    let mut found: Vec<String> = Vec::new();
    let mut stack = vec![
        Path::new(env!("CARGO_MANIFEST_DIR")).join("src"),
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests"),
        Path::new(env!("CARGO_MANIFEST_DIR")).join("benches"),
    ];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
                continue;
            }
            if path.extension().map(|e| e != "rs").unwrap_or(true) {
                continue;
            }
            let Ok(src) = fs::read_to_string(&path) else {
                continue;
            };
            for (index, line) in src.lines().enumerate() {
                // Assembled with `concat!` so this assertion does not match its own
                // source — the same trick `no_module_spawns_a_detached_thread_…` needs. A
                // hand-written `FIXME` is a deliberate note and is left alone.
                if line.contains(concat!("FIXME", ": Audit that the")) {
                    found.push(format!(
                        "{}:{}",
                        path.strip_prefix(env!("CARGO_MANIFEST_DIR"))
                            .unwrap_or(&path)
                            .display(),
                        index + 1
                    ));
                }
            }
        }
    }

    assert!(
        found.is_empty(),
        "`cargo fix` left these markers behind. Resolve the question and delete the \
         comment — env mutation belongs in `test_env::EnvGuard`, which is the audit they \
         ask for:\n  {}",
        found.join("\n  ")
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Minimum supported Rust version
// ─────────────────────────────────────────────────────────────────────────────

/// Every place that restates the release version must agree with `Cargo.toml`.
///
/// `site/config.toml` carries `version`, shown in the site header and in the page's
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
    let version = workspace_package_key("version");

    let site_config = read_repo("site/config.toml");
    assert!(
        site_config.contains(&format!(r#"version = "{version}""#)),
        "site/config.toml: expected `version = \"{version}\"` to match Cargo.toml. It is \
         rendered in the site header and structured data, so a stale value misreports which \
         release the documentation describes."
    );
}

/// Read a string key from the workspace manifest's `[workspace.package]` table.
///
/// Scoped to that table on purpose. Both members now inherit with
/// `rust-version.workspace = true`, and a scan of the whole file matches that line first
/// — `strip_prefix("rust-version")` succeeds on it, `find_map` short-circuits, and the
/// value comes back empty. Anchoring to the table is also what makes the answer
/// unambiguous: `xtask` declares an MSRV of its own, and it is not the one this
/// repository promises.
fn workspace_package_key(key: &str) -> String {
    let manifest = read_repo("Cargo.toml");
    let table = manifest
        .split("[workspace.package]")
        .nth(1)
        .expect("Cargo.toml must declare a [workspace.package] table");
    table
        .lines()
        // Stop at the next table header, or a key from a later table would match.
        .take_while(|line| !line.trim_start().starts_with('['))
        .find_map(|line| line.trim().strip_prefix(key))
        .and_then(|rest| rest.trim_start().strip_prefix('='))
        .and_then(|rest| rest.split('"').nth(1).map(str::to_owned))
        .unwrap_or_else(|| panic!("[workspace.package] must declare {key}"))
}

/// Every `.rs` file in a crate's `src/` must be reachable from its module tree.
///
/// A file nobody declares is never compiled: its tests never run, it never type-checks
/// against the APIs it calls, and it rots against dependency upgrades without a single
/// warning. It reads as a feature to anyone browsing the tree and is not one.
///
/// This caught a 399-line `core/column_filter.rs` carrying 13 tests that had never
/// executed and code that no longer compiled against `sha2` 0.11 — while duplicating
/// masking the transform pipeline already provides and tests.
///
/// `#[path = "..."]` includes count as declarations: `config/loader_tests.rs` is reached
/// that way and is genuinely live.
#[test]
fn every_source_file_is_reachable_from_its_module_tree() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("workspace root")
        .to_path_buf();

    let mut orphans = Vec::new();

    for crate_dir in [
        "crates/rustcdc",
        "crates/rustcdc-server",
        "crates/xtask",
        "crates/crash-workers",
    ] {
        let src = root.join(crate_dir).join("src");
        let mut files = Vec::new();
        collect_rs_files(&src, &mut files);

        let declared: String = files
            .iter()
            .filter_map(|path| fs::read_to_string(path).ok())
            .collect::<Vec<_>>()
            .join("\n");

        for path in &files {
            let stem = path
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or_default()
                .to_owned();
            // Crate roots and module roots declare themselves; `src/bin/*` are targets.
            if matches!(stem.as_str(), "lib" | "main" | "mod")
                || path.components().any(|c| c.as_os_str() == "bin")
            {
                continue;
            }
            let declared_as_mod = declared.contains(&format!("mod {stem};"))
                || declared.contains(&format!("mod {stem} "))
                || declared.contains(&format!("mod {stem}{{"))
                || declared.contains(&format!("mod {stem} {{"));
            let included_by_path = declared.contains(&format!("path = \"{stem}.rs\""));
            if !declared_as_mod && !included_by_path {
                orphans.push(path.display().to_string());
            }
        }
    }

    assert!(
        orphans.is_empty(),
        "these source files are declared by nothing, so they are never compiled and their \
         tests never run:\n  {}\n\nDeclare them or delete them — a file that does not \
         compile is not a feature.",
        orphans.join("\n  ")
    );
}

fn collect_rs_files(dir: &Path, into: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_rs_files(&path, into);
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            into.push(path);
        }
    }
}

/// Extract the MSRV from the workspace manifest.
fn declared_msrv() -> String {
    workspace_package_key("rust-version")
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

    let dockerfile = read_repo("Dockerfile");
    if !dockerfile.contains(&format!("ARG RUST_VERSION={msrv}")) {
        wrong.push(format!("Dockerfile: expected `ARG RUST_VERSION={msrv}`"));
    }

    let site_config = read_repo("site/config.toml");
    if !site_config.contains(&format!(r#"rust_version = "{msrv}""#)) {
        wrong.push(format!(
            "site/config.toml: expected `rust_version = \"{msrv}\"`"
        ));
    }

    // Both READMEs: the repository landing page and the published crate's own, which is
    // what crates.io and docs.rs render. They are different documents for different
    // audiences and both state the toolchain, so both can drift.
    for readme in ["README.md", "crates/rustcdc/README.md"] {
        if !read_repo(readme).contains(&format!("Rust {msrv}+")) {
            wrong.push(format!("{readme}: badge/text should say `Rust {msrv}+`"));
        }
    }

    // The getting-started page states the number in prose. It used to interpolate a Zola
    // shortcode instead, which Zola 0.23 removed along with every other shortcode — so the
    // literal is now the only option. That is not a downgrade: an indirection only moved
    // where the number was written, whereas this check is what actually prevents drift.
    let getting_started = read_repo("site/content/docs/getting-started.md");
    if !getting_started.contains(&format!("Requires Rust {msrv} or later")) {
        wrong.push(format!(
            "site/content/docs/getting-started.md: expected `Requires Rust {msrv} or later`"
        ));
    }

    // The CI job must *derive* the toolchain rather than restate it, or this test would
    // have to be updated in lockstep with a value it is supposed to be policing.
    let ci = read_repo(".github/workflows/ci.yml");
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
            if path.extension().is_some_and(|e| e == "rs")
                && let Ok(src) = fs::read_to_string(&path)
            {
                out.push((path.to_string_lossy().into_owned(), code_only(&src)));
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

/// `src` with comments, string literals and char literals blanked out.
///
/// The scanner decides "this setting is read" by looking for `.<field>` in the corpus, and
/// without this it counted **prose**. Three of this crate's own doc comments mention
/// `.allow_insecure` and `.token_endpoint` while explaining the scanner; every validation
/// error message that names a TOML path (`"sink.iceberg.catalog.rest.uri"`) contains
/// `.uri`; every doc example that shows a field access contains that access. All of them
/// marked settings live that no code reads — which is a false *pass* in a check whose
/// entire job is to find settings nothing reads.
///
/// Rust never needs a string or a comment to read a field, so blanking both loses no true
/// reader. Byte-for-byte replacement keeps offsets, which keeps any future line reporting
/// honest.
fn code_only(src: &str) -> String {
    #[derive(Clone, Copy, PartialEq)]
    enum State {
        Code,
        LineComment,
        BlockComment(u32),
        Str,
        RawStr(usize),
        Char,
    }

    let bytes = src.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut state = State::Code;
    let mut index = 0usize;

    while index < bytes.len() {
        let byte = bytes[index];
        let next = bytes.get(index + 1).copied();

        match state {
            State::Code => {
                // Raw string: `r"`, `r#"`, `r##"` …
                if byte == b'r' {
                    let mut hashes = 0usize;
                    let mut probe = index + 1;
                    while bytes.get(probe) == Some(&b'#') {
                        hashes += 1;
                        probe += 1;
                    }
                    if bytes.get(probe) == Some(&b'"') {
                        out.extend(std::iter::repeat_n(b' ', probe - index + 1));
                        index = probe + 1;
                        state = State::RawStr(hashes);
                        continue;
                    }
                }
                if byte == b'/' && next == Some(b'/') {
                    state = State::LineComment;
                    out.push(b' ');
                } else if byte == b'/' && next == Some(b'*') {
                    state = State::BlockComment(1);
                    out.push(b' ');
                } else if byte == b'"' {
                    state = State::Str;
                    out.push(b' ');
                } else if byte == b'\'' {
                    // A lifetime (`&'a str`) is not a char literal. Char literals are at
                    // most a few bytes and always closed by a quote.
                    let closes = (1..=4).any(|n| bytes.get(index + n) == Some(&b'\''));
                    if closes {
                        state = State::Char;
                    }
                    out.push(b' ');
                } else {
                    out.push(byte);
                }
                index += 1;
            }
            State::LineComment => {
                if byte == b'\n' {
                    state = State::Code;
                    out.push(b'\n');
                } else {
                    out.push(b' ');
                }
                index += 1;
            }
            State::BlockComment(depth) => {
                if byte == b'/' && next == Some(b'*') {
                    state = State::BlockComment(depth + 1);
                    out.extend_from_slice(b"  ");
                    index += 2;
                } else if byte == b'*' && next == Some(b'/') {
                    state = if depth == 1 {
                        State::Code
                    } else {
                        State::BlockComment(depth - 1)
                    };
                    out.extend_from_slice(b"  ");
                    index += 2;
                } else {
                    out.push(if byte == b'\n' { b'\n' } else { b' ' });
                    index += 1;
                }
            }
            State::Str => {
                if byte == b'\\' {
                    out.extend_from_slice(b"  ");
                    index += 2;
                    continue;
                }
                if byte == b'"' {
                    state = State::Code;
                }
                out.push(if byte == b'\n' { b'\n' } else { b' ' });
                index += 1;
            }
            State::RawStr(hashes) => {
                if byte == b'"' && (0..hashes).all(|n| bytes.get(index + 1 + n) == Some(&b'#')) {
                    out.extend(std::iter::repeat_n(b' ', hashes + 1));
                    index += hashes + 1;
                    state = State::Code;
                    continue;
                }
                out.push(if byte == b'\n' { b'\n' } else { b' ' });
                index += 1;
            }
            State::Char => {
                if byte == b'\\' {
                    out.extend_from_slice(b"  ");
                    index += 2;
                    continue;
                }
                if byte == b'\'' {
                    state = State::Code;
                }
                out.push(b' ');
                index += 1;
            }
        }
    }

    String::from_utf8(out).unwrap_or_else(|_| src.to_string())
}

/// Does `src` read `.<field>` as a whole field access?
///
/// A bare `src.contains(".token")` — the previous test — also matches `.token_endpoint`,
/// `.tokens` and `.token_sha256_hex`. That direction of error is the dangerous one for an
/// *inert-settings* scanner: an unrelated longer field name marks a genuinely dead setting
/// as live, and the check reports success while missing exactly what it exists to find.
///
/// The trailing character must not continue the identifier, **and must not be `(`**: `.port(`
/// is `SocketAddr::port()`, not a configuration field. Counting method calls seemed harmless
/// until `tests/snowflake_contract.rs` called `addr.port()` and made a load-bearing SQL
/// Server exemption look stale. A field access is never a call.
fn reads_field(src: &str, field: &str) -> bool {
    let needle = format!(".{field}");
    let bytes = src.as_bytes();
    let mut from = 0usize;
    while let Some(offset) = src[from..].find(&needle) {
        let start = from + offset;
        let end = start + needle.len();
        let continues = bytes
            .get(end)
            .is_some_and(|c| c.is_ascii_alphanumeric() || *c == b'_' || *c == b'(');
        if !continues {
            return true;
        }
        from = end;
    }
    false
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

            // Any other *item* resets the pending derive, so `#[derive(Deserialize)] enum`
            // followed by a plain struct does not leak the derive across.
            //
            // Attributes are **not** items. This used to clear on any non-empty,
            // non-comment line, which meant a `#[serde(deny_unknown_fields)]` between the
            // derive and the struct erased the derive — and the whole struct became
            // invisible to this scanner. Every setting it declared was then exempt from the
            // inert check by accident, in the guard the project relies on most.
            if depth == 0
                && !trimmed.is_empty()
                && !trimmed.starts_with("//")
                && !trimmed.starts_with("#[")
            {
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
        // `to_runtime_config` is their only reader, like every other field of this
        // struct — the replication-slot lag side-channel that used to read
        // `pg.host/port/user/database/conn_timeout_secs` is gone, since rustcdc samples
        // slot lag itself.
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
        // A per-connection transport setting, not a producer one, so it maps in
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
        // ── same-file consumers found once the scanner stopped reading prose ───
        //
        // These three were never inert. They were *masked*: the corpus included comments
        // and string literals, so an unrelated doc comment or a validation message that
        // happened to contain `.scope`, `.extensions` or `.user` counted as an external
        // reader and the check passed for the wrong reason. With `code_only` in place they
        // surface correctly as same-file consumption, which is what these entries record.
        ("scope", "sink.rs: OidcTokenProvider::builder().scope()"),
        (
            "extensions",
            "sink.rs: OidcTokenProvider sasl_extension + the static-token arm",
        ),
        (
            "user",
            "source.rs: SqlServerProfileConfig::to_runtime_config",
        ),
        // Read through `IcebergCatalogConfig::location()`, which the sink calls to infer
        // the storage backend. It stopped having an external reader when the catalog became
        // an enum over REST and S3 Tables and the two locations were unified behind one
        // accessor — the scanner caught that on the same commit, which is the point of it.
        (
            "warehouse",
            "sink.rs: IcebergCatalogConfig::location, used by sink/iceberg.rs",
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
        let declaring: Vec<&String> = declared
            .iter()
            .filter(|(name, _)| name == field)
            .map(|(_, file)| file)
            .collect();

        if declaring.is_empty() {
            stale.push(field);
            continue;
        }

        // Two structs declaring the same field name make the "has an external reader"
        // half of this check undecidable: the scanner matches by name, so a reader of
        // *either* field satisfies it. Adding a `SnowflakeSinkConfig` with `user` and
        // `database` made three SQL Server exemptions look stale overnight, and deleting
        // load-bearing exemptions on the strength of an ambiguity is worse than keeping a
        // possibly-redundant one. The existence half still applies.
        if declaring.len() > 1 {
            continue;
        }

        let file = declaring[0];
        if corpus
            .iter()
            .any(|(path, src)| path != file && reads_field(src, field))
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
        let read_elsewhere = corpus
            .iter()
            .any(|(path, src)| *path != file && reads_field(src, &field));
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
    let workflow = read_repo(".github/workflows/ci.yml");

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

    // `integration_kafka` is parameterised over the broker, and naming the suite once is
    // not enough: it would run whichever broker the default happens to be and report
    // coverage of both. Redpanda is an independent reimplementation of the wire protocol,
    // and the two already disagreed on metadata propagation and coordinator election the
    // first time this matrix ran.
    if suites.iter().any(|suite| suite == "integration_kafka") {
        for broker in ["redpanda", "kafka"] {
            assert!(
                workflow.contains(&format!("broker: {broker}")),
                "the CI matrix must run integration_kafka against `{broker}`. Running one \
                 broker and claiming both is the coverage-overstatement this file exists \
                 to prevent."
            );
        }
    }
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
