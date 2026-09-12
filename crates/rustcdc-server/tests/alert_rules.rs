//! Every metric an alert rule references must be one the server actually emits.
//!
//! `promtool check rules` validates PromQL *syntax*. It has no idea which metrics exist,
//! so a rule keying off a name the server never exports passes every gate and then
//! sits silent forever. That is strictly worse than having no rule: the dashboard shows
//! a configured alert, nobody is paged, and the absence of alerts reads as health.
//!
//! When this test was written, **18 of the 29** `rustcdc_*` metrics referenced by
//! `monitoring/rustcdc_slo_alerts.yml` did not exist anywhere in the source. Among them
//! were the checkpoint-age durability alert, the delivery-latency SLO, the revoked-token
//! security alert, and both reconciliation-recovery alerts — the last of which are the
//! ones that would report the `effectively_once` crash window the docs promise is
//! "detected and reported".

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

/// The **workspace** root, which is where `monitoring/` lives.
///
/// One level up from this package: the alert rules describe the deployed server but are a
/// repository-level artefact, like the Dockerfile and the documentation site.
fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("the server package sits two levels below the workspace root")
        .to_path_buf()
}

/// The server crate's own source tree, which is what emits the metric names.
fn server_src() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src")
}

/// Every `rustcdc_*` identifier appearing in the alert rules, with Prometheus'
/// histogram/summary suffixes stripped back to the family name.
fn metrics_referenced_by_alerts(rules: &str) -> BTreeSet<String> {
    let mut found = BTreeSet::new();
    let bytes = rules.as_bytes();
    let mut index = 0;

    while let Some(offset) = rules[index..].find("rustcdc_") {
        let start = index + offset;
        let mut end = start;
        while end < bytes.len() && (bytes[end].is_ascii_alphanumeric() || bytes[end] == b'_') {
            end += 1;
        }
        found.insert(rules[start..end].to_string());
        index = end;
    }

    found
}

/// Every `rustcdc_*` identifier that appears anywhere in the source tree.
///
/// This is a name-existence check rather than a render-and-scrape check: metric names
/// are emitted from several unrelated code paths (the encoder, `push_str` blocks in the
/// admin module, the SLO block), and no single fixture renders all of them. A typo or a
/// rename that leaves the rules behind changes the *name*, which is exactly what this
/// catches.
fn metric_names_in_source(dir: &Path, into: &mut BTreeSet<String>) {
    let entries = std::fs::read_dir(dir).expect("read source dir");
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            metric_names_in_source(&path, into);
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            let source = std::fs::read_to_string(&path).expect("read source file");
            into.extend(metrics_referenced_by_alerts(&emitting_code(&source)));
        }
    }
}

/// The part of a source file that can actually emit a metric.
///
/// Comments and inline test modules are cut out, and both exclusions are load-bearing
/// rather than tidiness. A metric name is "emitted" only if production code writes it; a
/// name in a comment or in an assertion is a name nobody scrapes.
///
/// This was not always so, and the consequence was a test that could not fail: the
/// regression assertion pinning the *removal* of a divergent metric spelling put that
/// spelling back into the scanned corpus, so an alert rule referencing the dead name
/// resolved against the assertion forbidding it. The check passed by reading its own
/// tombstone.
///
/// The cut is at an inline `#[cfg(test)] mod … {` — a module with a body — and not at a
/// bare `#[cfg(test)] mod tests;` *declaration*, which several modules carry near the top
/// of the file. Cutting at the declaration discards the entire production body below it,
/// which silently under-reports what the server emits and turns this check into the
/// opposite of what it is for.
fn emitting_code(source: &str) -> String {
    let mut out = Vec::new();
    let mut lines = source.lines().peekable();

    while let Some(line) = lines.next() {
        let trimmed = line.trim_start();
        if trimmed.starts_with("//") || trimmed.starts_with('*') {
            continue;
        }
        if trimmed.starts_with("#[cfg(test)]") {
            // Cut only at an inline test *module* — `mod something {`. Not at a bare
            // `mod tests;` declaration, and not at a `#[cfg(test)] fn` helper: both of
            // those sit above production code, and cutting there discards the rest of the
            // file. `admin/mod.rs` has three such helpers before line 2 200, so the
            // sloppier rule hid every metric the admin surface emits.
            let is_inline_module = lines
                .peek()
                .map(|next| {
                    let next = next.trim();
                    next.starts_with("mod ") && next.ends_with('{')
                })
                .unwrap_or(false);
            if is_inline_module {
                break;
            }
            continue;
        }
        out.push(line);
    }

    out.join("\n")
}

#[test]
fn the_scan_keeps_production_code_below_a_test_module_declaration() {
    for above in ["mod tests;", "fn only_used_by_tests() {"] {
        let source =
            format!("#[cfg(test)]\n{above}\n\nfn emit() {{ push(\"rustcdc_kept_total\"); }}\n");
        assert!(
            emitting_code(&source).contains("rustcdc_kept_total"),
            "`#[cfg(test)] {above}` must not truncate the production code below it"
        );
    }
}

#[test]
fn the_scan_drops_inline_test_modules_and_comments() {
    let source = "fn emit() { push(\"rustcdc_kept_total\"); }\n                  // rustcdc_commented_total\n                  #[cfg(test)]\nmod tests {\n  assert!(out.contains(\"rustcdc_asserted_total\"));\n}\n";
    let scanned = emitting_code(source);
    assert!(scanned.contains("rustcdc_kept_total"));
    assert!(
        !scanned.contains("rustcdc_commented_total"),
        "a comment is not an emission"
    );
    assert!(
        !scanned.contains("rustcdc_asserted_total"),
        "an assertion is not an emission"
    );
}

/// A referenced metric matches if the source contains the family name or any name that
/// extends it — `foo_seconds` covers `foo_seconds_bucket`, and a rule may equally well
/// reference the fully-suffixed form.
fn is_emitted(referenced: &str, emitted: &BTreeSet<String>) -> bool {
    if emitted.contains(referenced) {
        return true;
    }
    for suffix in ["_bucket", "_sum", "_count", "_total"] {
        if let Some(base) = referenced.strip_suffix(suffix)
            && emitted.contains(base)
        {
            return true;
        }
    }
    emitted
        .iter()
        .any(|candidate| candidate.starts_with(referenced))
}

/// The runtime families the library's renderer actually emits.
///
/// Rendered rather than grepped. Scanning a source tree for a metric name is a weaker
/// question than it looks: it passes when the name appears *anywhere* — in a comment, in
/// dead code, or in a second renderer nothing calls. That is exactly how the replication
/// slot lag came to have two spellings, `rustcdc_replication_slot_lag_bytes` here and
/// `rustcdc_runtime_replication_slot_lag_bytes` in a duplicate renderer this crate used
/// to carry, with the shipped rules referencing both — so one of the two could never
/// fire, on the signal whose unbounded growth fills a PostgreSQL primary's WAL volume.
///
/// The fixture populates every optional field and uses a stalled verdict, so the
/// conditional families are emitted too.
fn rendered_runtime_families() -> BTreeSet<String> {
    let snapshot: rustcdc::core::RuntimeAdminSnapshot = serde_json::from_value(serde_json::json!({
        "source_type": "postgres",
        "state": "running",
        "readiness": true,
        "liveness": true,
        "capabilities": {
            "snapshot": true, "snapshot_checkpoint_resume": true, "handoff": true,
            "ddl_capture": true, "heartbeat": true, "tls": true,
            "schema_introspection": true, "truncate": true, "incremental_snapshot": true
        },
        "buffer_depth": 1,
        "in_flight_events": 1,
        "snapshot_active": false,
        "stream_active": true,
        "handoff_complete": true,
        "total_events_polled": 10,
        "total_events_committed": 7,
        "total_events_deduplicated": 1,
        "total_events_skipped": 1,
        "idempotency_evictions": 1,
        "idempotency_unidentifiable_passthrough": 1,
        "unmatched_transform_rules": [
            {
                "transform": "redact_pii", "kind": "mask", "rule": "email",
                "consequence": "the column ships in clear text"
            }
        ],
        "health": {
            "status": "stalled",
            "cause": "poll_loop_not_turning",
            "reason": "no poll has returned for 120000ms"
        },
        "started_at_ms": 1,
        "last_poll_at_ms": 2,
        "last_delivery_at_ms": 2,
        "last_commit_at_ms": 3,
        "checkpoint_age_ms": 5,
        "replication_lag_ms": 7,
        "replication_slot_lag_bytes": 42
    }))
    .expect("snapshot deserializes");

    let mut rendered = Vec::new();
    rustcdc::core::write_runtime_metrics_prometheus(&snapshot, &mut rendered)
        .expect("writing to a Vec is infallible");
    let rendered = String::from_utf8(rendered).expect("valid UTF-8");

    let families = metrics_referenced_by_alerts(&rendered);
    assert!(
        families.len() > 10,
        "the renderer emitted only {} families — the fixture is not fully populated, \
         which would make every check against it vacuously weak",
        families.len()
    );
    families
}

#[test]
fn every_alert_rule_references_a_metric_the_server_emits() {
    let root = repo_root();
    let rules = std::fs::read_to_string(root.join("monitoring/rustcdc_slo_alerts.yml"))
        .expect("read the alert rules");

    let referenced = metrics_referenced_by_alerts(&rules);
    assert!(
        referenced.len() > 20,
        "the extractor found only {} metrics — it is probably broken, which would make \
         this test vacuously green",
        referenced.len()
    );

    // The runtime families come from the library's renderer and are taken from its
    // **output**; everything else — sink, DLQ, admin, signal — is rendered by this crate
    // from several unrelated code paths that no single fixture drives, so those are still
    // name-scanned. The library's source tree is deliberately *not* scanned: a name that
    // appears there but is never rendered is precisely the failure this test exists for.
    let mut emitted = rendered_runtime_families();
    metric_names_in_source(&server_src(), &mut emitted);

    let dangling: Vec<&String> = referenced
        .iter()
        .filter(|name| !is_emitted(name, &emitted))
        .collect();

    assert!(
        dangling.is_empty(),
        "these alert rules reference metrics the server never emits, so they can never \
         fire:\n  {}\n\nEither point the rule at the real metric name or export the \
         signal. A rule that cannot fire is worse than no rule — it looks like coverage.",
        dangling
            .iter()
            .map(|name| name.as_str())
            .collect::<Vec<_>>()
            .join("\n  ")
    );
}

/// Latency and duration metrics must be denominated in seconds.
///
/// Prometheus metric families use base units, and `histogram_quantile` inherits the
/// bound's unit — a `_ms` family silently produces millisecond quantiles that every
/// dashboard convention will read as seconds.
#[test]
fn no_alert_rule_depends_on_a_millisecond_denominated_latency_metric() {
    let rules = std::fs::read_to_string(repo_root().join("monitoring/rustcdc_slo_alerts.yml"))
        .expect("read the alert rules");

    let offenders: Vec<String> = metrics_referenced_by_alerts(&rules)
        .into_iter()
        .filter(|name| {
            (name.contains("latency") || name.contains("duration") || name.contains("lag"))
                && (name.contains("_ms") || name.contains("_us"))
        })
        .collect();

    assert!(
        offenders.is_empty(),
        "these alert rules key off millisecond- or microsecond-denominated metrics; \
         export the family in seconds instead:\n  {}",
        offenders.join("\n  ")
    );
}
