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

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
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
            into.extend(metrics_referenced_by_alerts(&source));
        }
    }
}

/// A referenced metric matches if the source contains the family name or any name that
/// extends it — `foo_seconds` covers `foo_seconds_bucket`, and a rule may equally well
/// reference the fully-suffixed form.
fn is_emitted(referenced: &str, emitted: &BTreeSet<String>) -> bool {
    if emitted.contains(referenced) {
        return true;
    }
    for suffix in ["_bucket", "_sum", "_count", "_total"] {
        if let Some(base) = referenced.strip_suffix(suffix) {
            if emitted.contains(base) {
                return true;
            }
        }
    }
    emitted
        .iter()
        .any(|candidate| candidate.starts_with(referenced))
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

    let mut emitted = BTreeSet::new();
    metric_names_in_source(&root.join("src"), &mut emitted);

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
