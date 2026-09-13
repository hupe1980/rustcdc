//! Guards on the assembled `/metrics` body.
//!
//! A sibling of `tests.rs` rather than another block inside it: that file had reached the
//! per-file budget `tests/architecture.rs` enforces, and the budget exists because a file
//! nobody can navigate is where defects hide. The seam is by concern — everything here is
//! about the *document* `/metrics` serves, rather than about any one metric's value.

use std::time::Duration;

use super::tests::sample_config;
use super::{AbuseLimitScope, AdminState, InstanceState};

/// Every metric family in `/metrics` must declare itself exactly once.
///
/// # Why this is not covered by the encoder's own guard
///
/// The exposition is a concatenation of independently-built blocks — the library's runtime
/// renderer, the recoverable-error and sink renderers, the SLO block, the auth block, the
/// audit counter and the snapshot-progress block. `PrometheusTextEncoder` tracks the
/// families it has emitted, but that tracking lives inside a single encoder instance and
/// three of those blocks do not use the encoder at all; they `push_str` their headers
/// directly.
///
/// So the invariant the format actually imposes — Prometheus and OpenMetrics both require
/// exactly one `# HELP`/`# TYPE` per family — had no guard spanning the document. A family
/// added to two blocks would render a scrape that strict parsers reject **whole**, so the
/// symptom is every metric disappearing at once, not one metric going wrong.
///
/// The check is on the real assembled body, via the same function the handler serves, so
/// it cannot pass against a reconstruction that has drifted from what is served.
#[tokio::test]
async fn the_metrics_exposition_declares_each_family_once() {
    let cfg = sample_config();
    let admin = AdminState::new(&cfg).await.expect("admin state");

    // Populate the optional blocks, so families that only appear once something has
    // happened are present in the document under test rather than silently skipped.
    admin.set_state(InstanceState::Running).await;
    admin
        .record_rate_limited_request(AbuseLimitScope::Metrics)
        .await;
    admin
        .record_rate_limiter_decision(AbuseLimitScope::Metrics, Duration::from_millis(1))
        .await;
    admin.record_shutdown_request_os_signal().await;

    let exposition = admin.metrics_exposition().await;

    let mut help: std::collections::BTreeMap<&str, usize> = std::collections::BTreeMap::new();
    let mut type_: std::collections::BTreeMap<&str, usize> = std::collections::BTreeMap::new();
    for line in exposition.lines() {
        let (table, rest) = match line.strip_prefix("# HELP ") {
            Some(rest) => (&mut help, rest),
            None => match line.strip_prefix("# TYPE ") {
                Some(rest) => (&mut type_, rest),
                None => continue,
            },
        };
        let family = rest.split_whitespace().next().unwrap_or_default();
        *table.entry(family).or_default() += 1;
    }

    assert!(
        !help.is_empty(),
        "the fixture rendered no metrics at all, so this guard would pass vacuously"
    );

    // Keyed by family so a family duplicated in both its HELP and its TYPE line — the
    // usual shape — is reported once rather than twice.
    let mut duplicated: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    for (family, count) in help.iter().chain(type_.iter()) {
        if *count > 1 {
            duplicated.insert(format!("{family} ({count}x)"));
        }
    }
    let duplicated: Vec<String> = duplicated.into_iter().collect();
    assert!(
        duplicated.is_empty(),
        "these metric families are declared more than once in one exposition, which makes \
         the scrape malformed: {}",
        duplicated.join(", ")
    );
}
