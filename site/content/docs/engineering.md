+++
title = "How defects are prevented"
description = "The structural guards in rustcdc's test suite: assertions that encode a whole class of defect rather than one instance, what each one caught, and where each one is blind."
weight = 80
+++

Most tests check that the code does what it does. The guards on this page check that the
code *cannot* do a specific wrong thing, for every future change. Each entry states the
defect class, how the guard works, and where it is **blind**: a guard whose limits are
undocumented buys confidence it has not earned.

All of these run on every `cargo test`. None needs a database, a broker or a network.


## 1. Inert settings

`tests/architecture.rs::every_configuration_setting_is_read_by_something`

**Defect class.** A setting that parses, validates, documents itself and reaches no code.
The operator sets it, sees no error, and believes the system is behaving as configured.

**How it works.** Extracts every field of every configuration struct and requires each to
appear in a consumer outside `src/config/`. The corpus is stripped to code (comments and
string literals do not count as readers), matches need an identifier boundary, attributes
between a derive and its struct are skipped rather than ending the scan, and a method call
such as `addr.port()` is not a read of a field called `port`.

**Caught:** `*_rate_limit_burst` (parsed, validated, and ignored: an operator setting `40`
got `1`), `--admin-write-token` and `--admin-write-token-env` (read by nothing).

**Does not catch:** a setting that is read and then discarded, or read on a path that never
executes. It matches by field *name*, so the staleness half of the check is skipped for a
name two structs share.


## 2. Panicking constructs in production code

`tests/architecture.rs::production_code_panics_only_where_the_allowlist_says_it_may`

**Defect class.** `unwrap`, `expect` and `panic!` outside tests. In a server holding a
replication slot, a panic is an unreleased slot, a WAL volume filling up, and a checkpoint
that stops advancing.

**How it works.** Scans `src/` outside `#[cfg(test)]` blocks and `*_tests.rs` files and
compares every panicking construct against an allowlist, each entry carrying the argument
for why it cannot fire. The list is checked in both directions: an entry whose construct is
gone also fails.

**Caught:** three `Mutex::lock().expect(..)` in the signal ledger, and a
`serde_json::to_vec(..).expect(..)` on the `migrate-state` recovery path.

**Does not catch:** arithmetic overflow, slice indexing or `RefCell` borrow panics. It cannot
judge whether an allowlist argument is *true*.


## 2b. `unsafe` outside the allowlist

`tests/architecture.rs::unsafe_code_appears_only_where_the_allowlist_says_it_may`

**Defect class.** `unsafe` appearing where nobody argued for it.

**How it works.** The server binary carries `#![forbid(unsafe_code)]`, which no inner
`#[allow]` can override. Both crates deny `unsafe_code` through `[workspace.lints]`, and the
test scans both crates' `src/` for the `unsafe` keyword in code. Two files are allowed:

| File | Why |
|---|---|
| `crates/rustcdc-server/src/test_env.rs` | `std::env::set_var` is `unsafe` in Rust 2024; the one place the server mutates the environment, serialised by `EnvGuard` |
| `crates/rustcdc/src/checkpoint/owner_lease.rs` | Windows process-liveness check: the `OpenProcess`/`CloseHandle` FFI has no safe wrapper in the dependency tree |

The list is checked in both directions.

**Caught:** seven sites, six `unsafe` blocks and an `unsafe fn`, in a package that had not
opted into `[workspace.lints]`.

**Does not catch:** `unsafe` in a dependency. That is what the TLS-stack guard and
`cargo deny` are for.


## 2c. Secret-named settings must be typed as secrets

`tests/architecture.rs::a_secret_named_setting_is_typed_as_a_secret`

**Defect class.** A credential in the configuration typed as a plain `String`.

**How it works.** `GET /status` and `GET /config` serialise the running configuration for
any read-scoped token. `SecretString` serialises as `[REDACTED]`, so a credential with that
type never reaches the wire; the name rules in `src/redaction.rs` are a backstop for strings
that arrive from elsewhere (a `?api_key=` in a URL, a header value). The guard requires every
secret-named field to have the type.

**Does not catch:** a secret in a field named nothing like one.


## 2d. Redaction exceptions must earn their place

`redaction::tests::every_redaction_exception_names_a_field_that_exists`

**Defect class.** A "do not redact" entry for a field that is gone, or that would never have
been redacted anyway. Either way it is a standing licence for the next field to take the name.

**How it works.** Requires every exception to name a field that exists and that the
sensitive-name rules would otherwise redact. A `*_env` name is a variable name, not a secret,
and is not redacted: `GET /config` is where an operator answers "which variable holds my
token?".

**Caught:** five of six entries: two named fields that exist nowhere, three were no-ops.


## 3. One TLS stack in every build

`tests/architecture.rs::the_default_build_has_one_tls_stack`, `the_all_features_build_has_one_tls_stack`

**Defect class.** A dependency quietly adding a second TLS implementation: two X.509
verifiers, two certificate-validation paths and two advisory streams.

**How it works.** Asserts against the resolved dependency graph (`cargo tree --edges normal`),
not against `Cargo.toml`, for the default build and the `--all-features` build the image
ships: one `rustls`, one `rustls-webpki`. The default build must also not link `tiberius`.
`the_tls_stack_check_fires_on_a_second_rustls` plants a second `rustls` and requires the
check to fail.

**Does not catch:** a second TLS *implementation* that is not rustls (`native-tls`,
OpenSSL). `cargo deny`'s bans refuse those.


## 4. Spec drift between the router and `/openapi.json`

`tests/architecture.rs::every_admin_route_appears_in_the_openapi_document`

**Defect class.** A published API specification that has stopped describing the server.

**How it works.** Extracts the paths the axum router serves and requires each to appear in
the document.

**Does not catch:** a documented route whose *schema* has drifted from the handler. Paths
are checked; payloads are not.


## 5. Alert rules that reference metrics the server emits

`tests/alert_rules.rs::every_alert_rule_references_a_metric_the_server_emits`

**Defect class.** A shipped alert rule watching a metric name that does not exist. It never
fires and is discovered during the incident it was meant to catch.

**How it works.** Extracts every metric name from `monitoring/rustcdc_slo_alerts.yml` and
requires the Prometheus render path to emit it. A sibling test forbids alerting on
millisecond-denominated families, because a rule written against the wrong unit is off by
a factor of a thousand.

**Does not catch:** a family that is rendered but can never be non-zero. That is pinned
separately by `pipeline::binding::extended_sink_counters_reach_the_registry_through_the_router`,
which sends through a real router and asserts on a counter the generic adapter interface
cannot carry.


## 6. Nothing spawns a detached runtime

`tests/architecture.rs::no_module_spawns_a_detached_thread_with_its_own_runtime`

**Defect class.** A module starting its own tokio runtime on its own OS thread. Work on it is
invisible to shutdown and the graceful-drain path, and holds file descriptors past the point
where the state lease is released.

**Does not catch:** a `tokio::spawn` whose handle is dropped. Those are supervised by the
worker registry, which `shutdown_workers` joins.


## 7. File size

`tests/architecture.rs::no_source_file_grows_past_the_point_of_navigability`

**Defect class.** The module that becomes unreviewable because it mixes HTTP handlers, shared
state, background workers and metric rendering.

**How it works.** 3 600 lines for a production module and 3 850 for a test file under the
server's `src/`, tracked separately. Each split lowers both numbers to just above the
largest file left, because a budget above the worst case in the tree does no work.

**Does not catch:** a file under budget that is still unreviewable. The library's `src/` is
not scanned.


## 8. Version and MSRV restatements

`tests/architecture.rs::every_statement_of_the_release_version_matches_cargo_toml`
and `every_statement_of_the_msrv_matches_cargo_toml`

**Defect class.** A number restated in several places and updated in only some of them.

**Does not catch:** a version stated in prose rather than in a recognised field.


## 9. Every integration suite is run by CI, against every broker

`tests/architecture.rs::every_integration_suite_is_run_by_ci`

**Defect class.** A suite that exists in the repository and in nobody's pipeline.

**How it works.** Enumerates `tests/integration_*.rs`, requires each to appear in the CI
workflow matrix, and requires the job to set `RUSTCDC_INTEGRATION=1`, without which every
test returns immediately and the job passes having tested nothing. `integration_kafka.rs`
must run against both Apache Kafka and Redpanda: they differ in how new-topic metadata
propagates and when a group coordinator is elected.

**Does not catch:** a test inside a suite that skips on its own. Those exist for Iceberg
REST and for pointing at an external cluster.


## 10. The guard on a guard

`tests/architecture.rs::sink_config_enum_body_is_extracted_not_the_whole_file`

Several sink-configuration guards extract the body of one enum and search it. If the marker
moves, the search runs over an empty string and the guard passes forever. This test asserts
the extractor found the enum and not the whole file: **a structural guard needs its own
tripwire**, because the failure mode of a guard is silence.


## Summary

| Guard | Defect class | Caught |
|---|---|---|
| Inert settings | Configuration that reaches no code | 4, plus 3 a prose-matching corpus masked |
| Panicking constructs | `unwrap`/`expect`/`panic!` outside tests | 4 |
| `unsafe` allowlist | `unsafe` nobody argued for | 7 |
| Secret-named settings | A credential typed as a plain `String` | prevents by construction |
| Redaction exceptions | "Do not redact" entries that are stale or no-ops | 5 of 6 |
| TLS stack graph | A second X.509 verifier via a transitive dep | 1 |
| Router ↔ OpenAPI | Specification drift | prevents by construction |
| Alert ↔ metric names | Rules that can never fire | 2 (blind to zero-valued families, see §5) |
| Detached runtimes | Work invisible to shutdown | 1 |
| File size | Unreviewable modules | 1 |
| Version / MSRV | Numbers restated and not updated | 1 |
| CI ↔ integration suites | Suites nobody runs; a broker matrix silently halved | 2 |
| Extractor tripwire | A guard that silently stopped guarding | prevents by construction |

These encode defect *classes*. They are not a substitute for running against a real
database, and §5 shows a guard can be green while the behaviour it describes is broken.
