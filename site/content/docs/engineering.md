+++
title = "How defects are prevented"
description = "The structural guards in rustcdc's test suite: assertions that encode a whole class of defect rather than one instance, what each one caught, and where each one is blind."
weight = 80
+++

Most test suites check that the code does what it does. A handful of tests here check that
the code *cannot* do a specific wrong thing — and keep checking it for every future change,
including changes nobody has thought of yet.

This page lists them, says what each one caught in practice, and — the part that matters
more — says where each one is **blind**. A guard whose limits are undocumented is worse
than no guard, because it buys confidence it has not earned. Every entry below has a "does
not catch" line, and one of them is there because a real defect walked straight past it.

All of these run on every `cargo test`. None needs a database, a broker or a network.


## 1. Inert settings

`tests/architecture.rs::every_configuration_setting_is_read_by_something`

**The defect class.** A setting that parses, validates, documents itself and reaches no
code. It is worse than a missing feature: the operator sets it, sees no error, and believes
the system is behaving as configured.

**How it works.** Extracts every field of every configuration struct, then requires each
one to appear in a consumer outside `src/config/`. A field nothing reads fails the build.

**Caught so far:** four. `*_rate_limit_burst` was parsed and validated while new clients
were unconditionally admitted with one token, so an operator setting `40` got `1`.
`--admin-write-token` and `--admin-write-token-env` were accepted by the CLI and read by
nothing, because no command wrote to the admin API — the `snapshot` subcommand exists
because this scanner asked what those flags were for.

**It was blind five ways, and later rounds found all five.** The corpus it searched
included **comments and string literals**, so a doc comment mentioning `.scope` or a
validation message containing `"sink.iceberg.catalog.rest.uri"` counted as a reader. The
match was a bare substring, so `.token` was satisfied by `.token_endpoint`. Both errors run
in the direction that matters: they mark a dead setting live, and the check passes while
missing exactly what it exists to find. The corpus is now stripped to code, and the match
requires an identifier boundary — which immediately surfaced three settings the prose had
been masking.

Two more surfaced when a new sink was added. Its `#[serde(deny_unknown_fields)]` sat between
the derive and the struct, and the scanner cleared its pending derive on any non-comment
line — so **the entire struct was invisible** and every setting it declared was exempt by
accident. An attribute is not an item; it no longer clears. And `addr.port()` counted as a
read of a config field called `port`, because method calls were being treated as readers. A
field access is never a call.

**Does not catch:** a setting that is read but whose value is then discarded, or read on a
path that never executes. Reachability is not liveness. It also matches by field *name*, so
two structs sharing a name are indistinguishable — the staleness half of the check is now
skipped for such names rather than reporting a false positive, and that is a real limit, not
a fix.


## 2. Panicking constructs in production code

`tests/architecture.rs::production_code_panics_only_where_the_allowlist_says_it_may`

**The defect class.** `unwrap`, `expect` and `panic!` outside tests. In a server holding a
replication slot, a panic is not a crash report — it is an unreleased slot, a WAL volume
filling up, and a checkpoint that stops advancing.

**How it works.** Scans `src/` outside `#[cfg(test)]` blocks and `*_tests.rs` files for
panicking constructs and compares them against a hard-coded allowlist. Each allowlist entry
carries the argument for why that construct cannot fire. The allowlist is checked in both
directions: an entry whose construct has been deleted also fails, so the list cannot rot
into a blanket licence.

**Caught so far:** five. Three `Mutex::lock().expect(..)` in the signal ledger — one
poisoned lock there is a permanent outage of the whole signal-ingress path, because the
panic guard around signal actions keeps the process alive to refuse every later command —
and a `serde_json::to_vec(..).expect(..)` on the disaster-recovery path of `migrate-state`.
One remains: an `unreachable!` guarded by an observation two lines above it.

**Does not catch:** arithmetic overflow, slice indexing, or `RefCell` borrow panics, none of
which look like the scanned constructs. It also cannot judge whether an allowlist argument
is *true*.


## 2b. `unsafe` in the library

`tests/architecture.rs::unsafe_code_appears_only_where_the_allowlist_says_it_may`

**The defect class.** `unsafe` appearing where nobody argued for it.

**What it caught.** Seven sites — six `unsafe` blocks and an `unsafe fn` wrapping a call to
`kill(1)` that did nothing unsafe at all. They survived because the lint meant to prevent
them **was not running**: `[workspace.lints]` only applies to a package that opts in with
`[lints] workspace = true`, and this one did not.

**How it works now.** The binary carries `#![forbid(unsafe_code)]`, which no inner
`#[allow]` can override — so the artifact this project ships genuinely contains none. The
library carries `deny` with exactly one exemption, `test_env::write_env`, because
`std::env::set_var` is `unsafe` in Rust 2024 and several tests need a variable in the
process environment. The allowlist is checked in both directions.

**Worth knowing.** Edition 2024 is what made this visible: `set_var` is safe to call in
2021, so the hazard — it races any concurrent `getenv`, and `cargo test` is multi-threaded —
was invisible. Seven env-mutating sites turned up across five files, none consistently
serialised, several leaking the variable into every later test when an assertion failed
between set and restore.

**Does not catch:** `unsafe` in a dependency. That is what the TLS-stack guard and
`cargo deny` are for.


## 2c. Secret-named settings must be typed as secrets

`tests/architecture.rs::a_secret_named_setting_is_typed_as_a_secret`

**The defect class.** A credential in the configuration typed as a plain `String`.

**Why the type is what matters.** `GET /status` and `GET /config` serialise the running
configuration for any read-scoped token. `SecretString`'s `Serialize` emits `[REDACTED]`, so
a credential with that type never reaches the wire at all — the name-matching rules in
`src/redaction.rs` are a *backstop* for strings that arrive from elsewhere (a `?api_key=` in
a URL, a header value), not the primary defence.

Which makes the dangerous shape a secret-named field typed as `String`: nothing protects it
at the type level, and it survives only as long as a substring rule happens to match.

**A trap worth naming.** It is easy to "prove" a leak by handing the redactor a JSON string
you wrote yourself — but production never produces that shape, because the field is a
`SecretString`. The same caution as §5: a check that constructs its own inputs tests the
check, not the system.

**Does not catch:** a secret in a field named nothing like one. Nothing can.


## 2d. Redaction exceptions must earn their place

`redaction::tests::every_redaction_exception_names_a_field_that_exists`

**The defect class.** An entry that says "do not redact this" for a field that is gone, or
that would never have been redacted anyway. Either way it is a standing licence inherited by
the next field to take the name.

**Caught immediately:** five of six entries. Two — `trusted_signer_public_keys_hex`,
`revoked_signer_public_keys_hex` — named fields that exist nowhere in the tree. Three more —
`public_key_hex`, `trusted_public_keys_hex`, `signature_hex` — were **no-ops**: no token in
the sensitive-name list matches them, so exempting them changed nothing. They are the fossil
of an earlier list that contained a bare `"key"`. One entry survived, and it is provably
load-bearing.

**It also replaced a tautology.** The test it supersedes walked the schema for "secret-like"
field names using its **own inline copy** of the token list — which drifted, and which, once
corrected to use the real constant, asserts only that a name containing a token is matched
by the rule that matches names containing tokens.

**Over-redaction is a cost too.** `read_token_env` and `password_env` were blanked while
`audit_signing_key_env` was not — nobody chose that; it fell out of which substrings were in
the list. `GET /config` is where an operator answers "which variable holds my token?", and
blanking it sends them to read the config file off the disk, which is worse for security
rather than better. A `*_env` name is a variable name, not a secret, and is now treated as
one.


## 3. One TLS stack in the default build

`tests/architecture.rs::the_default_build_has_one_tls_stack`

**The defect class.** A dependency quietly adding a second TLS implementation. Two stacks
means two X.509 verifiers, two certificate-validation code paths and two advisory streams —
and the second one is invariably the unmaintained one.

**How it works.** Asserts against the resolved dependency graph of the default feature set,
not against `Cargo.toml`. A transitive dependency is exactly as capable of adding a stack as
a direct one.

**Why it exists.** `tiberius 0.12` pins rustls 0.21 and brings four suppressed RUSTSEC
advisories with it. That is why connectors are opt-in cargo features rather than always
compiled: a PostgreSQL-only build links one stack and needs none of the reachability
analysis that "unreachable in your configuration" requires.

**Does not catch:** a second stack in a non-default feature combination. Enabling
`sqlserver` deliberately re-adds one, which is documented rather than prevented.


## 4. Spec drift between the router and `/openapi.json`

`tests/architecture.rs::every_admin_route_appears_in_the_openapi_document`

**The defect class.** A published API specification that has quietly stopped describing the
server. Every generated client, every contract test and every integration written against
it inherits the drift.

**How it works.** Extracts the paths the axum router actually serves and requires each one
to appear in the document. Adding a route without documenting it fails the build.

**Does not catch:** a documented route whose *schema* has drifted from the handler. Paths
are checked; payloads are not.


## 5. Alert rules that reference metrics the server emits

`tests/alert_rules.rs::every_alert_rule_references_a_metric_the_server_emits`

**The defect class.** A shipped alert rule watching a metric name that does not exist. It
never fires, it looks like coverage on a dashboard, and it is discovered during the incident
it was meant to catch.

**How it works.** Extracts every metric name from `monitoring/rustcdc_slo_alerts.yml` and
requires the Prometheus render path to emit it. A sibling test forbids alerting on
millisecond-denominated families, because Prometheus convention is base units and a rule
written against the wrong one is off by a factor of a thousand.

**Does not catch — and this is the one that was tested in anger.** It checks that the family
is *rendered*, not that it can ever be non-zero. Thirty `rustcdc_sink_*` and
`rustcdc_iceberg_*` families were fed through the routing layer, whose generic adapter
interface carries four counters; the other twenty-nine were dropped on the floor and
rendered a constant `0` for the life of the process. Four alert rules watched four of them.
This guard passed the whole time, because the names were all present.

What closed it was not a bigger name check but a test that drives the **production path** —
`pipeline::binding::extended_sink_counters_reach_the_registry_through_the_router` sends
through a real router and asserts on a counter that is *structurally unrepresentable* in the
generic interface. Reverting to the old path puts it back to zero and fails. The general
lesson is the one this project keeps relearning: **a test that builds its own inputs proves
the assertion, not the system.**


## 6. Nothing spawns a detached runtime

`tests/architecture.rs::no_module_spawns_a_detached_thread_with_its_own_runtime`

**The defect class.** A module quietly starting its own tokio runtime on its own OS thread.
Work on it is invisible to shutdown, unaffected by the graceful-drain path, and holds file
descriptors past the point where the state lease is released.

**Does not catch:** a `tokio::spawn` whose handle is dropped. Those are supervised by the
worker registry instead, which `shutdown_workers` joins.


## 7. File size

`tests/architecture.rs::no_source_file_grows_past_the_point_of_navigability`

**The defect class.** The module that becomes unreviewable. Length is not a style question
when the file mixes HTTP handlers, shared state, background workers and metric rendering:
the cost lands on every future review of every one of them.

**How it works.** 3 700 lines for a production module, 4 000 for a test file, tracked
separately — test files legitimately grow with the cases they cover.

**A note on calibration.** The budget fired for the first time at 4 144 lines, and the right
response turned out to be two things, not one: split the module, *and* lower the threshold.
A guard calibrated above the worst case in the tree is not doing work.


## 8. Version and MSRV restatements

`tests/architecture.rs::every_statement_of_the_release_version_matches_cargo_toml`
and `every_statement_of_the_msrv_matches_cargo_toml`

**The defect class.** A number restated in four places and updated in three. The MSRV had
already drifted once — the manifest said `1.94` while CI pinned `1.94.0` — and the symptom
was a build failure that named neither.

**Does not catch:** a version stated in prose rather than in a recognised field.


## 9. Every integration suite is run by CI, against every broker

`tests/architecture.rs::every_integration_suite_is_run_by_ci`

**The defect class.** A suite that exists in the repository and in nobody's pipeline.
Enumerates `tests/integration_*.rs` and requires each to appear in the CI workflow matrix,
and requires the job to set `RUSTCDC_INTEGRATION=1` — without which every test in every
suite returns immediately and the job passes having tested nothing.

**It also pins the broker matrix.** `tests/integration_kafka.rs` is parameterised over
Apache Kafka and Redpanda, and naming the suite once would run whichever the default is
while reporting coverage of both. Redpanda is an independent reimplementation of the Kafka
wire protocol, not a repackaging, and the two disagreed on the *first* run: Apache Kafka
propagates new-topic metadata asynchronously and elects a group coordinator lazily, neither
of which Redpanda does. A Redpanda-only suite would have shipped a harness that could not
test Kafka at all.

**Does not catch:** a test inside a suite that skips on its own. Those still exist for
Iceberg REST and for pointing at an external cluster, and they are now the smaller of the
two gaps rather than the whole story.


## 10. The guard on a guard

`tests/architecture.rs::sink_config_enum_body_is_extracted_not_the_whole_file`

Several sink-configuration guards work by extracting the body of one enum and searching it.
The extraction is the part that can silently fail: if the marker moves, the search runs over
an empty string and the guard passes trivially, forever.

So there is a test asserting that the extractor found the enum and not the whole file. If
you take one idea from this page, take this one — **a structural guard needs its own
tripwire**, because the failure mode of a guard is silence.


## Where this leaves the suite

| Guard | Defect class | Caught in practice |
|---|---|---|
| Inert settings | Configuration that reaches no code | 4, plus 3 its corpus had masked, plus a whole struct class its parser could not see |
| Panicking constructs | `unwrap`/`expect`/`panic!` outside tests | 4 |
| `unsafe` allowlist | `unsafe` nobody argued for — and a lint that was not running | 7 |
| Secret-named settings | A credential typed as a plain `String` | prevents by construction |
| Redaction exceptions | "Do not redact" entries that are stale or no-ops | 5 of 6 |
| TLS stack graph | A second X.509 verifier via a transitive dep | 1 (design-forcing) |
| Router ↔ OpenAPI | Specification drift | 0 so far; prevents by construction |
| Alert ↔ metric names | Rules that can never fire | 2, and missed 4 — see §5 |
| Detached runtimes | Work invisible to shutdown | 1 |
| File size | Unreviewable modules | 1, and recalibrated itself |
| Version / MSRV | Numbers restated and not updated | 1 |
| CI ↔ integration suites | Suites nobody runs; a broker matrix silently halved | 2 |
| Extractor tripwire | A guard that silently stopped guarding | prevents by construction |

The honest summary: these encode defect *classes*, which is the reason they keep paying out
long after the instance that motivated them is forgotten. They are not a substitute for
running the thing against a real database, and §5 is the standing proof that a guard can be
green while the behaviour it describes is broken.
