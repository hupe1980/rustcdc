+++
title = "Security"
description = "rustcdc's TLS posture, secret handling, WASM sandbox, and the security advisories it carries."
weight = 120
+++

This document records rustcdc's security-relevant defaults and its known, unfixed
dependency exposure. It is maintained alongside `deny.toml`, which enforces the same
policy in CI on every pull request.

## Transport defaults

- **All connectors default to TLS.** Plaintext requires the explicit escape hatch
  `TransportConfig::plaintext()`, which is documented as unsuitable for production.
- Private-CA and mutual-TLS deployments are supported directly through
  `TransportConfig::tls_with_ca_cert_path(...)` and `TransportConfig::mtls(...)`. Neither
  requires a Cargo feature, so the secure path is never the one behind a flag.
- `allow_invalid_hostnames` is deliberately **not** wired to tiberius' `trust_cert()` for
  SQL Server. `trust_cert()` disables the entire chain check, which is a strictly larger
  concession than relaxing hostname verification; conflating the two would silently
  discard the operator's configured CA.
- **PostgreSQL sets `sslmode=require` whenever the transport is TLS.** `tokio-postgres`
  defaults to `prefer`, which falls back to an unencrypted connection when the server
  refuses the SSL request — a configuration that says TLS and delivers plaintext, visible
  only in a packet capture. Both connections a PostgreSQL stream opens (SQL and
  replication) enforce this and build their rustls configuration from the same
  `TransportConfig`, so they cannot disagree about what they verify. A server that refuses TLS
  fails the connection rather than being silently downgraded.

## Authentication code this crate owns

The PostgreSQL replication transport authenticates itself (`source::postgres::wire::auth`)
rather than through a client library, because `tokio-postgres` cannot open a replication
connection at all. That puts a **SCRAM-SHA-256 implementation inside this crate**, so it is
called out here rather than left to be discovered:

- **SCRAM-SHA-256** (RFC 5802 / RFC 7677) is the primary method. `Hi`, the client proof and
  the server signature are each asserted against the published RFC 7677 test vectors, not
  against themselves.
- **The server signature is verified.** Skipping that check is the common shortcut, and it
  leaves the exchange authenticating the client to the server but not the reverse. The
  comparison is constant-time.
- **The server nonce is checked to extend the client nonce**, which is what makes a
  replayed or substituted server-first message fail. The client nonce is 24 bytes from
  rustls's CSPRNG — no separate RNG dependency was added, so there is one secure random
  source to audit rather than two.
- **`SCRAM-SHA-256-PLUS` (TLS channel binding) is not implemented.** The residual risk is a
  downgrade: an attacker positioned to intercept could advertise only `SCRAM-SHA-256`. That
  position already requires a certificate chaining to a trusted root for the right hostname,
  since verification is on by default and cannot be disabled on this connector. When a
  server advertises `-PLUS` the connector logs that a stronger mechanism was available, so
  the downgrade is not silent. A server that offers *only* `-PLUS` is refused with an error
  naming `WalTransport::SqlPeek`, which authenticates through `tokio-postgres`.
- **The iteration count is capped, and the derivation runs off the caller's executor.** The
  count is chosen by the *server* and is the loop bound of a PBKDF2 derivation the *client*
  performs. `scram_iterations` accepts anything up to `INT_MAX`, and neither the RFC nor libpq
  imposes a ceiling, so `i=4294967295` asks this client for roughly four billion HMAC-SHA256
  rounds — minutes to hours of CPU per connection attempt, free for the server to trigger and
  indistinguishable on the wire from a slow handshake. Counts above **1,000,000** are refused
  with an error naming the setting to change (~250× PostgreSQL's default of 4096, and past
  OWASP's 600k guidance, so no legitimate server reaches it). The derivation itself runs on a
  blocking worker: it is CPU work of remote-chosen duration, and this crate runs inside the
  embedder's Tokio runtime, where deriving inline would stall a worker thread — every task in
  the process, on a current-thread runtime.
- **MD5** is supported because servers still run it, and logs a warning each time: the
  stored digest is a password equivalent, and PostgreSQL has deprecated the method.
- **Cleartext** is refused unless the transport is TLS.

## Secrets

- Passwords and connection secrets are held in `SecretString`, which zeroizes on drop and
  redacts in `Debug` output.
- Structured log events redact credential-bearing fields. `tests/logging_structured.rs`
  asserts that a connection error carrying a JWT does not reproduce it in the log line.

## Dependency policy

`cargo deny check` runs in the `policy-gate` CI job and gates every pull request across
advisories, licenses, bans and sources. The dependency graph is resolved with
`all-features = true`; without that, every optional dependency — including the wasmtime
JIT, the crypto stack, mysql_async and tiberius — would be invisible to the advisory scan.

Every advisory suppression in `deny.toml` must carry the RUSTSEC ID, the exact package
chain, and the reason it cannot be fixed today. Suppressions are removed as soon as
upstream ships a fix.

## Known exposure: `sqlserver` feature

> **This is the only unfixed vulnerability class in a production code path.** Everything
> else suppressed in `deny.toml` is dev-dependency-only and absent from released builds.

Enabling the `sqlserver` feature pulls in `tiberius 0.12.3`, which hard-pins
`tokio-rustls 0.24` and therefore `rustls 0.21` / `rustls-webpki 0.101.7`. That copy of
webpki carries three advisories:

| Advisory | Issue | Reachable in rustcdc? |
|---|---|---|
| [RUSTSEC-2026-0098](https://rustsec.org/advisories/RUSTSEC-2026-0098) | URI name constraints ignored, therefore accepted | No path depends on it — webpki 0.101 offers no API to assert URI names, and rustcdc asserts none |
| [RUSTSEC-2026-0099](https://rustsec.org/advisories/RUSTSEC-2026-0099) | Name constraints accepted for wildcard names | **In principle yes** — it is in the server-certificate verification path |
| [RUSTSEC-2026-0104](https://rustsec.org/advisories/RUSTSEC-2026-0104) | Reachable panic parsing certificate revocation lists | No — rustcdc never configures a CRL, and tiberius exposes no API to supply one |

**Why it is not fixed:** `tiberius 0.12.3` is the latest published release. The advisories
are fixed only in `rustls-webpki >= 0.103.12`, which requires `rustls 0.23` — not
semver-reachable from tiberius' pin. rustcdc's *own* TLS stack is already `rustls 0.23`;
this older copy exists solely inside tiberius and cannot be deduplicated away.

**What exploitation requires.** All three are name-constraint or CRL parsing defects.
Name constraints restrict certificates that are otherwise properly issued, so the bug is
reached only *after* signature verification succeeds. Exploiting RUSTSEC-2026-0099
additionally requires a CA the client already trusts to have **misissued** a wildcard
certificate that escapes a name constraint.

**Guidance:**

- Deployments that pin a private CA via `TransportConfig::tls_with_ca_cert_path(...)` and
  do not rely on name-constrained intermediates are not materially exposed.
- Deployments that cannot accept this exposure should not enable the `sqlserver` feature.
  It is opt-in; `--no-default-features` and the PostgreSQL/MySQL/MariaDB profiles do not
  compile tiberius at all.
- The suppressions in `deny.toml` will be removed as soon as tiberius publishes a release
  built against `rustls 0.23`.

## Reporting

Report suspected vulnerabilities in rustcdc itself through the repository's private
security advisory channel rather than a public issue.
