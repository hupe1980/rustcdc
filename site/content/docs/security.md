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
  rustls's CSPRNG, so there is one secure random source to audit rather than two.
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

### Outbound webhook signing

The HTTP sink signs every request to
[Standard Webhooks](https://www.standardwebhooks.com/): HMAC-SHA256 (`v1`) or ed25519
(`v1a`) over `{id}.{timestamp}.{payload}`. Correctness is pinned against the
specification's published test vector, so the check is interoperability with receiver
libraries rather than self-consistency.

- **The signed bytes are the sent bytes** — one `Bytes` buffer reaches both the signer and
  the HTTP client, so no re-serialisation can slip between them.
- **Each attempt is signed afresh**, because the timestamp is inside the signature and
  receivers reject one outside their tolerance.
- **Signing keys must be deferred references.** A literal `key` is refused at load; a leaked
  one lets anyone forge events as this pipeline.
- **A key with the wrong scheme's prefix is refused** — an ed25519 private key is a valid
  HMAC secret and would otherwise sign requests nothing could verify.

Prefer ed25519 whenever the receiver is not you: it holds only the public half, so a
compromised receiver cannot forge events back.

## Secrets

- Passwords and connection secrets are held in `SecretString`, which zeroizes on drop and
  redacts in `Debug` output.
- The webhook signer's `Debug` prints its scheme and key *count*, never key material.
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

## One TLS stack, in every build

Every TLS connection rustcdc makes — every source including SQL Server, every sink, the schema
registries and the admin listener — goes through one `rustls` 0.23 and one `rustls-webpki`. A
test asserts it against the resolved dependency graph of both the default build and the
`--all-features` build the container image is made from, so a dependency that adds a second
stack fails the build rather than shipping.

The crypto backend under it is `aws-lc-rs` in the server, installed as the process default at
startup. The library uses `ring` unless you install a process default; its `sqlserver` feature
also compiles `aws-lc-rs`, which the SQL Server driver selects when you have not.

## What `deny.toml` still suppresses

Three advisories, none in a path that handles untrusted TLS or decrypts anything:

| Advisory | Crate | Where | Why it stays |
|---|---|---|---|
| [RUSTSEC-2024-0436](https://rustsec.org/advisories/RUSTSEC-2024-0436) | `paste` | Proc-macro, compile time only | No fixed release exists |
| [RUSTSEC-2023-0071](https://rustsec.org/advisories/RUSTSEC-2023-0071) | `rsa` 0.9 | Azure Blob request signing, through OpenDAL | A timing side-channel in RSA *decryption*; signing does not decrypt. Clears when the signer moves to `rsa` 0.10 |
| [RUSTSEC-2026-0440](https://rustsec.org/advisories/RUSTSEC-2026-0440) | `astral-tokio-tar` | `testcontainers`, a dev-dependency | Unmaintained notice; in no published crate and no image |

## Reporting

Report suspected vulnerabilities in rustcdc itself through the repository's private
security advisory channel rather than a public issue.
