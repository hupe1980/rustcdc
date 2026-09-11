//! Negative-path security suite.
//!
//! A regression test written after a defect proves that defect is gone; it does not go
//! looking. These probes do — each one states an attack and asserts it fails.
//!
//! They drive the **real axum router** in-process via `tower::ServiceExt`, so
//! they exercise the same extractors, middleware and handlers a network client hits.
//! Calling the handler functions directly would skip precisely the layers where an
//! authorisation bypass tends to live.
//!
//! Each test states the attack, not the mechanism. A test named
//! `expired_token_is_rejected` is a claim about the system's behaviour under attack;
//! `test_auth_3` is not.

use std::net::SocketAddr;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use ed25519_dalek::{Signer, SigningKey};
use rustcdc_server::admin::AdminState;
use rustcdc_server::config::AppConfig;
use rustcdc_server::token_manifest_policy::{
    TokenManifestFile, TokenManifestSignature, TokenManifestToken, TokenManifestUnsigned,
    canonical_signing_payload,
};
use tower::ServiceExt;

// ─────────────────────────────────────────────────────────────────────────────
// Fixtures
// ─────────────────────────────────────────────────────────────────────────────

const READ_TOKEN: &str = "read-token-value-not-a-real-secret";
const WRITE_TOKEN: &str = "write-token-value-not-a-real-secret";

fn sha256_hex(value: &str) -> String {
    use sha2::Digest;
    let mut hasher = sha2::Sha256::new();
    hasher.update(value.as_bytes());
    hex::encode(hasher.finalize())
}

fn manifest_token(id: &str, token: &str, scopes: &[&str]) -> TokenManifestToken {
    TokenManifestToken {
        id: id.to_string(),
        token_sha256_hex: sha256_hex(token),
        scopes: scopes.iter().map(|s| s.to_string()).collect(),
        not_before: None,
        expires_at: None,
        revoked: false,
    }
}

fn write_manifest(path: &std::path::Path, key: &SigningKey, tokens: Vec<TokenManifestToken>) {
    let unsigned = TokenManifestUnsigned {
        tokens: tokens.clone(),
    };
    let payload = canonical_signing_payload(&unsigned).expect("canonical payload");
    let signature = key.sign(&payload);
    let manifest = TokenManifestFile {
        tokens,
        signature: TokenManifestSignature {
            algorithm: "ed25519".to_string(),
            public_key_hex: hex::encode(key.verifying_key().to_bytes()),
            signature_hex: hex::encode(signature.to_bytes()),
        },
    };
    std::fs::write(
        path,
        serde_json::to_vec_pretty(&manifest).expect("serialize"),
    )
    .expect("write manifest");
}

/// A config whose admin API authenticates from a signed manifest.
fn config_with_manifest(dir: &std::path::Path, tokens: Vec<TokenManifestToken>) -> AppConfig {
    let key = SigningKey::from_bytes(&[0x42; 32]);
    let manifest_path = dir.join("tokens.json");
    write_manifest(&manifest_path, &key, tokens);

    let config_toml = format!(
        r#"api_version = "v1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc"
password = {{ env = "CDC_TEST_SOURCE_PASSWORD" }}
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 200
max_events_per_poll = 1000
table_include_list = ["public.orders"]
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "{state}"

[admin]
token_manifest_file = "{manifest}"
token_manifest_trusted_public_keys_hex = ["{pubkey}"]
# Auth fails closed if the manifest has not reloaded within this window.
token_manifest_max_staleness_ms = 300000
# Write-capable signalling requires an audit destination for the actions it accepts.
notification_log_file = "{notifications}"
"#,
        state = dir.join("state").display(),
        notifications = dir.join("notifications.jsonl").display(),
        manifest = manifest_path.display(),
        pubkey = hex::encode(key.verifying_key().to_bytes()),
    );

    let config_path = dir.join("cdc.toml");
    std::fs::write(&config_path, config_toml).expect("write config");
    rustcdc_server::config::load(&config_path).expect("config must load")
}

async fn probe(state: AdminState, mut request: Request<Body>) -> (StatusCode, String) {
    // The handlers extract `ConnectInfo<SocketAddr>` for rate limiting. In production
    // that comes from `into_make_service_with_connect_info`, which is a service
    // *factory* and cannot be driven by `oneshot`; injecting the extension gives the
    // extractor the same value without bypassing any of the layers under test.
    request.extensions_mut().insert(axum::extract::ConnectInfo(
        "203.0.113.7:54321"
            .parse::<SocketAddr>()
            .expect("peer addr"),
    ));

    let response = rustcdc_server::admin::router(state)
        .oneshot(request)
        .await
        .expect("router responds");
    let (status, body) = (response.status(), response.into_body());
    let bytes = axum::body::to_bytes(body, 1 << 20)
        .await
        .unwrap_or_default();
    (status, String::from_utf8_lossy(&bytes).to_string())
}

fn get(path: &str, authorization: Option<&str>) -> Request<Body> {
    let mut builder = Request::builder().uri(path).method("GET");
    if let Some(value) = authorization {
        builder = builder.header("Authorization", value);
    }
    builder.body(Body::empty()).expect("request builds")
}

// ─────────────────────────────────────────────────────────────────────────────
// Probes
// ─────────────────────────────────────────────────────────────────────────────

/// Baseline. Without this, every "is rejected" test below could pass because the
/// fixture is broken rather than because the control works.
#[tokio::test]
async fn a_valid_read_token_is_accepted() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = config_with_manifest(
        dir.path(),
        vec![
            manifest_token("reader", READ_TOKEN, &["read"]),
            manifest_token("writer", WRITE_TOKEN, &["write"]),
        ],
    );
    let state = AdminState::new(&config).await.expect("admin state");

    let (status, _) = probe(state, get("/status", Some(&format!("Bearer {READ_TOKEN}")))).await;
    assert_eq!(status, StatusCode::OK, "the control must actually work");
}

#[tokio::test]
async fn an_unauthenticated_request_is_rejected() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = config_with_manifest(
        dir.path(),
        vec![
            manifest_token("reader", READ_TOKEN, &["read"]),
            manifest_token("writer", WRITE_TOKEN, &["write"]),
        ],
    );
    let state = AdminState::new(&config).await.expect("admin state");

    for path in ["/status", "/metrics", "/notifications"] {
        let (status, body) = probe(state.clone(), get(path, None)).await;
        assert_eq!(
            status,
            StatusCode::UNAUTHORIZED,
            "{path} must require a token"
        );
        assert!(
            !body.contains("cdc_slot") && !body.contains("public.orders"),
            "an unauthorised response must not leak configuration: {body}"
        );
    }
}

/// A wrong token must be rejected, and the rejection must not describe *why*.
#[tokio::test]
async fn a_wrong_token_is_rejected_without_revealing_anything() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = config_with_manifest(
        dir.path(),
        vec![
            manifest_token("reader", READ_TOKEN, &["read"]),
            manifest_token("writer", WRITE_TOKEN, &["write"]),
        ],
    );
    let state = AdminState::new(&config).await.expect("admin state");

    for candidate in [
        "Bearer wrong-token",
        "Bearer ",
        "Bearer",
        // Right value, wrong scheme.
        &format!("Basic {READ_TOKEN}"),
        &format!("bearer{READ_TOKEN}"),
        // Leading/trailing whitespace must not be normalised into a match.
        &format!("Bearer  {READ_TOKEN} "),
        // A prefix of a valid token must not match.
        &format!("Bearer {}", &READ_TOKEN[..10]),
    ] {
        let (status, body) = probe(state.clone(), get("/status", Some(candidate))).await;
        assert_eq!(
            status,
            StatusCode::UNAUTHORIZED,
            "credential {candidate:?} must be rejected"
        );
        assert!(
            !body.contains(READ_TOKEN) && !body.contains(WRITE_TOKEN),
            "the rejection must not echo a token back: {body}"
        );
    }
}

/// **Privilege escalation.** A read-scoped token must not reach a write endpoint.
#[tokio::test]
async fn a_read_scoped_token_cannot_reach_a_write_endpoint() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = config_with_manifest(
        dir.path(),
        vec![
            manifest_token("reader", READ_TOKEN, &["read"]),
            manifest_token("writer", WRITE_TOKEN, &["write"]),
        ],
    );
    let state = AdminState::new(&config).await.expect("admin state");

    let request = Request::builder()
        .uri("/signals")
        .method("POST")
        .header("Authorization", format!("Bearer {READ_TOKEN}"))
        .header("Content-Type", "application/json")
        .body(Body::from(r#"{"action":"pause"}"#))
        .expect("request builds");

    let (status, _) = probe(state, request).await;
    assert_ne!(
        status,
        StatusCode::OK,
        "a read token must not be able to control the pipeline"
    );
}

/// Write implies read — deliberate, and worth pinning so it is not lost by accident.
#[tokio::test]
async fn a_write_scoped_token_may_read() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = config_with_manifest(
        dir.path(),
        vec![
            manifest_token("reader", READ_TOKEN, &["read"]),
            manifest_token("writer", WRITE_TOKEN, &["write"]),
        ],
    );
    let state = AdminState::new(&config).await.expect("admin state");

    let (status, _) = probe(
        state,
        get("/status", Some(&format!("Bearer {WRITE_TOKEN}"))),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn a_revoked_token_is_rejected() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut revoked = manifest_token("reader", READ_TOKEN, &["read"]);
    revoked.revoked = true;
    let config = config_with_manifest(
        dir.path(),
        vec![revoked, manifest_token("writer", WRITE_TOKEN, &["write"])],
    );
    let state = AdminState::new(&config).await.expect("admin state");

    let (status, _) = probe(state, get("/status", Some(&format!("Bearer {READ_TOKEN}")))).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn an_expired_token_is_rejected() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut expired = manifest_token("reader", READ_TOKEN, &["read"]);
    expired.expires_at = Some(chrono::Utc::now() - chrono::Duration::hours(1));
    let config = config_with_manifest(
        dir.path(),
        vec![expired, manifest_token("writer", WRITE_TOKEN, &["write"])],
    );
    let state = AdminState::new(&config).await.expect("admin state");

    let (status, _) = probe(state, get("/status", Some(&format!("Bearer {READ_TOKEN}")))).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn a_token_that_is_not_yet_valid_is_rejected() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut future = manifest_token("reader", READ_TOKEN, &["read"]);
    future.not_before = Some(chrono::Utc::now() + chrono::Duration::hours(1));
    let config = config_with_manifest(
        dir.path(),
        vec![future, manifest_token("writer", WRITE_TOKEN, &["write"])],
    );
    let state = AdminState::new(&config).await.expect("admin state");

    let (status, _) = probe(state, get("/status", Some(&format!("Bearer {READ_TOKEN}")))).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

// ─────────────────────────────────────────────────────────────────────────────
// Manifest trust
// ─────────────────────────────────────────────────────────────────────────────

/// A manifest signed by a key that is not trusted must not load.
///
/// This is the whole point of the signature: without it, anyone who can write the
/// manifest file can mint themselves a write-scoped token.
#[tokio::test]
async fn a_manifest_signed_by_an_untrusted_key_is_refused() {
    let dir = tempfile::tempdir().expect("tempdir");

    let trusted = SigningKey::from_bytes(&[0x42; 32]);
    let attacker = SigningKey::from_bytes(&[0x99; 32]);
    let manifest_path = dir.path().join("tokens.json");
    write_manifest(
        &manifest_path,
        &attacker,
        vec![manifest_token("attacker", WRITE_TOKEN, &["write"])],
    );

    let trusted_keys =
        rustcdc_server::token_manifest_policy::parse_trusted_manifest_keys(&[hex::encode(
            trusted.verifying_key().to_bytes(),
        )])
        .expect("trusted key parses");

    let err = rustcdc_server::token_manifest_policy::load_signed_token_manifest(
        &manifest_path,
        &trusted_keys,
    )
    .expect_err("a manifest signed by an untrusted key must be refused");
    assert!(err.contains("untrusted"), "{err}");
}

/// Tampering with the token list after signing must invalidate the signature.
#[tokio::test]
async fn a_tampered_manifest_is_refused() {
    let dir = tempfile::tempdir().expect("tempdir");
    let key = SigningKey::from_bytes(&[0x42; 32]);
    let manifest_path = dir.path().join("tokens.json");
    write_manifest(
        &manifest_path,
        &key,
        vec![manifest_token("reader", READ_TOKEN, &["read"])],
    );

    // Escalate the scope in place, leaving the signature untouched.
    let raw = std::fs::read_to_string(&manifest_path).expect("read");
    let tampered = raw.replace(r#""read""#, r#""write""#);
    assert_ne!(raw, tampered, "the tamper must actually change the file");
    std::fs::write(&manifest_path, tampered).expect("write");

    let trusted_keys =
        rustcdc_server::token_manifest_policy::parse_trusted_manifest_keys(&[hex::encode(
            key.verifying_key().to_bytes(),
        )])
        .expect("trusted key parses");

    rustcdc_server::token_manifest_policy::load_signed_token_manifest(
        &manifest_path,
        &trusted_keys,
    )
    .expect_err("a tampered manifest must fail signature verification");
}

/// An unsigned or wrong-algorithm manifest must not be accepted.
#[tokio::test]
async fn a_manifest_with_a_substituted_algorithm_is_refused() {
    let dir = tempfile::tempdir().expect("tempdir");
    let key = SigningKey::from_bytes(&[0x42; 32]);
    let manifest_path = dir.path().join("tokens.json");
    write_manifest(
        &manifest_path,
        &key,
        vec![manifest_token("reader", READ_TOKEN, &["read"])],
    );

    let raw = std::fs::read_to_string(&manifest_path).expect("read");
    // "none" is the classic JWT-family downgrade; the manifest must not honour it.
    std::fs::write(&manifest_path, raw.replace("ed25519", "none")).expect("write");

    let trusted_keys =
        rustcdc_server::token_manifest_policy::parse_trusted_manifest_keys(&[hex::encode(
            key.verifying_key().to_bytes(),
        )])
        .expect("trusted key parses");

    let err = rustcdc_server::token_manifest_policy::load_signed_token_manifest(
        &manifest_path,
        &trusted_keys,
    )
    .expect_err("an algorithm downgrade must be refused");
    assert!(err.contains("unsupported signature algorithm"), "{err}");
}

// ─────────────────────────────────────────────────────────────────────────────
// Configuration trust boundaries
// ─────────────────────────────────────────────────────────────────────────────

/// A non-loopback admin bind must require both tokens and TLS.
///
/// Bearer tokens over plaintext HTTP are credentials on the wire.
#[test]
fn a_non_loopback_admin_bind_requires_tokens_and_tls() {
    let dir = tempfile::tempdir().expect("tempdir");

    let base = |admin: &str| {
        format!(
            r#"api_version = "v1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc"
password = {{ env = "CDC_TEST_SOURCE_PASSWORD" }}
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 200
max_events_per_poll = 1000
table_include_list = ["public.orders"]
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "{state}"

{admin}
"#,
            state = dir.path().join("state").display(),
        )
    };

    // Tokens but no TLS.
    let path = dir.path().join("no-tls.toml");
    std::fs::write(
        &path,
        base(
            r#"[admin]
bind = "0.0.0.0:8080"
read_token_env  = "CDC_TEST_SOURCE_PASSWORD"
write_token_env = "CDC_TEST_SOURCE_PASSWORD"
notification_log_file = "/tmp/rustcdc-security-probe-notifications.jsonl""#,
        ),
    )
    .expect("write");
    let err = rustcdc_server::config::load(&path).expect_err("plaintext admin must be refused");
    assert!(err.to_string().contains("admin.tls"), "{err}");

    // No tokens at all.
    let path = dir.path().join("no-tokens.toml");
    std::fs::write(
        &path,
        base(
            r#"[admin]
bind = "0.0.0.0:8080""#,
        ),
    )
    .expect("write");
    let err =
        rustcdc_server::config::load(&path).expect_err("unauthenticated admin must be refused");
    assert!(err.to_string().contains("token"), "{err}");
}

/// Credentials must never be accepted as literals where a reference is required.
#[test]
fn credential_literals_are_refused_at_load() {
    let dir = tempfile::tempdir().expect("tempdir");

    let write = |name: &str, body: &str| {
        let path = dir.path().join(name);
        std::fs::write(&path, body).expect("write");
        path
    };

    let source_literal = write(
        "literal-source.toml",
        &format!(
            r#"api_version = "v1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc"
password = "hunter2"
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 200
max_events_per_poll = 1000
table_include_list = ["public.orders"]
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "{}"
"#,
            dir.path().join("state").display()
        ),
    );
    let err = rustcdc_server::config::load(&source_literal)
        .expect_err("a literal replication credential must be refused");
    assert!(err.to_string().contains("deferred secret"), "{err}");

    let url_userinfo = write(
        "url-userinfo.toml",
        &format!(
            r#"api_version = "v1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc"
password = {{ env = "CDC_TEST_SOURCE_PASSWORD" }}
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 200
max_events_per_poll = 1000
table_include_list = ["public.orders"]
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "http"
url  = "https://admin:hunter2@api.example.com/events"

[state]
dir = "{}"
"#,
            dir.path().join("state").display()
        ),
    );
    let err = rustcdc_server::config::load(&url_userinfo)
        .expect_err("a credential in a sink URL must be refused");
    assert!(err.to_string().contains("credentials"), "{err}");
}

// ─────────────────────────────────────────────────────────────────────────────
// Redaction
// ─────────────────────────────────────────────────────────────────────────────

/// No credential from a real config reaches `/config`, whichever carrier it uses.
///
/// Four carriers in one config, because they are covered by four different rules and a
/// regression in any one of them is a credential disclosure to every read-scoped token:
///
/// * a `SecretString` (`source.password`) — redacted by its own `Serialize`
/// * a `SecretString` behind a differently-named field (`sink.bearer_token`)
/// * a header map entry (`sink.headers.authorization`, `x-api-key`) — redacted by name
/// * a **URL query parameter** (`sink.http.url?api_key=`) — redacted by value
///
/// The last is the one that was open: `sink.http.url` matches no enumerated path and no
/// secret-looking key name, and since the loader rejects userinfo in that field outright,
/// the query string is the only way a credential can actually get there — which is also
/// how webhook and ingest endpoints normally carry one.
#[tokio::test]
async fn no_secret_from_a_real_config_reaches_the_config_endpoint() {
    const SENTINEL: &str = "sentinel-secret-9f3a7c";

    let dir = tempfile::tempdir().expect("tempdir");
    let config_path = dir.path().join("cdc.toml");
    std::fs::write(
        &config_path,
        format!(
            r#"api_version = "v1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc"
password = {{ env = "CDC_TEST_SENTINEL_SECRET" }}
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 200
max_events_per_poll = 1000
table_include_list = ["public.orders"]
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "http"
url  = "https://api.example.com/events?region=eu&api_key={SENTINEL}"
bearer_token = {{ env = "CDC_TEST_SENTINEL_SECRET" }}

[sink.headers]
authorization = "Bearer {SENTINEL}"
x-api-key     = "{SENTINEL}"

[state]
dir = "{state}"

[admin]
read_token_env = "CDC_TEST_STATUS_READ_TOKEN"
"#,
            state = dir.path().join("state").display(),
        ),
    )
    .expect("write config");

    // **The token is the point.** This probe used to be unauthenticated, on the reasoning
    // that a loopback bind with no tokens leaves `/status` open. It does not — read scope
    // fails closed when no read token is configured — so the response was the four-byte
    // string `unauthorized`, and "no secret appears in the body" held trivially. The test
    // named after redaction never exercised redaction once. The legibility assertion below
    // is what exposed that, and is why it is here rather than being merely nice to have.
    let _env = rustcdc_server::test_env::EnvGuard::set(&[(
        "CDC_TEST_STATUS_READ_TOKEN",
        "status-read-token",
    )]);
    let config = rustcdc_server::config::load(&config_path).expect("config loads");
    let state = AdminState::new(&config).await.expect("admin state");

    let (status, body) = probe(state, get("/config", Some("Bearer status-read-token"))).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "the probe must actually reach the handler, or this asserts nothing: {body}"
    );
    assert!(
        !body.contains(SENTINEL),
        "a secret reached the config endpoint. Redaction is enumerate-by-name, so a \
         newly added credential field is invisible to it until someone remembers to \
         add it:\n{body}"
    );

    // Redaction must stay legible, or operators stop reading the snapshot and it stops
    // being a diagnostic. The host and the non-secret query parameter survive.
    assert!(
        body.contains("api.example.com") && body.contains("region=eu"),
        "redaction over-reached: the snapshot must still identify the endpoint:\n{body}"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Rate limiting
// ─────────────────────────────────────────────────────────────────────────────

/// The configured burst must actually be available to a client that has not been seen
/// before.
///
/// This suite found the opposite: every new client was admitted with exactly **one**
/// token regardless of `admin.status_rate_limit_burst`, so a handful of legitimate
/// back-to-back requests returned `429`. The anti-amplification rationale was sound but
/// applied unconditionally; it is now applied only when the client-key table is under
/// the pressure that makes IP-rotation amplification possible.
///
/// Note this returns 429 *before* authentication, which is deliberate — an
/// unauthenticated flood should be cheap to refuse — and is why this probe uses a valid
/// token: it is testing the limiter, not the auth.
#[tokio::test]
async fn a_new_client_receives_the_configured_burst_not_a_single_token() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = config_with_manifest(
        dir.path(),
        vec![
            manifest_token("reader", READ_TOKEN, &["read"]),
            manifest_token("writer", WRITE_TOKEN, &["write"]),
        ],
    );
    let state = AdminState::new(&config).await.expect("admin state");

    // Well within the default status burst (40) and far more than one.
    for attempt in 0..8 {
        let (status, _) = probe(
            state.clone(),
            get("/status", Some(&format!("Bearer {READ_TOKEN}"))),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::OK,
            "request {attempt} was refused; a first-time client must get the burst the \
             operator configured, not a single token"
        );
    }
}

/// Rate limiting must be per-client, so one noisy peer cannot deny service to others.
///
/// A globally-keyed limiter would let a single unauthenticated attacker lock every
/// legitimate scraper out — denial of service with no credentials at all.
#[tokio::test]
async fn one_peer_exhausting_its_budget_does_not_affect_another() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = config_with_manifest(
        dir.path(),
        vec![
            manifest_token("reader", READ_TOKEN, &["read"]),
            manifest_token("writer", WRITE_TOKEN, &["write"]),
        ],
    );
    let state = AdminState::new(&config).await.expect("admin state");

    let noisy: SocketAddr = "198.51.100.9:1234".parse().expect("addr");
    let quiet: SocketAddr = "198.51.100.10:1234".parse().expect("addr");

    // Burn well past the burst from one peer.
    for _ in 0..60 {
        let mut request = get("/status", Some(&format!("Bearer {READ_TOKEN}")));
        request
            .extensions_mut()
            .insert(axum::extract::ConnectInfo(noisy));
        let _ = rustcdc_server::admin::router(state.clone())
            .oneshot(request)
            .await
            .expect("router responds");
    }

    let mut request = get("/status", Some(&format!("Bearer {READ_TOKEN}")));
    request
        .extensions_mut()
        .insert(axum::extract::ConnectInfo(quiet));
    let response = rustcdc_server::admin::router(state)
        .oneshot(request)
        .await
        .expect("router responds");

    assert_eq!(
        response.status(),
        StatusCode::OK,
        "a second peer must be unaffected by the first exhausting its budget — a \
         globally-keyed limiter would be an unauthenticated denial of service"
    );
}
