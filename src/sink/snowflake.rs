//! Snowflake, via the Snowpipe Streaming high-performance REST API.
//!
//! # Exactly-once without a Kafka transaction
//!
//! A channel carries an **offset token**: a string attached to a batch, which Snowflake
//! persists once those rows commit and returns when the channel is reopened. That is the
//! same contract as the checkpoint store, enforced destination-side — so after a crash this
//! sink can ask what Snowflake already has instead of guessing.
//!
//! # Why `flush` waits
//!
//! `Append Rows` returning `200` means Snowflake *buffered* the rows, and reopening a
//! channel **discards uncommitted buffered rows**. A flush that returned on the append would
//! let the pipeline checkpoint past rows that vanish on the next restart. So `flush` appends
//! and then waits for `last_committed_offset_token` to reach the batch, bounded by
//! `commit_timeout_ms` — which the loader keeps below `runtime.sink_flush_timeout_ms`, or
//! the runtime cancels the wait after the rows are sent.
//!
//! # Resume
//!
//! `checkpoint ≤ committed token` therefore always holds. They diverge in one direction: a
//! crash between a committed flush and the checkpoint write leaves Snowflake ahead, so
//! [`ResumeFilter`] skips replayed rows up to and including the committed token.
//!
//! # Authentication
//!
//! Every method reduces to a bearer credential plus an `X-Snowflake-Authorization-Token-Type`
//! label, exchanged once at `POST /oauth/token` for a scoped token. Omitting the label is not
//! neutral — Snowflake then assumes `OAUTH`.

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use reqwest::header::AUTHORIZATION;

/// Names the credential in the `Authorization` header. Optional per the docs, and always
/// sent: without it Snowflake assumes `OAUTH`.
const TOKEN_TYPE_HEADER: &str = "X-Snowflake-Authorization-Token-Type";

use rustcdc::core::{Error as RtError, Event};
use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::config::schema::SnowflakeSinkConfig;
use crate::config::sink::SnowflakeAuthConfig;

/// How long a minted JWT claims to be valid.
///
/// Snowflake caps this at one hour. Shorter is not safer here — the JWT is only ever sent
/// to the token endpoint — but a long-lived credential in memory is worth bounding anyway.
const JWT_LIFETIME: Duration = Duration::from_secs(3_600);

/// Re-mint the scoped token this long before it expires.
///
/// A token that expires mid-flush turns a durability wait into a 401, and the flush fails
/// for a reason that has nothing to do with the data.
const SCOPED_TOKEN_REFRESH_MARGIN: Duration = Duration::from_secs(300);

/// Snowflake's documented lifetime for a scoped token.
const SCOPED_TOKEN_LIFETIME: Duration = Duration::from_secs(3_600);

/// The error code Snowflake returns when a continuation token's sequencer is stale.
///
/// It means another writer opened the channel, or ours lapsed. The only recovery is to
/// reopen — which also tells us what is actually committed.
const STALE_SEQUENCER: &str = "STALE_CONTINUATION_TOKEN_SEQUENCER";

// ─────────────────────────────────────────────────────────────────────────────
// Wire types
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct HostnameResponse {
    hostname: String,
}

#[derive(Debug, Deserialize)]
struct OpenChannelResponse {
    next_continuation_token: String,
    channel_status: ChannelStatus,
}

#[derive(Debug, Deserialize)]
struct AppendRowsResponse {
    next_continuation_token: String,
}

/// Only the fields this sink acts on.
///
/// `rows_inserted` and `rows_parsed` are in the response and deliberately absent here: an
/// unused field is a claim that something reads it. The counts that matter operationally
/// come from the sink's own accounting, which is what the Prometheus families expose.
#[derive(Debug, Clone, Deserialize, Default)]
struct ChannelStatus {
    #[serde(default)]
    channel_status_code: String,
    #[serde(default)]
    last_committed_offset_token: Option<String>,
    #[serde(default)]
    rows_error_count: u64,
    #[serde(default)]
    last_error_message: Option<String>,
}

#[derive(Debug, Deserialize)]
struct BulkChannelStatusResponse {
    channel_statuses: std::collections::HashMap<String, ChannelStatus>,
}

#[derive(Debug, Deserialize, Default)]
struct ApiError {
    #[serde(default)]
    code: String,
    #[serde(default)]
    message: String,
}

// ─────────────────────────────────────────────────────────────────────────────
// Resume filter
// ─────────────────────────────────────────────────────────────────────────────

/// Skips events Snowflake already committed, after a crash between commit and checkpoint.
///
/// Matches by offset-string **equality**, not ordering: source offsets are opaque
/// per-connector strings, and the runtime replays in order from at-or-before the committed
/// token — so "skip until the token, then stream" needs no parser.
#[derive(Debug)]
pub(crate) enum ResumeFilter {
    /// Nothing committed, or the resume window is finished.
    Streaming,
    /// Skipping until `token` is seen.
    Skipping {
        token: String,
        scanned: u64,
        limit: u64,
    },
}

/// What [`ResumeFilter::admit`] decided about one event.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Admission {
    /// Deliver it.
    Deliver,
    /// Snowflake already has it.
    Skip,
    /// The scan window was exhausted without matching. Deliver, and say so.
    DeliverAfterExhaustedScan,
}

impl ResumeFilter {
    pub(crate) fn new(committed: Option<String>, limit: u64) -> Self {
        match committed {
            Some(token) if !token.is_empty() => Self::Skipping {
                token,
                scanned: 0,
                limit,
            },
            _ => Self::Streaming,
        }
    }

    pub(crate) fn admit(&mut self, offset: &str) -> Admission {
        let Self::Skipping {
            token,
            scanned,
            limit,
        } = self
        else {
            return Admission::Deliver;
        };

        // The matching event is itself already committed, so it is skipped and everything
        // after it is delivered.
        if offset == token {
            *self = Self::Streaming;
            return Admission::Skip;
        }

        *scanned += 1;
        if *scanned >= *limit {
            *self = Self::Streaming;
            return Admission::DeliverAfterExhaustedScan;
        }

        Admission::Skip
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Sink
// ─────────────────────────────────────────────────────────────────────────────

/// Counters this sink publishes through `SinkDeliveryMetrics`.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct SnowflakeAccounting {
    pub(crate) rows_appended: u64,
    pub(crate) rows_skipped_on_resume: u64,
    pub(crate) append_requests: u64,
    pub(crate) channel_reopens: u64,
    pub(crate) commit_waits: u64,
    pub(crate) commit_wait_ms_total: u64,
    pub(crate) retries: u64,
    pub(crate) resume_scan_exhausted: u64,
}

pub struct SnowflakeSink {
    client: reqwest::Client,
    config: SnowflakeSinkConfig,
    auth: SnowflakeAuthConfig,
    /// `runtime.max_event_bytes`, enforced on the NDJSON row this sink actually sends.
    max_event_bytes: usize,
    /// `https`, or `http` for a loopback fake. Decided once from the configured URL.
    scheme: &'static str,
    /// Ingest hostname, from `GET /v2/streaming/hostname`. Falls back to the account URL's
    /// host when the endpoint is unavailable.
    host: String,
    scoped_token: Option<(String, Instant)>,
    continuation_token: Option<String>,
    resume: ResumeFilter,
    /// Buffered NDJSON rows and the offset of the last one, which becomes the batch's
    /// `endOffsetToken`.
    pending: Vec<u8>,
    pending_rows: usize,
    pending_end_offset: Option<String>,
    pending_since: Option<Instant>,
    /// End offset of an append whose commit this process never saw confirmed.
    ///
    /// Set before the wait, so a `flush` future dropped by the runtime's own
    /// `sink_flush_timeout_ms` still leaves the marker — without which that cancellation
    /// produced duplicate rows on the retry.
    unconfirmed_end_offset: Option<String>,
    accounting: SnowflakeAccounting,
    closed: bool,
}

impl SnowflakeSink {
    pub async fn new(
        config: &SnowflakeSinkConfig,
        max_event_bytes: usize,
    ) -> Result<Self, crate::error::AppError> {
        config.validate().map_err(crate::error::AppError::Other)?;

        let is_https = config.account_url.trim().starts_with("https://");
        let client = reqwest::Client::builder()
            .min_tls_version(reqwest::tls::Version::TLS_1_2)
            // `https_only` follows the configured scheme, and the loader has already refused
            // plaintext for anything but loopback — so this cannot be relaxed by config.
            .https_only(is_https)
            .timeout(Duration::from_millis(config.request_timeout_ms))
            .build()
            .map_err(|e| {
                crate::error::AppError::Other(format!("failed to build Snowflake HTTP client: {e}"))
            })?;

        let host = config
            .account_url
            .trim()
            .trim_end_matches('/')
            .trim_start_matches("https://")
            .trim_start_matches("http://")
            .to_string();

        Ok(Self {
            client,
            auth: config.auth.clone(),
            config: config.clone(),
            max_event_bytes,
            scheme: if is_https { "https" } else { "http" },
            host,
            scoped_token: None,
            continuation_token: None,
            resume: ResumeFilter::Streaming,
            pending: Vec::new(),
            pending_rows: 0,
            pending_end_offset: None,
            pending_since: None,
            unconfirmed_end_offset: None,
            accounting: SnowflakeAccounting::default(),
            closed: false,
        })
    }

    pub(crate) fn accounting(&self) -> SnowflakeAccounting {
        self.accounting
    }

    fn base(&self) -> String {
        format!("{}://{}", self.scheme, self.host)
    }

    fn channel_path(&self) -> String {
        format!(
            "{}/v2/streaming/databases/{}/schemas/{}/pipes/{}/channels/{}",
            self.base(),
            self.config.database,
            self.config.schema,
            self.config.pipe,
            self.config.channel
        )
    }

    fn rows_path(&self) -> String {
        format!(
            "{}/v2/streaming/data/databases/{}/schemas/{}/pipes/{}/channels/{}/rows",
            self.base(),
            self.config.database,
            self.config.schema,
            self.config.pipe,
            self.config.channel
        )
    }

    // ── Authentication ───────────────────────────────────────────────────────

    /// A scoped token, minted if absent or close to expiry.
    async fn scoped_token(&mut self) -> Result<String, RtError> {
        if let Some((token, expires_at)) = &self.scoped_token
            && *expires_at > Instant::now() + SCOPED_TOKEN_REFRESH_MARGIN
        {
            return Ok(token.clone());
        }

        // The credential travels in `Authorization`, **not** in the form body. This is the
        // one shape all four of Snowflake's REST auth methods share, and getting it wrong
        // is silent: an `assertion=` form parameter is simply ignored, and the request is
        // then an unauthenticated one that fails with a message about the token.
        let credential = self.credential().await?;
        let response = self
            .client
            .post(format!("{}/oauth/token", self.base()))
            .header(AUTHORIZATION, format!("Bearer {credential}"))
            // Documented as optional, sent anyway: without it Snowflake *guesses*, and it
            // guesses `OAUTH`. A key-pair JWT judged as an OAuth token is rejected with an
            // error about the wrong thing entirely.
            .header(TOKEN_TYPE_HEADER, self.auth.token_type())
            .form(&[
                ("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer"),
                ("scope", self.host.as_str()),
            ])
            .send()
            .await
            .map_err(|e| RtError::SourceError(format!("Snowflake token exchange failed: {e}")))?;

        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        if !status.is_success() {
            // The body of a token-exchange failure can echo request parameters, so it is
            // summarised rather than logged whole: the JWT is in the request.
            return Err(RtError::ConfigError(format!(
                "Snowflake token exchange returned {status}; check sink.snowflake.account, \
                 .user and the registered public key (RSA_PUBLIC_KEY_FP)"
            )));
        }

        // The endpoint returns a bare token string, not JSON.
        let token = body.trim().trim_matches('"').to_string();
        if token.is_empty() {
            return Err(RtError::SourceError(
                "Snowflake token exchange returned an empty token".to_string(),
            ));
        }
        self.scoped_token = Some((token.clone(), Instant::now() + SCOPED_TOKEN_LIFETIME));
        Ok(token)
    }

    /// The `Authorization: Bearer …` value for the configured method.
    ///
    /// Every method reduces to a bearer string plus a token-type label; that is what makes
    /// key pairs, programmatic access tokens and workload identity federation the same
    /// three lines at the call site instead of three exchange flows.
    async fn credential(&self) -> Result<String, RtError> {
        match &self.auth {
            SnowflakeAuthConfig::KeyPair { .. } => self.mint_jwt(),
            SnowflakeAuthConfig::ProgrammaticAccessToken { token } => token
                .resolve()
                .map_err(|e| RtError::ConfigError(format!("sink.snowflake.auth.token: {e}"))),
            SnowflakeAuthConfig::WorkloadIdentity {
                provider,
                token_file,
            } => {
                // Re-read every time. These attestations are short-lived by design and the
                // platform rewrites the file in place — Kubernetes refreshes a projected
                // service-account token at 80 % of its lifetime. Caching one at startup
                // works for an hour and then looks like an outage.
                let token = tokio::fs::read_to_string(token_file).await.map_err(|e| {
                    RtError::ConfigError(format!(
                        "sink.snowflake.auth.token_file '{}' could not be read: {e}",
                        token_file.display()
                    ))
                })?;
                let token = token.trim();
                if token.is_empty() {
                    return Err(RtError::ConfigError(format!(
                        "sink.snowflake.auth.token_file '{}' is empty",
                        token_file.display()
                    )));
                }
                Ok(format!("WIF.{}.{token}", provider.as_wire()))
            }
        }
    }

    /// The decoded signing key, or a clear error about which half is wrong.
    fn private_key(&self) -> Result<rsa::RsaPrivateKey, RtError> {
        let SnowflakeAuthConfig::KeyPair {
            private_key,
            passphrase,
        } = &self.auth
        else {
            return Err(RtError::ConfigError(
                "a key-pair JWT was requested for a sink configured with a different \
                 authentication method"
                    .to_string(),
            ));
        };
        let pem = private_key
            .resolve()
            .map_err(|e| RtError::ConfigError(format!("sink.snowflake.auth.private_key: {e}")))?;
        let passphrase = match passphrase {
            Some(secret) => Some(secret.resolve().map_err(|e| {
                RtError::ConfigError(format!("sink.snowflake.auth.passphrase: {e}"))
            })?),
            None => None,
        };
        decode_private_key(&pem, passphrase.as_deref())
    }

    /// Build and sign the key-pair JWT.
    ///
    /// `iss` is `<ACCOUNT>.<USER>.SHA256:<base64 fingerprint of the DER public key>` and
    /// `sub` is `<ACCOUNT>.<USER>`, both upper-cased — Snowflake matches the fingerprint
    /// against the user's registered `RSA_PUBLIC_KEY_FP`, and a lower-cased account or user
    /// fails with an error that names neither.
    fn mint_jwt(&self) -> Result<String, RtError> {
        let private = self.private_key()?;
        let qualified = format!(
            "{}.{}",
            self.config.account.trim().to_ascii_uppercase(),
            self.config.user.trim().to_ascii_uppercase()
        );
        let fingerprint = public_key_fingerprint(&private)?;

        let issued_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| RtError::SourceError(format!("system clock is before the epoch: {e}")))?
            .as_secs();

        // An RS256 JWT is `base64url(header).base64url(claims).base64url(signature)`, the
        // signature being PKCS#1 v1.5 over SHA-256 of the first two joined by a dot. That
        // is three lines with the `rsa` crate already required for the fingerprint, so no
        // JWT library — and no second crypto backend — is needed.
        use base64::Engine as _;
        use rsa::pkcs1v15::SigningKey;
        use rsa::signature::{SignatureEncoding, Signer as _};

        let b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD;
        let header = b64.encode(br#"{"alg":"RS256","typ":"JWT"}"#);
        let claims = b64.encode(
            serde_json::json!({
                "iss": format!("{qualified}.{fingerprint}"),
                "sub": qualified,
                "iat": issued_at,
                "exp": issued_at + JWT_LIFETIME.as_secs(),
            })
            .to_string(),
        );
        let signing_input = format!("{header}.{claims}");

        // `rsa`'s own re-export, not the crate-level `sha2`: this project is on sha2 0.11
        // and `rsa 0.9` is on 0.10, so the `Digest` traits are different types. The
        // fingerprint above uses the project's sha2 because it only needs bytes out.
        let signature =
            SigningKey::<rsa::sha2::Sha256>::new(private).sign(signing_input.as_bytes());

        Ok(format!(
            "{signing_input}.{}",
            b64.encode(signature.to_bytes())
        ))
    }

    // ── Channel lifecycle ────────────────────────────────────────────────────

    /// Open (or reopen) the channel and adopt whatever Snowflake says is committed.
    ///
    /// Called at preflight and again whenever a continuation token goes stale. Reopening is
    /// also what discards Snowflake-side uncommitted rows, so the committed token this
    /// returns is the true durable position — which is exactly why `flush` must not report
    /// success before that token has moved.
    async fn open_channel(&mut self) -> Result<(), RtError> {
        let token = self.scoped_token().await?;
        let response = self
            .client
            .put(self.channel_path())
            .bearer_auth(token)
            .json(&serde_json::json!({}))
            .send()
            .await
            .map_err(|e| RtError::SourceError(format!("Snowflake open channel failed: {e}")))?;

        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        if !status.is_success() {
            return Err(classify_api_error(status, &body, "open channel"));
        }

        let opened: OpenChannelResponse = serde_json::from_str(&body).map_err(|e| {
            RtError::SourceError(format!(
                "Snowflake open channel returned unreadable JSON: {e}"
            ))
        })?;

        let committed = opened.channel_status.last_committed_offset_token.clone();
        tracing::info!(
            channel = %self.config.channel,
            status = %opened.channel_status.channel_status_code,
            committed_offset_token = committed.as_deref().unwrap_or("<none>"),
            "Snowflake channel opened"
        );

        self.continuation_token = Some(opened.next_continuation_token);
        self.resume = ResumeFilter::new(committed, self.config.resume_scan_max_events);
        self.accounting.channel_reopens += 1;
        Ok(())
    }

    async fn channel_status(&mut self) -> Result<ChannelStatus, RtError> {
        let token = self.scoped_token().await?;
        let url = format!(
            "{}/v2/streaming/databases/{}/schemas/{}/pipes/{}:bulk-channel-status",
            self.base(),
            self.config.database,
            self.config.schema,
            self.config.pipe
        );
        let response = self
            .client
            .post(url)
            .bearer_auth(token)
            .json(&serde_json::json!({ "channel_names": [self.config.channel] }))
            .send()
            .await
            .map_err(|e| RtError::SourceError(format!("Snowflake channel status failed: {e}")))?;

        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        if !status.is_success() {
            return Err(classify_api_error(status, &body, "channel status"));
        }

        let parsed: BulkChannelStatusResponse = serde_json::from_str(&body).map_err(|e| {
            RtError::SourceError(format!(
                "Snowflake channel status returned unreadable JSON: {e}"
            ))
        })?;
        parsed
            .channel_statuses
            .get(&self.config.channel)
            .cloned()
            .ok_or_else(|| {
                RtError::SourceError(format!(
                    "Snowflake channel status omitted channel '{}'",
                    self.config.channel
                ))
            })
    }

    // ── Delivery ─────────────────────────────────────────────────────────────

    /// Buffer one event as an NDJSON row, unless resume says Snowflake already has it.
    pub(crate) async fn append_event(&mut self, event: &Event) -> Result<(), RtError> {
        if self.closed {
            return Err(RtError::StateError("Snowflake sink is closed".to_string()));
        }

        // An append this process never saw confirmed must be reconciled *before* anything
        // is buffered, because the resume filter runs on the way in. Reconciling at flush
        // time would be too late: the rows would already be queued for a second append.
        self.reconcile_unconfirmed_append().await?;

        match self.resume.admit(&event.source.offset) {
            crate::sink::snowflake::Admission::Skip => {
                self.accounting.rows_skipped_on_resume += 1;
                return Ok(());
            }
            crate::sink::snowflake::Admission::DeliverAfterExhaustedScan => {
                self.accounting.resume_scan_exhausted += 1;
                tracing::warn!(
                    channel = %self.config.channel,
                    scanned = self.config.resume_scan_max_events,
                    "Snowflake resume scan was exhausted without matching the committed \
                     offset token; delivering from here, so this window is at-least-once. \
                     Raise sink.snowflake.resume_scan_max_events if the source replays more \
                     than this after a crash."
                );
            }
            crate::sink::snowflake::Admission::Deliver => {}
        }

        let mut row = serde_json::to_vec(event).map_err(|e| {
            RtError::SourceError(format!("failed to encode an event as NDJSON: {e}"))
        })?;
        row.push(b'\n');

        // Enforced here, on the bytes that go on the wire, rather than by `SinkBinding` on a
        // second rendering made only to be measured. Exact and free — the row already
        // exists.
        if row.len() > self.max_event_bytes {
            return Err(RtError::ConfigError(format!(
                "encoded event payload size {} exceeds runtime.max_event_bytes {}",
                row.len(),
                self.max_event_bytes
            )));
        }

        // Flush *before* adding the row that would breach the limit, not after: the API
        // rejects an over-sized body outright, and one 4 MiB event cannot be split.
        if !self.pending.is_empty() && self.pending.len() + row.len() > self.config.batch_max_bytes
        {
            self.flush_pending().await?;
        }

        self.pending.extend_from_slice(&row);
        self.pending_rows += 1;
        self.pending_end_offset = Some(event.source.offset.clone());
        self.pending_since.get_or_insert_with(Instant::now);

        if self.pending_rows >= self.config.batch_max_rows
            || self.pending.len() >= self.config.batch_max_bytes
        {
            self.flush_pending().await?;
        }
        Ok(())
    }

    /// Re-arm the resume filter from the committed offset token after an unconfirmed append.
    ///
    /// Reached when `await_commit` failed, or when the whole `flush` future was dropped by
    /// the runtime's timeout so no error path ran. Either way the batch is retried, and
    /// asking Snowflake is the only way to know what landed.
    async fn reconcile_unconfirmed_append(&mut self) -> Result<(), RtError> {
        let Some(end_offset) = self.unconfirmed_end_offset.clone() else {
            return Ok(());
        };

        let status = self.channel_status().await?;
        let committed = status.last_committed_offset_token.clone();
        tracing::warn!(
            channel = %self.config.channel,
            appended_through = %end_offset,
            committed = committed.as_deref().unwrap_or("<none>"),
            "reconciling a Snowflake append whose commit was never confirmed; rows already \
             committed will be skipped on the retry rather than appended twice"
        );
        self.resume = ResumeFilter::new(committed, self.config.resume_scan_max_events);
        self.unconfirmed_end_offset = None;
        Ok(())
    }

    /// Append the buffer and wait for Snowflake to commit it.
    pub(crate) async fn flush_pending(&mut self) -> Result<(), RtError> {
        // An unconfirmed append with nothing buffered still has to be settled — `close()`
        // and the periodic flush tick both land here with an empty buffer.
        if self.pending.is_empty() {
            return self.reconcile_unconfirmed_append().await;
        }
        let end_offset = self.pending_end_offset.clone().ok_or_else(|| {
            RtError::StateError("a buffered Snowflake batch has no end offset token".to_string())
        })?;

        let body = std::mem::take(&mut self.pending);
        let rows = self.pending_rows;
        self.pending_rows = 0;
        self.pending_since = None;

        let mut attempt = 0u32;
        loop {
            match self.append_once(&body, &end_offset).await {
                Ok(()) => break,
                Err(AppendFailure::Stale) => {
                    // Another writer took the channel, or ours lapsed. Reopening tells us
                    // what is committed; if this batch is already in, the resume filter
                    // will drop it on the retry rather than duplicating it.
                    tracing::warn!(
                        channel = %self.config.channel,
                        "Snowflake continuation token went stale; reopening the channel"
                    );
                    self.open_channel().await?;
                    self.accounting.retries += 1;
                    attempt += 1;
                    if attempt > self.config.max_retries {
                        return Err(RtError::SourceError(format!(
                            "Snowflake channel '{}' kept going stale after {attempt} reopens; \
                             another writer is almost certainly using the same channel name",
                            self.config.channel
                        )));
                    }
                }
                Err(AppendFailure::Retryable(error)) => {
                    attempt += 1;
                    if attempt > self.config.max_retries {
                        return Err(error);
                    }
                    self.accounting.retries += 1;
                    let backoff = Duration::from_millis(100u64.saturating_mul(1 << attempt.min(6)));
                    tokio::time::sleep(backoff).await;
                }
                Err(AppendFailure::Terminal(error)) => return Err(error),
            }
        }

        self.accounting.append_requests += 1;
        self.accounting.rows_appended += rows as u64;
        // Set *before* the wait, so a dropped future still leaves the marker behind.
        self.unconfirmed_end_offset = Some(end_offset.clone());
        self.await_commit(&end_offset).await?;
        self.unconfirmed_end_offset = None;
        Ok(())
    }

    async fn append_once(&mut self, body: &[u8], end_offset: &str) -> Result<(), AppendFailure> {
        let continuation = self
            .continuation_token
            .clone()
            .ok_or(AppendFailure::Stale)?;
        let token = self.scoped_token().await.map_err(AppendFailure::Terminal)?;

        let response = self
            .client
            .post(self.rows_path())
            .bearer_auth(token)
            .query(&[
                ("continuationToken", continuation.as_str()),
                ("endOffsetToken", end_offset),
            ])
            .header(reqwest::header::CONTENT_TYPE, "application/x-ndjson")
            .body(body.to_vec())
            .send()
            .await
            .map_err(|e| {
                if e.is_timeout() || e.is_connect() {
                    AppendFailure::Retryable(RtError::TimeoutError(format!(
                        "Snowflake append rows: {e}"
                    )))
                } else {
                    AppendFailure::Retryable(RtError::SourceError(format!(
                        "Snowflake append rows: {e}"
                    )))
                }
            })?;

        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        if status.is_success() {
            let parsed: AppendRowsResponse = serde_json::from_str(&text).map_err(|e| {
                AppendFailure::Terminal(RtError::SourceError(format!(
                    "Snowflake append rows returned unreadable JSON: {e}"
                )))
            })?;
            self.continuation_token = Some(parsed.next_continuation_token);
            return Ok(());
        }

        let api: ApiError = serde_json::from_str(&text).unwrap_or_default();
        if api.code == STALE_SEQUENCER {
            return Err(AppendFailure::Stale);
        }
        if status.as_u16() == 429 || status.is_server_error() {
            return Err(AppendFailure::Retryable(classify_api_error(
                status,
                &text,
                "append rows",
            )));
        }
        Err(AppendFailure::Terminal(classify_api_error(
            status,
            &text,
            "append rows",
        )))
    }

    /// Block until the channel's committed offset token reaches `end_offset`.
    ///
    /// This is the whole durability contract. Returning early would let the pipeline
    /// checkpoint past rows that a channel reopen discards.
    async fn await_commit(&mut self, end_offset: &str) -> Result<(), RtError> {
        let deadline = Instant::now() + Duration::from_millis(self.config.commit_timeout_ms);
        let started = Instant::now();
        let poll = Duration::from_millis(self.config.commit_poll_ms);
        self.accounting.commit_waits += 1;

        loop {
            let status = self.channel_status().await?;
            if status.last_committed_offset_token.as_deref() == Some(end_offset) {
                self.accounting.commit_wait_ms_total += started.elapsed().as_millis() as u64;
                return Ok(());
            }

            if status.rows_error_count > 0 {
                // Rows Snowflake could not parse are dropped on its side. Reporting success
                // here would advance the checkpoint past events that reached no table.
                return Err(RtError::SourceError(format!(
                    "Snowflake channel '{}' reported {} row error(s) — last: {}. The rows were \
                     rejected server-side and are not in the table, so this flush is a \
                     failure rather than a partial success.",
                    self.config.channel,
                    status.rows_error_count,
                    status.last_error_message.as_deref().unwrap_or("<none>")
                )));
            }

            if Instant::now() >= deadline {
                return Err(RtError::TimeoutError(format!(
                    "Snowflake did not commit up to offset token '{end_offset}' on channel '{}' \
                     within {} ms (last committed: {}). Failing the flush rather than letting \
                     the checkpoint advance past rows a channel reopen would discard.",
                    self.config.channel,
                    self.config.commit_timeout_ms,
                    status
                        .last_committed_offset_token
                        .as_deref()
                        .unwrap_or("<none>")
                )));
            }
            tokio::time::sleep(poll).await;
        }
    }

    /// Resolve the ingest hostname and open the channel.
    pub(crate) async fn preflight(&mut self) -> Result<(), crate::error::AppError> {
        if let Ok(token) = self.scoped_token().await
            && let Ok(response) = self
                .client
                .get(format!("{}/v2/streaming/hostname", self.base()))
                .bearer_auth(token)
                .send()
                .await
            && response.status().is_success()
            && let Ok(parsed) = response.json::<HostnameResponse>().await
            && !parsed.hostname.trim().is_empty()
        {
            // The account URL is a control-plane host; ingest may live elsewhere. Following
            // it is the documented flow, and skipping it costs a redirect per request at
            // best and a 404 at worst.
            let resolved = parsed.hostname.trim().trim_start_matches("https://");
            if resolved != self.host {
                tracing::info!(from = %self.host, to = %resolved, "Snowflake ingest hostname resolved");
                self.host = resolved.to_string();
                // The scoped token's `scope` was the old host.
                self.scoped_token = None;
            }
        }

        self.open_channel()
            .await
            .map_err(|e| crate::error::AppError::Other(format!("Snowflake sink preflight: {e}")))
    }

    pub(crate) fn flush_tick_interval(&self) -> Duration {
        Duration::from_millis(self.config.batch_max_delay_ms)
    }

    pub(crate) fn pending_rows(&self) -> usize {
        self.pending_rows
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.closed
    }

    pub(crate) async fn close(&mut self) -> Result<(), RtError> {
        if self.closed {
            return Ok(());
        }
        self.flush_pending().await?;
        // The channel is deliberately **not** dropped: its offset token is the durable
        // record of what this pipeline has delivered, and dropping the channel deletes it.
        // A restart would then re-deliver everything the checkpoint had not yet covered.
        self.closed = true;
        Ok(())
    }
}

/// Why one `Append Rows` attempt failed.
enum AppendFailure {
    /// The continuation token's sequencer is stale; reopen and retry.
    Stale,
    Retryable(RtError),
    Terminal(RtError),
}

/// Map an API error onto this project's recoverable/terminal split.
fn classify_api_error(status: reqwest::StatusCode, body: &str, context: &str) -> RtError {
    let api: ApiError = serde_json::from_str(body).unwrap_or_default();
    let detail = if api.code.is_empty() {
        body.chars().take(300).collect::<String>()
    } else {
        format!("{}: {}", api.code, api.message)
    };

    if status.as_u16() == 429 || status.is_server_error() {
        return RtError::SourceError(format!("Snowflake {context} returned {status} ({detail})"));
    }
    if status == reqwest::StatusCode::UNAUTHORIZED || status == reqwest::StatusCode::FORBIDDEN {
        return RtError::ConfigError(format!(
            "Snowflake {context} returned {status} ({detail}); the service user's key pair or \
             its grants on the pipe are wrong. This is not retried — retrying a rejected \
             credential drains the change stream against a wall."
        ));
    }
    RtError::StateError(format!("Snowflake {context} returned {status} ({detail})"))
}

/// Decode the configured private key, decrypting it when it is an encrypted PKCS#8 PEM.
///
/// Both forms are accepted because `snowsql`'s own key-generation recipe produces an
/// **encrypted** key by default — so the encrypted case is the common one, not the exotic
/// one, and a sink that only read unencrypted keys would fail for most operators on their
/// first attempt.
pub(crate) fn decode_private_key(
    pem: &str,
    passphrase: Option<&str>,
) -> Result<rsa::RsaPrivateKey, RtError> {
    use rsa::pkcs8::DecodePrivateKey as _;

    let encrypted = pem.contains("BEGIN ENCRYPTED PRIVATE KEY");
    match (encrypted, passphrase) {
        (true, Some(passphrase)) => rsa::RsaPrivateKey::from_pkcs8_encrypted_pem(pem, passphrase)
            .map_err(|e| {
                // Deliberately does not echo the error's detail: PKCS#5 decryption failures
                // can carry parameter values, and this one is reached with a passphrase in
                // scope. The two causes are worth naming instead.
                let _ = e;
                RtError::ConfigError(
                    "sink.snowflake.auth.private_key could not be decrypted: either the \
                     passphrase is wrong or the key is not PKCS#8"
                        .to_string(),
                )
            }),
        (true, None) => Err(RtError::ConfigError(
            "sink.snowflake.auth.private_key is encrypted but no passphrase was configured"
                .to_string(),
        )),
        (false, _) => rsa::RsaPrivateKey::from_pkcs8_pem(pem).map_err(|e| {
            RtError::ConfigError(format!(
                "sink.snowflake.auth.private_key is not a PKCS#8 RSA private key in PEM form: {e}"
            ))
        }),
    }
}

/// `SHA256:<base64>` over the DER-encoded **public** key, as Snowflake stores it in
/// `RSA_PUBLIC_KEY_FP`.
fn public_key_fingerprint(private: &rsa::RsaPrivateKey) -> Result<String, RtError> {
    use base64::Engine as _;
    use rsa::pkcs8::EncodePublicKey;

    let der = private
        .to_public_key()
        .to_public_key_der()
        .map_err(|e| RtError::ConfigError(format!("failed to derive the public key: {e}")))?;

    let digest = Sha256::digest(der.as_bytes());
    Ok(format!(
        "SHA256:{}",
        base64::engine::general_purpose::STANDARD.encode(digest)
    ))
}

/// Fixtures shared by the unit tests here and the contract suite in
/// `tests/integration_snowflake.rs`, which drives this sink against a fake of the API.
#[cfg(test)]
pub(crate) mod test_support {
    use crate::config::schema::SnowflakeSinkConfig;

    pub(crate) const TEST_KEY: &str = include_str!("../../tests/fixtures/snowflake_test_key.pem");

    pub(crate) const ENCRYPTED_TEST_KEY: &str =
        include_str!("../../tests/fixtures/snowflake_test_key_encrypted.pem");
    /// The passphrase `tests/fixtures/snowflake_test_key_encrypted.pem` was created with.
    pub(crate) const TEST_PASSPHRASE: &str = "test-passphrase";

    pub(crate) fn key_pair_auth() -> crate::config::sink::SnowflakeAuthConfig {
        crate::config::sink::SnowflakeAuthConfig::KeyPair {
            private_key: rustcdc::SecretString::new(TEST_KEY.to_string()),
            passphrase: None,
        }
    }

    pub(crate) fn config(account_url: &str) -> SnowflakeSinkConfig {
        SnowflakeSinkConfig {
            account_url: account_url.to_string(),
            user: "cdc_svc".to_string(),
            account: "myorg-myaccount".to_string(),
            auth: key_pair_auth(),
            database: "CDC".to_string(),
            schema: "PUBLIC".to_string(),
            pipe: "EVENTS_PIPE".to_string(),
            channel: "rustcdc".to_string(),
            batch_max_rows: 10_000,
            batch_max_bytes: 3 * 1024 * 1024,
            batch_max_delay_ms: 1_000,
            commit_timeout_ms: 5_000,
            commit_poll_ms: 10,
            request_timeout_ms: 5_000,
            max_retries: 3,
            resume_scan_max_events: 1_000_000,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Admission, ResumeFilter};

    /// The resume contract, stated as a test because it is the whole exactly-once story.
    ///
    /// Snowflake is ahead of the checkpoint by one committed batch; the runtime replays it;
    /// everything up to and including the committed token must be dropped, and everything
    /// after it delivered exactly once.
    #[test]
    fn resume_skips_through_the_committed_token_and_streams_after_it() {
        let mut filter = ResumeFilter::new(Some("0/300".to_string()), 100);
        assert_eq!(filter.admit("0/100"), Admission::Skip);
        assert_eq!(filter.admit("0/200"), Admission::Skip);
        // The matching event is itself committed, so it is skipped too.
        assert_eq!(filter.admit("0/300"), Admission::Skip);
        assert_eq!(filter.admit("0/400"), Admission::Deliver);
        assert_eq!(filter.admit("0/500"), Admission::Deliver);
    }

    #[test]
    fn a_channel_with_nothing_committed_delivers_from_the_first_event() {
        for committed in [None, Some(String::new())] {
            let mut filter = ResumeFilter::new(committed, 100);
            assert_eq!(filter.admit("0/100"), Admission::Deliver);
        }
    }

    /// The bound exists so a token that will never appear cannot silently discard the
    /// entire stream. Exhausting it is at-least-once, and must be visible rather than
    /// quiet — the caller counts it and logs.
    #[test]
    fn an_exhausted_resume_scan_starts_delivering_rather_than_dropping_everything() {
        let mut filter = ResumeFilter::new(Some("never-appears".to_string()), 3);
        assert_eq!(filter.admit("a"), Admission::Skip);
        assert_eq!(filter.admit("b"), Admission::Skip);
        assert_eq!(
            filter.admit("c"),
            Admission::DeliverAfterExhaustedScan,
            "the third event exhausts the window and must be delivered"
        );
        assert_eq!(
            filter.admit("d"),
            Admission::Deliver,
            "and the filter must stay off afterwards rather than re-arming"
        );
    }

    /// The fingerprint must equal what Snowflake's own documented `openssl` recipe
    /// produces, because Snowflake compares it against the `RSA_PUBLIC_KEY_FP` an operator
    /// obtained by running exactly that recipe.
    ///
    /// A mismatch fails authentication with an error naming neither the key nor the
    /// account, so asserting the *shape* ("starts with SHA256:", "44 base64 chars") would
    /// be worthless — every wrong answer has that shape too. The expected value below was
    /// produced independently by:
    ///
    /// ```text
    /// openssl rsa -pubin -in key.pub -outform DER \
    ///   | openssl dgst -sha256 -binary | openssl enc -base64 -A
    /// ```
    #[test]
    fn the_public_key_fingerprint_matches_snowflakes_own_openssl_recipe() {
        const TEST_KEY: &str = include_str!("../../tests/fixtures/snowflake_test_key.pem");
        const EXPECTED: &str = "SHA256:uW27/Up/ytcMzO5yCz2tLHX2KWQKBWP9LSAualR3ssQ=";

        assert_eq!(
            super::public_key_fingerprint(&decode(TEST_KEY, None)).expect("fingerprint"),
            EXPECTED
        );

        // The *encrypted* form of the same key must produce the same fingerprint — the
        // fingerprint is over the public half, which encryption does not touch. Getting
        // this wrong would make an encrypted key authenticate as a different user.
        assert_eq!(
            super::public_key_fingerprint(&decode(
                super::test_support::ENCRYPTED_TEST_KEY,
                Some(super::test_support::TEST_PASSPHRASE)
            ))
            .expect("fingerprint"),
            EXPECTED
        );
    }

    fn decode(pem: &str, passphrase: Option<&str>) -> rsa::RsaPrivateKey {
        super::decode_private_key(pem, passphrase).expect("the key must decode")
    }

    /// `snowsql`'s own key-generation recipe produces an **encrypted** key by default, so
    /// this is the common case rather than the exotic one.
    #[test]
    fn an_encrypted_pkcs8_key_is_decrypted_with_its_passphrase() {
        use super::test_support::{ENCRYPTED_TEST_KEY, TEST_PASSPHRASE};

        super::decode_private_key(ENCRYPTED_TEST_KEY, Some(TEST_PASSPHRASE))
            .expect("the right passphrase must decrypt");

        let wrong = super::decode_private_key(ENCRYPTED_TEST_KEY, Some("not-the-passphrase"))
            .expect_err("a wrong passphrase must fail");
        assert!(
            wrong.to_string().contains("passphrase is wrong"),
            "the error must name the likely cause: {wrong}"
        );
        // And must not leak the passphrase or PKCS#5 parameters into the message.
        assert!(!wrong.to_string().contains("not-the-passphrase"), "{wrong}");

        let missing = super::decode_private_key(ENCRYPTED_TEST_KEY, None)
            .expect_err("an encrypted key without a passphrase must fail");
        assert!(missing.to_string().contains("encrypted"), "{missing}");
    }

    /// The four auth methods must each carry the label Snowflake expects, because an
    /// unlabelled credential is assumed to be `OAUTH` and rejected for the wrong reason.
    #[test]
    fn each_auth_method_names_its_own_token_type() {
        use crate::config::sink::{SnowflakeAuthConfig, SnowflakeWorkloadIdentityProvider};

        assert_eq!(
            super::test_support::key_pair_auth().token_type(),
            "KEYPAIR_JWT"
        );
        assert_eq!(
            SnowflakeAuthConfig::ProgrammaticAccessToken {
                token: rustcdc::SecretString::new("pat".to_string())
            }
            .token_type(),
            "PROGRAMMATIC_ACCESS_TOKEN"
        );
        assert_eq!(
            SnowflakeAuthConfig::WorkloadIdentity {
                provider: SnowflakeWorkloadIdentityProvider::Oidc,
                token_file: "/dev/null".into(),
            }
            .token_type(),
            "WORKLOAD_IDENTITY_FEDERATION"
        );

        for (provider, wire) in [
            (SnowflakeWorkloadIdentityProvider::Oidc, "OIDC"),
            (SnowflakeWorkloadIdentityProvider::Aws, "AWS"),
            (SnowflakeWorkloadIdentityProvider::Azure, "AZURE"),
            (SnowflakeWorkloadIdentityProvider::Gcp, "GCP"),
        ] {
            assert_eq!(provider.as_wire(), wire);
        }
    }

    /// The JWT must carry the claims Snowflake matches on, in the case it matches in.
    #[test]
    fn the_jwt_carries_an_uppercased_qualified_username_and_the_fingerprint() {
        use base64::Engine as _;

        const TEST_KEY: &str = include_str!("../../tests/fixtures/snowflake_test_key.pem");
        let sink = super::SnowflakeSink {
            client: reqwest::Client::new(),
            config: super::test_support::config("https://acct.snowflakecomputing.com"),
            auth: super::test_support::key_pair_auth(),
            max_event_bytes: 1 << 20,
            scheme: "https",
            host: "acct.snowflakecomputing.com".to_string(),
            scoped_token: None,
            continuation_token: None,
            resume: ResumeFilter::Streaming,
            pending: Vec::new(),
            pending_rows: 0,
            pending_end_offset: None,
            pending_since: None,
            unconfirmed_end_offset: None,
            accounting: Default::default(),
            closed: false,
        };

        let jwt = sink.mint_jwt().expect("the JWT must sign");
        let payload = jwt.split('.').nth(1).expect("a JWT has three parts");
        let decoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(payload)
            .expect("base64url payload");
        let claims: serde_json::Value =
            serde_json::from_slice(&decoded).expect("the payload is JSON");

        // Lower-case here is the classic failure: Snowflake stores both upper-cased and
        // reports only "JWT token is invalid".
        assert_eq!(claims["sub"], "MYORG-MYACCOUNT.CDC_SVC");
        assert_eq!(
            claims["iss"],
            format!(
                "MYORG-MYACCOUNT.CDC_SVC.{}",
                super::public_key_fingerprint(&decode(TEST_KEY, None)).expect("fingerprint")
            )
        );
        assert!(
            claims["exp"].as_u64().expect("exp") > claims["iat"].as_u64().expect("iat"),
            "the token must expire after it was issued"
        );
    }

    /// The RS256 signature is hand-rolled — `jsonwebtoken` was dropped because its crypto
    /// backend added a second `untrusted` to the graph — so it is verified here rather than
    /// assumed. A JWT Snowflake cannot verify fails with "JWT token is invalid", which names
    /// nothing.
    #[test]
    fn the_jwt_signature_verifies_against_the_public_key() {
        use base64::Engine as _;
        use rsa::pkcs1v15::{Signature, VerifyingKey};
        use rsa::pkcs8::DecodePrivateKey as _;
        use rsa::signature::Verifier as _;

        const TEST_KEY: &str = include_str!("../../tests/fixtures/snowflake_test_key.pem");
        let sink = super::SnowflakeSink {
            client: reqwest::Client::new(),
            config: super::test_support::config("https://acct.snowflakecomputing.com"),
            auth: super::test_support::key_pair_auth(),
            max_event_bytes: 1 << 20,
            scheme: "https",
            host: "acct.snowflakecomputing.com".to_string(),
            scoped_token: None,
            continuation_token: None,
            resume: ResumeFilter::Streaming,
            pending: Vec::new(),
            pending_rows: 0,
            pending_end_offset: None,
            pending_since: None,
            unconfirmed_end_offset: None,
            accounting: Default::default(),
            closed: false,
        };

        let jwt = sink.mint_jwt().expect("sign");
        let (signing_input, encoded_signature) =
            jwt.rsplit_once('.').expect("a JWT has three parts");

        let b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD;
        let signature = Signature::try_from(
            b64.decode(encoded_signature)
                .expect("the signature is base64url")
                .as_slice(),
        )
        .expect("the signature is a valid PKCS#1 v1.5 blob");

        let public = rsa::RsaPrivateKey::from_pkcs8_pem(TEST_KEY)
            .expect("key")
            .to_public_key();
        VerifyingKey::<rsa::sha2::Sha256>::new(public)
            .verify(signing_input.as_bytes(), &signature)
            .expect("the signature must verify over `header.claims` with RS256");

        // And the header must actually say RS256 — a correct signature under the wrong
        // declared algorithm is rejected by every verifier.
        let header = jwt.split('.').next().expect("header");
        let decoded: serde_json::Value =
            serde_json::from_slice(&b64.decode(header).expect("base64url header"))
                .expect("the header is JSON");
        assert_eq!(decoded["alg"], "RS256");
        assert_eq!(decoded["typ"], "JWT");
    }
}
