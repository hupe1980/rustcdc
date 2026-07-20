use std::{
    io::Write,
    path::PathBuf,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use rustcdc::{
    core::{Error as RtError, Event},
    sink::SinkAdapter,
    SecretString,
};
use sha2::Digest;

use crate::config::{schema::HttpSinkConfig, validate_http_sink_url_policy};
use crate::sink::{
    HTTP_BATCH_RETRY_DURATION_MS_BUCKETS, HTTP_BATCH_SIZE_BUCKETS, HTTP_RETRY_DELAY_MS_BUCKETS,
};

#[derive(Debug, Clone, Default)]
pub struct DeliveryAccounting {
    pub accepted: u64,
    pub http_requests_total: u64,
    pub http_batch_size_samples_total: u64,
    pub http_batch_size_bucket_counts: [u64; HTTP_BATCH_SIZE_BUCKETS.len()],
    pub http_batch_oldest_event_age_ms_last: u64,
    pub http_pending_bytes_current: u64,
    pub http_pending_bytes_high_watermark: u64,
    pub http_retry_delay_samples_total: u64,
    pub http_retry_delay_bucket_counts: [u64; HTTP_RETRY_DELAY_MS_BUCKETS.len()],
    pub http_batch_retry_duration_samples_total: u64,
    pub http_batch_retry_duration_bucket_counts: [u64; HTTP_BATCH_RETRY_DURATION_MS_BUCKETS.len()],
    pub http_batch_retry_duration_ms_total: u64,
    pub http_batch_retry_duration_ms_last: u64,
    pub retried: u64,
    pub retryable_status_429: u64,
    pub retryable_status_5xx: u64,
    pub retryable_error_timeout: u64,
    pub retryable_error_other: u64,
    pub terminal: u64,
    pub terminal_status_4xx: u64,
    pub terminal_status_other: u64,
    pub terminal_error_timeout: u64,
    pub terminal_error_other: u64,
    pub dropped: u64,
    pub dlq_written: u64,
}

pub struct HttpSink {
    client: reqwest::Client,
    url: String,
    timeout: Duration,
    batch_max_events: usize,
    batch_max_delay: Duration,
    max_pending_bytes: u64,
    max_retries: u32,
    backoff_initial: Duration,
    backoff_max: Duration,
    backoff_multiplier: f64,
    headers: Vec<(String, String)>,
    bearer_token: Option<SecretString>,
    dlq_path: Option<PathBuf>,
    dlq_max_bytes: u64,
    /// Global time budget ceiling for retries within a single flush operation.
    /// Prevents unbounded retry storms during sustained failures.
    /// Default: configured via `sink.http.batch_retry_time_budget_ms`.
    batch_retry_time_budget: Duration,
    accounting: DeliveryAccounting,
    pending: Vec<Vec<u8>>,
    pending_bytes: u64,
    pending_since: Option<std::time::Instant>,
    closed: bool,
}

#[derive(Debug, Clone, Copy)]
enum DeliveryPhase {
    Retryable,
    Terminal,
}

impl HttpSink {
    pub fn new(config: &HttpSinkConfig) -> rustcdc::core::Result<Self> {
        if !config.verify_tls {
            return Err(RtError::ConfigError(
                "sink.http.verify_tls must be true; insecure TLS bypass is unsupported".to_string(),
            ));
        }
        validate_http_sink_url_policy(&config.url)
            .map_err(|e| RtError::ConfigError(e.to_string()))?;
        if !config.backoff_multiplier.is_finite() {
            return Err(RtError::ConfigError(
                "sink.http.backoff_multiplier must be finite".to_string(),
            ));
        }
        if config.max_pending_bytes == 0 {
            return Err(RtError::ConfigError(
                "sink.http.max_pending_bytes must be > 0".to_string(),
            ));
        }

        let is_https = config.url.starts_with("https://");
        let mut builder = reqwest::Client::builder()
            // CR-005: explicit TLS hardening — do not rely on library defaults.
            .min_tls_version(reqwest::tls::Version::TLS_1_2)
            // Allow http:// only for loopback (validated above by url_policy);
            // enforce https_only for all other endpoints as defence-in-depth.
            .https_only(is_https)
            // explicit connection-pool tuning.
            .pool_max_idle_per_host(config.pool_max_idle_per_host);

        if let Some(secs) = config.tcp_keepalive_secs {
            builder = builder.tcp_keepalive(Duration::from_secs(secs));
        }
        if let Some(secs) = config.pool_idle_timeout_secs {
            builder = builder.pool_idle_timeout(Duration::from_secs(secs));
        }

        let client = builder
            .build()
            .map_err(|e| RtError::ConfigError(format!("failed to build HTTP client: {e}")))?;

        let headers = config
            .headers
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        Ok(Self {
            client,
            url: config.url.clone(),
            timeout: Duration::from_millis(config.timeout_ms),
            batch_max_events: config.batch_max_events,
            batch_max_delay: Duration::from_millis(config.batch_max_delay_ms),
            max_pending_bytes: config.max_pending_bytes,
            max_retries: config.max_retries,
            backoff_initial: Duration::from_millis(config.backoff_initial_ms),
            backoff_max: Duration::from_millis(config.backoff_max_ms),
            backoff_multiplier: config.backoff_multiplier,
            headers,
            bearer_token: config.bearer_token.clone(),
            dlq_path: config.dlq_path.clone(),
            dlq_max_bytes: config.dlq_max_bytes,
            batch_retry_time_budget: Duration::from_millis(config.batch_retry_time_budget_ms),
            accounting: DeliveryAccounting::default(),
            pending: Vec::new(),
            pending_bytes: 0,
            pending_since: None,
            closed: false,
        })
    }

    fn retry_budget_exhausted(&self, batch_start: std::time::Instant) -> bool {
        batch_start.elapsed() > self.batch_retry_time_budget
    }

    /// Validate HTTP endpoint connectivity before the pipeline starts by
    /// sending a HEAD request to the configured URL.  This ensures a
    /// misconfigured endpoint is caught early rather than on first event.
    pub async fn preflight_check(&self) -> Result<(), crate::error::AppError> {
        let result = self
            .client
            .head(&self.url)
            .timeout(Duration::from_secs(10))
            .send()
            .await;

        match result {
            Ok(resp) => {
                let status = resp.status();
                // 4xx responses that indicate the endpoint exists but rejects
                // HEAD or requires auth are acceptable signs of life.
                let is_sign_of_life = status.is_success()
                    || status == reqwest::StatusCode::UNAUTHORIZED       // 401: endpoint exists, auth needed
                    || status == reqwest::StatusCode::METHOD_NOT_ALLOWED // 405: HEAD not allowed but endpoint exists
                    || status == reqwest::StatusCode::UNSUPPORTED_MEDIA_TYPE // 415: content-type issue
                    || status.is_server_error(); // 5xx: server exists, but busy

                if is_sign_of_life {
                    tracing::info!(
                        url = %self.url,
                        status = %status,
                        "sink preflight: HTTP endpoint reachable"
                    );
                } else {
                    // Definitive 4xx (e.g. 404, 403, 410) strongly suggests a
                    // misconfigured URL.  Warn so operators notice at startup
                    // rather than discovering via DLQ writes.
                    tracing::warn!(
                        url = %self.url,
                        status = %status,
                        "sink preflight: HTTP endpoint returned a client-error status; \
                         check that the URL path is correct (pipeline will still start)"
                    );
                }
                Ok(())
            }
            Err(e) if e.is_connect() || e.is_timeout() => {
                Err(crate::error::AppError::Other(format!(
                    "sink preflight: HTTP endpoint '{}' unreachable: {e}",
                    self.url
                )))
            }
            Err(_) => {
                // Other reqwest errors (e.g., TLS handshake details) are
                // reachability indicators but not hard blockers — warn only.
                tracing::warn!(url = %self.url, "sink preflight: HTTP endpoint check inconclusive");
                Ok(())
            }
        }
    }

    fn next_backoff(&self, attempt: u32) -> Duration {
        let base_ms = self.backoff_initial.as_millis() as f64;
        let scaled = base_ms * self.backoff_multiplier.powi(attempt as i32);
        if !scaled.is_finite() {
            return self.backoff_max;
        }

        let capped = scaled
            .clamp(0.0, self.backoff_max.as_millis() as f64)
            .round();
        Duration::from_millis(capped as u64)
    }

    fn is_retryable_status(status: reqwest::StatusCode) -> bool {
        status.as_u16() == 429 || status.is_server_error()
    }

    fn classify_status(&mut self, status: reqwest::StatusCode, phase: DeliveryPhase) {
        match phase {
            DeliveryPhase::Retryable if status.as_u16() == 429 => {
                self.accounting.retryable_status_429 += 1;
            }
            DeliveryPhase::Retryable if status.is_server_error() => {
                self.accounting.retryable_status_5xx += 1;
            }
            DeliveryPhase::Terminal if status.is_client_error() => {
                self.accounting.terminal_status_4xx += 1;
            }
            DeliveryPhase::Terminal => {
                self.accounting.terminal_status_other += 1;
            }
            _ => {}
        }
    }

    fn classify_error(&mut self, error: &RtError, phase: DeliveryPhase) {
        let is_timeout = matches!(error, RtError::TimeoutError(_));
        match phase {
            DeliveryPhase::Retryable if is_timeout => {
                self.accounting.retryable_error_timeout += 1;
            }
            DeliveryPhase::Retryable => {
                self.accounting.retryable_error_other += 1;
            }
            DeliveryPhase::Terminal if is_timeout => {
                self.accounting.terminal_error_timeout += 1;
            }
            DeliveryPhase::Terminal => {
                self.accounting.terminal_error_other += 1;
            }
        }
    }

    fn observe_batch_size(&mut self, batch_events: usize) {
        self.accounting.http_batch_size_samples_total = self
            .accounting
            .http_batch_size_samples_total
            .saturating_add(1);
        let batch_events = batch_events as u64;
        for (index, upper_bound) in HTTP_BATCH_SIZE_BUCKETS.iter().enumerate() {
            if batch_events <= *upper_bound {
                self.accounting.http_batch_size_bucket_counts[index] =
                    self.accounting.http_batch_size_bucket_counts[index].saturating_add(1);
                return;
            }
        }

        let last = self.accounting.http_batch_size_bucket_counts.len() - 1;
        self.accounting.http_batch_size_bucket_counts[last] =
            self.accounting.http_batch_size_bucket_counts[last].saturating_add(1);
    }

    async fn fail_delivery(
        &mut self,
        payloads: &[Vec<u8>],
        message: String,
    ) -> rustcdc::core::Result<()> {
        self.accounting.terminal = self
            .accounting
            .terminal
            .saturating_add(payloads.len() as u64);
        for payload in payloads {
            self.append_dlq(payload, &message).await?;
        }
        Err(RtError::SourceError(format!(
            "HTTP sink delivery failed after retries: {message}"
        )))
    }

    async fn send_once(&self, payload: bytes::Bytes) -> rustcdc::core::Result<reqwest::StatusCode> {
        let idempotency_key = build_http_idempotency_key(&payload);
        let mut req = self
            .client
            .post(&self.url)
            .timeout(self.timeout)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .header("Idempotency-Key", idempotency_key)
            // `bytes::Bytes::clone` is a cheap reference-count bump — no heap copy.
            .body(payload.clone());

        for (k, v) in &self.headers {
            req = req.header(k, v);
        }
        if let Some(token) = &self.bearer_token {
            let token = token.resolve().map_err(|e| {
                RtError::ConfigError(format!("sink.http.bearer_token could not be resolved: {e}"))
            })?;
            req = req.bearer_auth(token);
        }

        let resp = req.send().await.map_err(|e| {
            if e.is_timeout() {
                RtError::TimeoutError(format!("HTTP sink timeout: {e}"))
            } else {
                RtError::SourceError(format!("HTTP sink request failed: {e}"))
            }
        })?;

        Ok(resp.status())
    }

    async fn send_once_counted(
        &mut self,
        payload: bytes::Bytes,
    ) -> rustcdc::core::Result<reqwest::StatusCode> {
        self.accounting.http_requests_total = self.accounting.http_requests_total.saturating_add(1);
        self.send_once(payload).await
    }

    async fn deliver_payload_with_retry(
        &mut self,
        payload: bytes::Bytes,
        accepted_events: usize,
        batch_start: std::time::Instant,
    ) -> Result<(), String> {
        for attempt in 0..=self.max_retries {
            if self.retry_budget_exhausted(batch_start) {
                return Err(format!(
                    "batch retry time budget exceeded ({:?})",
                    self.batch_retry_time_budget
                ));
            }

            // `bytes::Bytes::clone` is a cheap reference-count bump — no heap copy per attempt.
            match self.send_once_counted(payload.clone()).await {
                Ok(status) if status.is_success() => {
                    self.accounting.accepted = self
                        .accounting
                        .accepted
                        .saturating_add(accepted_events as u64);
                    return Ok(());
                }
                Ok(status) if Self::is_retryable_status(status) => {
                    if attempt < self.max_retries {
                        self.accounting.retried += 1;
                        self.classify_status(status, DeliveryPhase::Retryable);
                        let backoff = self.next_backoff(attempt);
                        if self.retry_budget_exhausted(batch_start) {
                            return Err(format!(
                                "batch retry time budget exceeded ({:?})",
                                self.batch_retry_time_budget
                            ));
                        }
                        self.observe_retry_delay(backoff);
                        tokio::time::sleep(backoff).await;
                        continue;
                    }
                    return Err(format!("retryable HTTP status {}", status.as_u16()));
                }
                Ok(status) => {
                    self.classify_status(status, DeliveryPhase::Terminal);
                    return Err(format!("terminal HTTP status {}", status.as_u16()));
                }
                Err(e) if e.is_recoverable() => {
                    if attempt < self.max_retries {
                        self.accounting.retried += 1;
                        self.classify_error(&e, DeliveryPhase::Retryable);
                        let backoff = self.next_backoff(attempt);
                        if self.retry_budget_exhausted(batch_start) {
                            return Err(format!(
                                "batch retry time budget exceeded ({:?})",
                                self.batch_retry_time_budget
                            ));
                        }
                        self.observe_retry_delay(backoff);
                        tokio::time::sleep(backoff).await;
                        continue;
                    }
                    return Err(e.to_string());
                }
                Err(e) => {
                    self.classify_error(&e, DeliveryPhase::Terminal);
                    return Err(e.to_string());
                }
            }
        }

        Err("unexpected HTTP sink retry-loop exit".to_string())
    }

    fn observe_retry_delay(&mut self, delay: Duration) {
        self.accounting.http_retry_delay_samples_total = self
            .accounting
            .http_retry_delay_samples_total
            .saturating_add(1);

        let delay_ms = delay.as_millis() as u64;
        for (index, upper_bound) in HTTP_RETRY_DELAY_MS_BUCKETS.iter().enumerate() {
            if delay_ms <= *upper_bound {
                self.accounting.http_retry_delay_bucket_counts[index] =
                    self.accounting.http_retry_delay_bucket_counts[index].saturating_add(1);
                return;
            }
        }

        let last = self.accounting.http_retry_delay_bucket_counts.len() - 1;
        self.accounting.http_retry_delay_bucket_counts[last] =
            self.accounting.http_retry_delay_bucket_counts[last].saturating_add(1);
    }

    fn observe_batch_retry_duration(&mut self, duration: Duration) {
        let duration_ms = duration.as_millis() as u64;
        self.accounting.http_batch_retry_duration_samples_total = self
            .accounting
            .http_batch_retry_duration_samples_total
            .saturating_add(1);
        self.accounting.http_batch_retry_duration_ms_total = self
            .accounting
            .http_batch_retry_duration_ms_total
            .saturating_add(duration_ms);
        self.accounting.http_batch_retry_duration_ms_last = duration_ms;

        for (index, upper_bound) in HTTP_BATCH_RETRY_DURATION_MS_BUCKETS.iter().enumerate() {
            if duration_ms <= *upper_bound {
                self.accounting.http_batch_retry_duration_bucket_counts[index] =
                    self.accounting.http_batch_retry_duration_bucket_counts[index]
                        .saturating_add(1);
                return;
            }
        }

        let last = self
            .accounting
            .http_batch_retry_duration_bucket_counts
            .len()
            - 1;
        self.accounting.http_batch_retry_duration_bucket_counts[last] =
            self.accounting.http_batch_retry_duration_bucket_counts[last].saturating_add(1);
    }

    fn build_http_batch_payload(payloads: &[Vec<u8>]) -> Vec<u8> {
        if payloads.is_empty() {
            return b"[]".to_vec();
        }

        let payload_bytes = payloads.iter().map(Vec::len).sum::<usize>();
        let mut batch = Vec::with_capacity(payload_bytes + payloads.len() + 2);
        batch.push(b'[');

        for (idx, payload) in payloads.iter().enumerate() {
            if idx > 0 {
                batch.push(b',');
            }
            batch.extend_from_slice(payload);
        }

        batch.push(b']');
        batch
    }

    async fn send_batch_payload(&mut self, payloads: &[Vec<u8>]) -> rustcdc::core::Result<()> {
        self.observe_batch_size(payloads.len());
        let batch_start = std::time::Instant::now();
        // Convert to `bytes::Bytes` once — all retry attempts share the same allocation
        // via reference-count bumps rather than heap copies.
        let payload = bytes::Bytes::from(Self::build_http_batch_payload(payloads));
        if let Err(message) = self
            .deliver_payload_with_retry(payload, payloads.len(), batch_start)
            .await
        {
            // Salvage terminal batch failures by retrying per-event payloads.
            if payloads.len() > 1 {
                let mut unsalvageable = Vec::new();
                let mut last_error = message;
                for single_payload in payloads {
                    if self.retry_budget_exhausted(batch_start) {
                        tracing::warn!(
                            "batch salvage exceeded time budget ({:?}); aborting remaining events",
                            self.batch_retry_time_budget
                        );
                        unsalvageable.push(single_payload.clone());
                        continue;
                    }

                    let sp = bytes::Bytes::from(single_payload.clone());
                    if let Err(error) = self.deliver_payload_with_retry(sp, 1, batch_start).await {
                        last_error = error;
                        unsalvageable.push(single_payload.clone());
                    }
                }

                if unsalvageable.is_empty() {
                    self.observe_batch_retry_duration(batch_start.elapsed());
                    return Ok(());
                }

                self.observe_batch_retry_duration(batch_start.elapsed());
                return self
                    .fail_delivery(
                        &unsalvageable,
                        format!(
                            "batch salvage left {} unsent events: {}",
                            unsalvageable.len(),
                            last_error
                        ),
                    )
                    .await;
            }

            self.observe_batch_retry_duration(batch_start.elapsed());
            return self.fail_delivery(payloads, message).await;
        }

        self.observe_batch_retry_duration(batch_start.elapsed());

        Ok(())
    }

    async fn flush_pending(&mut self) -> rustcdc::core::Result<()> {
        if self.pending.is_empty() {
            self.pending_since = None;
            self.pending_bytes = 0;
            self.accounting.http_pending_bytes_current = 0;
            return Ok(());
        }

        let oldest_event_age_ms = self
            .pending_since
            .map(|started| started.elapsed().as_millis() as u64)
            .unwrap_or(0);
        self.accounting.http_batch_oldest_event_age_ms_last = oldest_event_age_ms;

        let payloads = std::mem::take(&mut self.pending);
        self.pending_bytes = 0;
        self.accounting.http_pending_bytes_current = 0;
        self.pending_since = None;
        self.send_batch_payload(&payloads).await
    }

    fn should_flush_pending_by_delay(&self) -> bool {
        self.pending_since
            .is_some_and(|started| started.elapsed() >= self.batch_max_delay)
    }

    async fn enqueue_and_maybe_flush(&mut self, payload: Vec<u8>) -> rustcdc::core::Result<()> {
        let payload_len = payload.len() as u64;
        if payload_len > self.max_pending_bytes {
            return Err(RtError::StateError(format!(
                "HTTP sink event payload {} bytes exceeds sink.http.max_pending_bytes {}",
                payload_len, self.max_pending_bytes
            )));
        }

        if self.pending_bytes.saturating_add(payload_len) > self.max_pending_bytes {
            self.flush_pending().await?;
        }

        if self.pending.is_empty() {
            self.pending_since = Some(std::time::Instant::now());
        }

        self.pending_bytes = self.pending_bytes.saturating_add(payload_len);
        self.accounting.http_pending_bytes_current = self.pending_bytes;
        if self.pending_bytes > self.accounting.http_pending_bytes_high_watermark {
            self.accounting.http_pending_bytes_high_watermark = self.pending_bytes;
        }
        self.pending.push(payload);

        if self.pending.len() >= self.batch_max_events || self.should_flush_pending_by_delay() {
            self.flush_pending().await?;
        }

        Ok(())
    }

    async fn append_dlq(&mut self, payload: &[u8], error: &str) -> rustcdc::core::Result<()> {
        let Some(path) = &self.dlq_path else {
            self.accounting.dropped += 1;
            return Ok(());
        };

        let path = path.clone();
        let dlq_max_bytes = self.dlq_max_bytes;
        let payload = payload.to_vec();
        let error = error.to_string();
        let write_result = tokio::task::spawn_blocking(move || -> rustcdc::core::Result<()> {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).map_err(RtError::IoError)?;
            }

            let ts_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0);

            let dlq_entry = serde_json::json!({
                "ts_ms": ts_ms,
                "error": error,
                "event": serde_json::from_slice::<serde_json::Value>(&payload).unwrap_or_else(|_| {
                    serde_json::json!({"raw": String::from_utf8_lossy(&payload)})
                }),
            });

            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)
                .map_err(RtError::IoError)?;
            let line = serde_json::to_string(&dlq_entry)
                .map_err(|e| RtError::SerializationError(e.to_string()))?;

            let current_size = file.metadata().map_err(RtError::IoError)?.len();
            let projected_size = current_size.saturating_add(line.len() as u64 + 1);
            if projected_size > dlq_max_bytes {
                return Err(RtError::StateError(format!(
                    "DLQ size limit exceeded for {}: projected {} bytes > sink.http.dlq_max_bytes {}",
                    path.display(),
                    projected_size,
                    dlq_max_bytes
                )));
            }

            writeln!(file, "{line}").map_err(RtError::IoError)?;
            Ok(())
        })
        .await
        .map_err(|error| RtError::SourceError(format!("dlq write task join error: {error}")))?;

        write_result?;
        self.accounting.dlq_written += 1;
        Ok(())
    }

    async fn send_internal(&mut self, event: &Event) -> rustcdc::core::Result<()> {
        if self.closed {
            return Err(RtError::StateError("sink is closed".to_string()));
        }

        let payload =
            serde_json::to_vec(event).map_err(|e| RtError::SerializationError(e.to_string()))?;
        self.enqueue_and_maybe_flush(payload).await
    }

    pub async fn send_json_bytes(&mut self, event_json: &[u8]) -> rustcdc::core::Result<()> {
        if self.closed {
            return Err(RtError::StateError("sink is closed".to_string()));
        }
        self.enqueue_and_maybe_flush(event_json.to_vec()).await
    }

    pub async fn send_json_vec(&mut self, event_json: Vec<u8>) -> rustcdc::core::Result<()> {
        if self.closed {
            return Err(RtError::StateError("sink is closed".to_string()));
        }
        self.enqueue_and_maybe_flush(event_json).await
    }

    #[cfg(test)]
    pub fn accounting(&self) -> &DeliveryAccounting {
        &self.accounting
    }

    pub fn delivery_metrics(&self) -> DeliveryAccounting {
        self.accounting.clone()
    }

    pub fn pending_events(&self) -> usize {
        self.pending.len()
    }

    pub fn pending_bytes(&self) -> u64 {
        self.pending_bytes
    }

    pub fn flush_tick_interval(&self) -> std::time::Duration {
        self.batch_max_delay
    }
}

fn build_http_idempotency_key(payload: &[u8]) -> String {
    let digest = sha2::Sha256::digest(payload);
    format!("cdc-http-{}", hex::encode(digest))
}

impl SinkAdapter for HttpSink {
    fn name(&self) -> &str {
        "http"
    }

    async fn send(&mut self, event: &Event) -> rustcdc::core::Result<()> {
        self.send_internal(event).await
    }

    async fn flush(&mut self) -> rustcdc::core::Result<()> {
        self.flush_pending().await
    }

    async fn close(&mut self) -> rustcdc::core::Result<()> {
        if !self.closed {
            self.flush_pending().await?;
            self.closed = true;
        }

        let accounting = self.accounting.clone();
        tracing::info!(
            accepted = accounting.accepted,
            retried = accounting.retried,
            terminal = accounting.terminal,
            dropped = accounting.dropped,
            "http sink delivery accounting"
        );
        Ok(())
    }

    fn is_closed(&self) -> bool {
        self.closed
    }

    fn flush_tick_interval(&self) -> Option<std::time::Duration> {
        Some(self.batch_max_delay)
    }

    fn delivery_guarantee(&self) -> rustcdc::sink::SinkDeliveryGuarantee {
        // HTTP sink delivers at-least-once: retries are attempted on transient
        // failures, but duplicate delivery is possible.
        rustcdc::sink::SinkDeliveryGuarantee::AtLeastOnce
    }

    fn queue_depth(&self) -> Option<usize> {
        Some(self.pending_events())
    }

    async fn preflight_check(&mut self) -> rustcdc::core::Result<()> {
        // Reborrow as `&Self` so method resolution picks the inherent
        // `preflight_check(&self)` instead of this trait method (which would
        // cause infinite recursion since both have the same name).
        let this: &Self = self;
        this.preflight_check()
            .await
            .map_err(|e| rustcdc::core::Error::SourceError(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use rustcdc::core::{Event, Operation, SourceMetadata, EVENT_ENVELOPE_VERSION};
    use rustcdc::SecretString;
    use serde_json::json;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use super::HttpSink;
    use crate::config::schema::HttpSinkConfig;
    use rustcdc::sink::SinkAdapter;

    fn extract_header(req: &str, header_name: &str) -> Option<String> {
        req.lines().find_map(|line| {
            let (name, value) = line.split_once(':')?;
            if name.trim().eq_ignore_ascii_case(header_name) {
                Some(value.trim().to_string())
            } else {
                None
            }
        })
    }

    fn sample_event() -> Event {
        Event {
            before: None,
            after: Some(json!({"id": 1, "name": "alice"})),
            op: Operation::Insert,
            source: SourceMetadata {
                source_name: "postgres".to_string(),
                offset: "0/16B6A70".to_string(),
                timestamp: 1,
            },
            ts: 1,
            schema: Some("public".to_string()),
            table: "users".to_string(),
            primary_key: Some(vec!["id".to_string()]),
            snapshot: None,
            transaction: None,
            envelope_version: EVENT_ENVELOPE_VERSION,
            before_is_key_only: false,
            unavailable_columns: Vec::new(),
            before_unavailable_columns: Vec::new(),
        }
    }

    async fn spawn_http_server(
        statuses: Arc<Mutex<Vec<u16>>>,
        captures: Arc<Mutex<Vec<String>>>,
    ) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local addr");

        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    break;
                };

                let mut buf = vec![0u8; 16 * 1024];
                let Ok(n) = socket.read(&mut buf).await else {
                    continue;
                };
                if n == 0 {
                    continue;
                }
                let req = String::from_utf8_lossy(&buf[..n]).to_string();
                captures.lock().expect("captures lock").push(req);

                let status = {
                    let mut s = statuses.lock().expect("statuses lock");
                    if s.is_empty() {
                        200
                    } else {
                        s.remove(0)
                    }
                };
                let response = format!(
                    "HTTP/1.1 {status} TEST\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                );
                let _ = socket.write_all(response.as_bytes()).await;
            }
        });

        format!("http://{addr}")
    }

    #[tokio::test]
    async fn retries_and_succeeds() {
        let statuses = Arc::new(Mutex::new(vec![500, 502, 200]));
        let captures = Arc::new(Mutex::new(Vec::new()));
        let url = spawn_http_server(statuses, captures.clone()).await;

        let cfg = HttpSinkConfig {
            url,
            timeout_ms: 1000,
            batch_max_events: 64,
            batch_max_delay_ms: 10,
            max_pending_bytes: 1024 * 1024,
            max_retries: 3,
            batch_retry_time_budget_ms: 30_000,
            backoff_initial_ms: 1,
            backoff_max_ms: 5,
            backoff_multiplier: 2.0,
            headers: std::collections::HashMap::new(),
            bearer_token: None,
            verify_tls: true,
            dlq_path: None,
            dlq_max_bytes: 128 * 1024 * 1024,
            pool_max_idle_per_host: 10,
            pool_idle_timeout_secs: None,
            tcp_keepalive_secs: None,
            codec: None,
        };

        let mut sink = HttpSink::new(&cfg).expect("sink");
        sink.send(&sample_event())
            .await
            .expect("delivery should succeed");
        sink.flush().await.expect("flush should succeed");
        assert_eq!(sink.accounting().accepted, 1);
        assert_eq!(sink.accounting().http_batch_size_samples_total, 1);
        assert_eq!(sink.accounting().http_requests_total, 3);
        assert_eq!(sink.accounting().retried, 2);
        assert_eq!(sink.accounting().http_retry_delay_samples_total, 2);
        assert_eq!(sink.accounting().http_batch_retry_duration_samples_total, 1);
        assert!(sink.accounting().http_batch_retry_duration_ms_total > 0);
        assert!(
            sink.accounting().http_batch_retry_duration_ms_last
                <= sink.accounting().http_batch_retry_duration_ms_total
        );
        assert_eq!(sink.accounting().retryable_status_429, 0);
        assert_eq!(sink.accounting().retryable_status_5xx, 2);
        assert_eq!(sink.accounting().retryable_error_timeout, 0);
        assert_eq!(sink.accounting().retryable_error_other, 0);
        assert_eq!(sink.accounting().terminal, 0);
        assert_eq!(sink.accounting().terminal_status_4xx, 0);
        assert_eq!(sink.accounting().terminal_status_other, 0);
        assert_eq!(sink.accounting().terminal_error_timeout, 0);
        assert_eq!(sink.accounting().terminal_error_other, 0);
        let requests = captures.lock().expect("captures lock");
        assert_eq!(requests.len(), 3);
        let first_key = extract_header(&requests[0], "idempotency-key")
            .expect("first request should include Idempotency-Key");
        for req in requests.iter().skip(1) {
            let key = extract_header(req, "idempotency-key")
                .expect("retry request should include Idempotency-Key");
            assert_eq!(
                key, first_key,
                "retries must reuse the same idempotency key"
            );
        }
    }

    #[tokio::test]
    async fn batches_multiple_events_into_single_request() {
        let statuses = Arc::new(Mutex::new(vec![200]));
        let captures = Arc::new(Mutex::new(Vec::new()));
        let url = spawn_http_server(statuses, captures.clone()).await;

        let cfg = HttpSinkConfig {
            url,
            timeout_ms: 1000,
            batch_max_events: 2,
            batch_max_delay_ms: 60_000,
            max_pending_bytes: 1024 * 1024,
            max_retries: 1,
            batch_retry_time_budget_ms: 30_000,
            backoff_initial_ms: 1,
            backoff_max_ms: 5,
            backoff_multiplier: 2.0,
            headers: std::collections::HashMap::new(),
            bearer_token: None,
            verify_tls: true,
            dlq_path: None,
            dlq_max_bytes: 128 * 1024 * 1024,
            pool_max_idle_per_host: 10,
            pool_idle_timeout_secs: None,
            tcp_keepalive_secs: None,
            codec: None,
        };

        let mut sink = HttpSink::new(&cfg).expect("sink");
        sink.send(&sample_event())
            .await
            .expect("first event should enqueue");
        assert_eq!(captures.lock().expect("captures lock").len(), 0);

        sink.send(&sample_event())
            .await
            .expect("second event should trigger batch flush");

        tokio::time::sleep(Duration::from_millis(20)).await;
        let requests = captures.lock().expect("captures lock");
        assert_eq!(requests.len(), 1);
        assert!(
            extract_header(&requests[0], "idempotency-key").is_some(),
            "batch request should include Idempotency-Key"
        );
        assert!(requests[0].contains("[{"));
        assert_eq!(sink.accounting().accepted, 2);
        assert_eq!(sink.accounting().http_batch_size_samples_total, 1);
        assert_eq!(sink.accounting().http_requests_total, 1);
    }

    #[tokio::test]
    async fn records_oldest_event_age_and_pending_event_count_on_flush() {
        let statuses = Arc::new(Mutex::new(vec![200]));
        let captures = Arc::new(Mutex::new(Vec::new()));
        let url = spawn_http_server(statuses, captures.clone()).await;

        let cfg = HttpSinkConfig {
            url,
            timeout_ms: 1000,
            batch_max_events: 64,
            batch_max_delay_ms: 60_000,
            max_pending_bytes: 1024 * 1024,
            max_retries: 0,
            batch_retry_time_budget_ms: 30_000,
            backoff_initial_ms: 1,
            backoff_max_ms: 5,
            backoff_multiplier: 2.0,
            headers: std::collections::HashMap::new(),
            bearer_token: None,
            verify_tls: true,
            dlq_path: None,
            dlq_max_bytes: 128 * 1024 * 1024,
            pool_max_idle_per_host: 10,
            pool_idle_timeout_secs: None,
            tcp_keepalive_secs: None,
            codec: None,
        };

        let mut sink = HttpSink::new(&cfg).expect("sink");
        sink.send(&sample_event())
            .await
            .expect("event should enqueue");
        assert_eq!(
            sink.pending_events(),
            1,
            "event should be pending before flush"
        );

        tokio::time::sleep(Duration::from_millis(15)).await;
        sink.flush().await.expect("flush should succeed");

        assert_eq!(
            sink.pending_events(),
            0,
            "pending events should clear after flush"
        );
        assert_eq!(captures.lock().expect("captures lock").len(), 1);
        assert!(
            sink.accounting().http_batch_oldest_event_age_ms_last >= 10,
            "expected oldest pending age to be recorded on flush"
        );
        assert_eq!(sink.accounting().http_pending_bytes_current, 0);
        assert!(
            sink.accounting().http_pending_bytes_high_watermark > 0,
            "expected pending byte high-watermark to be recorded"
        );
    }

    #[tokio::test]
    async fn rejects_event_larger_than_max_pending_bytes() {
        let statuses = Arc::new(Mutex::new(vec![200]));
        let captures = Arc::new(Mutex::new(Vec::new()));
        let url = spawn_http_server(statuses, captures).await;

        let cfg = HttpSinkConfig {
            url,
            timeout_ms: 1000,
            batch_max_events: 64,
            batch_max_delay_ms: 60_000,
            max_pending_bytes: 1,
            max_retries: 0,
            batch_retry_time_budget_ms: 30_000,
            backoff_initial_ms: 1,
            backoff_max_ms: 5,
            backoff_multiplier: 2.0,
            headers: std::collections::HashMap::new(),
            bearer_token: None,
            verify_tls: true,
            dlq_path: None,
            dlq_max_bytes: 128 * 1024 * 1024,
            pool_max_idle_per_host: 10,
            pool_idle_timeout_secs: None,
            tcp_keepalive_secs: None,
            codec: None,
        };

        let mut sink = HttpSink::new(&cfg).expect("sink");
        let err = sink
            .send(&sample_event())
            .await
            .expect_err("event should fail when larger than max_pending_bytes");
        assert!(
            err.to_string().contains("max_pending_bytes"),
            "unexpected error: {err}"
        );
        assert_eq!(sink.accounting().http_pending_bytes_current, 0);
        assert_eq!(sink.accounting().http_pending_bytes_high_watermark, 0);
    }

    #[tokio::test]
    async fn salvages_terminal_batch_failure_via_per_event_retries() {
        let statuses = Arc::new(Mutex::new(vec![400, 200, 200]));
        let captures = Arc::new(Mutex::new(Vec::new()));
        let url = spawn_http_server(statuses, captures.clone()).await;

        let cfg = HttpSinkConfig {
            url,
            timeout_ms: 1000,
            batch_max_events: 2,
            batch_max_delay_ms: 60_000,
            max_pending_bytes: 1024 * 1024,
            max_retries: 0,
            batch_retry_time_budget_ms: 30_000,
            backoff_initial_ms: 1,
            backoff_max_ms: 1,
            backoff_multiplier: 2.0,
            headers: std::collections::HashMap::new(),
            bearer_token: None,
            verify_tls: true,
            dlq_path: None,
            dlq_max_bytes: 128 * 1024 * 1024,
            pool_max_idle_per_host: 10,
            pool_idle_timeout_secs: None,
            tcp_keepalive_secs: None,
            codec: None,
        };

        let mut sink = HttpSink::new(&cfg).expect("sink");
        sink.send(&sample_event())
            .await
            .expect("first event should enqueue");
        sink.send(&sample_event())
            .await
            .expect("second event should salvage after batch failure");

        let requests = captures.lock().expect("captures lock");
        assert_eq!(requests.len(), 3);
        for req in requests.iter() {
            assert!(
                extract_header(req, "idempotency-key").is_some(),
                "all batch/salvage requests must include Idempotency-Key"
            );
        }
        assert_eq!(sink.accounting().accepted, 2);
        assert_eq!(sink.accounting().terminal, 0);
    }

    #[tokio::test]
    async fn writes_dlq_on_terminal_failure() {
        let statuses = Arc::new(Mutex::new(vec![400]));
        let captures = Arc::new(Mutex::new(Vec::new()));
        let url = spawn_http_server(statuses, captures).await;

        let dir = tempfile::tempdir().expect("tempdir");
        let dlq = dir.path().join("failed.jsonl");

        let cfg = HttpSinkConfig {
            url,
            timeout_ms: 1000,
            batch_max_events: 64,
            batch_max_delay_ms: 10,
            max_pending_bytes: 1024 * 1024,
            max_retries: 1,
            batch_retry_time_budget_ms: 30_000,
            backoff_initial_ms: 1,
            backoff_max_ms: 1,
            backoff_multiplier: 2.0,
            headers: std::collections::HashMap::new(),
            bearer_token: None,
            verify_tls: true,
            dlq_path: Some(dlq.clone()),
            dlq_max_bytes: 128 * 1024 * 1024,
            pool_max_idle_per_host: 10,
            pool_idle_timeout_secs: None,
            tcp_keepalive_secs: None,
            codec: None,
        };

        let mut sink = HttpSink::new(&cfg).expect("sink");
        sink.send(&sample_event())
            .await
            .expect("event should enqueue before flush");
        let err = sink
            .flush()
            .await
            .expect_err("flush must fail and write dlq");
        assert!(err.to_string().contains("terminal HTTP status 400"));
        assert_eq!(sink.accounting().terminal, 1);
        assert_eq!(sink.accounting().terminal_status_4xx, 1);
        assert_eq!(sink.accounting().terminal_status_other, 0);
        assert_eq!(sink.accounting().terminal_error_timeout, 0);
        assert_eq!(sink.accounting().terminal_error_other, 0);

        let dlq_data = std::fs::read_to_string(dlq).expect("read dlq");
        assert!(dlq_data.contains("terminal HTTP status 400"));
        assert!(dlq_data.contains("\"table\":\"users\""));
    }

    #[tokio::test]
    async fn fails_closed_when_dlq_size_limit_would_be_exceeded() {
        let statuses = Arc::new(Mutex::new(vec![400]));
        let captures = Arc::new(Mutex::new(Vec::new()));
        let url = spawn_http_server(statuses, captures).await;

        let dir = tempfile::tempdir().expect("tempdir");
        let dlq = dir.path().join("failed.jsonl");

        let cfg = HttpSinkConfig {
            url,
            timeout_ms: 1000,
            batch_max_events: 64,
            batch_max_delay_ms: 10,
            max_pending_bytes: 1024 * 1024,
            max_retries: 0,
            batch_retry_time_budget_ms: 30_000,
            backoff_initial_ms: 1,
            backoff_max_ms: 1,
            backoff_multiplier: 2.0,
            headers: std::collections::HashMap::new(),
            bearer_token: None,
            verify_tls: true,
            dlq_path: Some(dlq.clone()),
            dlq_max_bytes: 1,
            pool_max_idle_per_host: 10,
            pool_idle_timeout_secs: None,
            tcp_keepalive_secs: None,
            codec: None,
        };

        let mut sink = HttpSink::new(&cfg).expect("sink");
        sink.send(&sample_event())
            .await
            .expect("event should enqueue before flush");
        let err = sink
            .flush()
            .await
            .expect_err("flush must fail closed on dlq size limit");
        assert!(
            err.to_string().contains("DLQ size limit exceeded"),
            "unexpected error: {err}"
        );
        assert_eq!(sink.accounting().terminal, 1);
        assert_eq!(sink.accounting().dlq_written, 0);
    }

    #[tokio::test]
    async fn sends_authorization_and_custom_headers() {
        let statuses = Arc::new(Mutex::new(vec![200]));
        let captures = Arc::new(Mutex::new(Vec::new()));
        let url = spawn_http_server(statuses, captures.clone()).await;

        let mut headers = std::collections::HashMap::new();
        headers.insert("x-cdc-source".to_string(), "unit-test".to_string());
        let cfg = HttpSinkConfig {
            url,
            timeout_ms: 1000,
            batch_max_events: 64,
            batch_max_delay_ms: 10,
            max_pending_bytes: 1024 * 1024,
            max_retries: 0,
            batch_retry_time_budget_ms: 30_000,
            backoff_initial_ms: 1,
            backoff_max_ms: 1,
            backoff_multiplier: 2.0,
            headers,
            bearer_token: Some(SecretString::from_callback("http-test-token", || {
                Ok("top-secret-token".to_string())
            })),
            verify_tls: true,
            dlq_path: None,
            dlq_max_bytes: 128 * 1024 * 1024,
            pool_max_idle_per_host: 10,
            pool_idle_timeout_secs: None,
            tcp_keepalive_secs: None,
            codec: None,
        };

        let mut sink = HttpSink::new(&cfg).expect("sink");
        sink.send(&sample_event())
            .await
            .expect("delivery should succeed");
        sink.flush().await.expect("flush should succeed");

        let requests = captures.lock().expect("captures lock");
        assert_eq!(requests.len(), 1);
        let req = requests[0].to_ascii_lowercase();
        assert!(req.contains("authorization: bearer top-secret-token"));
        assert!(req.contains("x-cdc-source: unit-test"));
    }

    #[test]
    fn rejects_insecure_tls_bypass() {
        let cfg = HttpSinkConfig {
            url: "http://127.0.0.1:8080".to_string(),
            timeout_ms: 1000,
            batch_max_events: 64,
            batch_max_delay_ms: 10,
            max_pending_bytes: 1024 * 1024,
            max_retries: 0,
            batch_retry_time_budget_ms: 30_000,
            backoff_initial_ms: 1,
            backoff_max_ms: 1,
            backoff_multiplier: 2.0,
            headers: std::collections::HashMap::new(),
            bearer_token: None,
            verify_tls: false,
            dlq_path: None,
            dlq_max_bytes: 128 * 1024 * 1024,
            pool_max_idle_per_host: 10,
            pool_idle_timeout_secs: None,
            tcp_keepalive_secs: None,
            codec: None,
        };

        let err = match HttpSink::new(&cfg) {
            Ok(_) => panic!("insecure TLS bypass must be rejected"),
            Err(err) => err,
        };
        assert!(err.to_string().contains("verify_tls must be true"));
    }

    #[test]
    fn rejects_non_loopback_http_endpoint_even_when_tls_is_enabled() {
        let cfg = HttpSinkConfig {
            url: "http://example.com/events".to_string(),
            timeout_ms: 1000,
            batch_max_events: 64,
            batch_max_delay_ms: 10,
            max_pending_bytes: 1024 * 1024,
            max_retries: 0,
            batch_retry_time_budget_ms: 30_000,
            backoff_initial_ms: 1,
            backoff_max_ms: 1,
            backoff_multiplier: 2.0,
            headers: std::collections::HashMap::new(),
            bearer_token: None,
            verify_tls: true,
            dlq_path: None,
            dlq_max_bytes: 128 * 1024 * 1024,
            pool_max_idle_per_host: 10,
            pool_idle_timeout_secs: None,
            tcp_keepalive_secs: None,
            codec: None,
        };

        let err = match HttpSink::new(&cfg) {
            Ok(_) => panic!("non-loopback http endpoint must be rejected"),
            Err(err) => err,
        };
        assert!(err.to_string().contains("must use https"));
    }

    #[test]
    fn preserves_fractional_backoff_growth() {
        let cfg = HttpSinkConfig {
            url: "https://example.com/events".to_string(),
            timeout_ms: 1000,
            batch_max_events: 64,
            batch_max_delay_ms: 10,
            max_pending_bytes: 1024 * 1024,
            max_retries: 0,
            batch_retry_time_budget_ms: 30_000,
            backoff_initial_ms: 1,
            backoff_max_ms: 10,
            backoff_multiplier: 1.5,
            headers: std::collections::HashMap::new(),
            bearer_token: None,
            verify_tls: true,
            dlq_path: None,
            dlq_max_bytes: 128 * 1024 * 1024,
            pool_max_idle_per_host: 10,
            pool_idle_timeout_secs: None,
            tcp_keepalive_secs: None,
            codec: None,
        };

        let sink = HttpSink::new(&cfg).expect("sink");
        assert_eq!(sink.next_backoff(1), Duration::from_millis(2));
    }

    #[test]
    fn rejects_non_finite_backoff_multiplier() {
        let cfg = HttpSinkConfig {
            url: "https://example.com/events".to_string(),
            timeout_ms: 1000,
            batch_max_events: 64,
            batch_max_delay_ms: 10,
            max_pending_bytes: 1024 * 1024,
            max_retries: 0,
            batch_retry_time_budget_ms: 30_000,
            backoff_initial_ms: 1,
            backoff_max_ms: 1,
            backoff_multiplier: f64::NAN,
            headers: std::collections::HashMap::new(),
            bearer_token: None,
            verify_tls: true,
            dlq_path: None,
            dlq_max_bytes: 128 * 1024 * 1024,
            pool_max_idle_per_host: 10,
            pool_idle_timeout_secs: None,
            tcp_keepalive_secs: None,
            codec: None,
        };

        let err = match HttpSink::new(&cfg) {
            Ok(_) => panic!("non-finite backoff multiplier must be rejected"),
            Err(err) => err,
        };
        assert!(err.to_string().contains("must be finite"));
    }
}
