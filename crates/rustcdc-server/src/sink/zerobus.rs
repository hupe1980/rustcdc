//! Databricks Zerobus Ingest — push events straight into a Unity Catalog Delta table.
//!
//! # Why this sink is thin
//!
//! Databricks ships an official Rust SDK (`databricks-zerobus-ingest-sdk`, Apache-2.0), so
//! unlike the Snowflake sink — which is hand-rolled over REST — the protocol, the OAuth
//! exchange, back-pressure and stream recovery are all upstream. What is left here is the
//! part that is *this project's* problem: deciding when a flush is durable, and saying
//! honestly what contract that buys.
//!
//! # `at_least_once`, and why not more
//!
//! The Snowflake sink reaches `effectively_once` because a Snowpipe Streaming channel's
//! offset token is a **durable, queryable, destination-side** record of what has been
//! committed — so after a crash the sink can ask, and skip what landed.
//!
//! Zerobus has no such thing. Its streams are *ephemeral*: `zerobus_service.proto` reserves
//! `last_offset_id` and documents reopening a stream by `stream_id` as `NOT SUPPORTED`.
//! Offsets are meaningful only inside one stream's lifetime, so a replayed batch is
//! re-ingested and there is nowhere to look to find out.
//!
//! What the acknowledgement *does* buy is the absence of loss.
//! `IngestRecordResponse.durability_ack_up_to_offset` means every record at or below that
//! offset is durable, and `flush()` does not return until the last record it queued is
//! covered. That is precisely `at_least_once` — duplicates possible, loss not — and it is
//! reported as such rather than dressed up.
//!
//! # The trap this inherits
//!
//! `flush()` blocking on a durability acknowledgement is the same shape as the Snowflake
//! commit wait, and it inherits the same failure: `runtime.sink_flush_timeout_ms` wraps the
//! whole flush, so if the sink's own `ack_timeout_ms` is not comfortably below it the
//! runtime cancels the wait *after* the records are queued. The loader refuses that
//! combination — see `validate_zerobus_ack_wait_fits_the_flush_timeout`.

use std::sync::Arc;
use std::time::Duration;

use databricks_zerobus_ingest_sdk::{
    JsonString, NoAuthHeadersProvider, OffsetId, ZerobusSdk, ZerobusStream,
};
use rustcdc::core::{Error as RtError, Event};

use crate::config::sink::{ZerobusAuthConfig, ZerobusSinkConfig};
use crate::error::AppError;

/// Counters this sink publishes through `SinkDeliveryMetrics`.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct ZerobusAccounting {
    pub(crate) records_ingested: u64,
    pub(crate) ack_waits: u64,
    pub(crate) ack_wait_ms_total: u64,
    pub(crate) stream_opens: u64,
}

pub struct ZerobusSink {
    config: ZerobusSinkConfig,
    /// Built at construction; the stream is opened lazily at preflight so a configuration
    /// error surfaces before the pipeline is marked ready.
    sdk: ZerobusSdk,
    stream: Option<ZerobusStream>,
    /// Highest offset handed back by `ingest_record_offset` since the last flush.
    ///
    /// `flush()` waits for exactly this one: the acknowledgement is cumulative
    /// (`durability_ack_up_to_offset`), so waiting for the newest record covers every record
    /// before it and costs one wait rather than one per record.
    pending_offset: Option<OffsetId>,
    pending_records: usize,
    max_event_bytes: usize,
    accounting: ZerobusAccounting,
    closed: bool,
}

impl ZerobusSink {
    pub async fn new(config: &ZerobusSinkConfig, max_event_bytes: usize) -> Result<Self, AppError> {
        config.validate().map_err(AppError::Other)?;

        let mut builder = ZerobusSdk::builder()
            .endpoint(config.endpoint.trim())
            .unity_catalog_url(config.unity_catalog_url.trim());

        // Plaintext only reaches here for a loopback endpoint — the loader refuses it for
        // anything else, because the OAuth client secret crosses this connection.
        if !config.endpoint.trim().starts_with("https://") {
            builder = builder.no_tls();
        }

        let sdk = builder
            .build()
            .map_err(|e| AppError::Other(format!("failed to build the Zerobus client: {e}")))?;

        Ok(Self {
            config: config.clone(),
            sdk,
            stream: None,
            pending_offset: None,
            pending_records: 0,
            max_event_bytes,
            accounting: ZerobusAccounting::default(),
            closed: false,
        })
    }

    pub(crate) fn accounting(&self) -> ZerobusAccounting {
        self.accounting
    }

    pub(crate) fn pending_records(&self) -> usize {
        self.pending_records
    }

    pub(crate) fn flush_tick_interval(&self) -> Duration {
        Duration::from_millis(self.config.flush_interval_ms)
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.closed
    }

    /// Open the ingest stream.
    ///
    /// Done at preflight rather than on the first event so a wrong table name, a revoked
    /// service principal or an unreachable endpoint fails before the instance reports ready
    /// — which is the whole point of the preflight hook.
    pub(crate) async fn preflight(&mut self) -> Result<(), AppError> {
        self.open_stream()
            .await
            .map_err(|e| AppError::Other(format!("Zerobus sink preflight: {e}")))
    }

    async fn open_stream(&mut self) -> Result<(), RtError> {
        let mut builder = self
            .sdk
            .stream_builder()
            .table(self.config.table.trim())
            // JSON rather than protobuf: the event already *is* JSON, and the protobuf path
            // would mean deriving a descriptor from the Delta table's schema and keeping it
            // in step with it. The envelope this sink writes is fixed, so there is nothing
            // a descriptor would buy beyond a second thing to drift.
            .json()
            .max_inflight_requests(self.config.max_inflight_records)
            .flush_timeout_ms(self.config.ack_timeout_ms)
            .recovery(self.config.recovery_enabled);

        builder = match &self.config.auth {
            ZerobusAuthConfig::Oauth {
                client_id,
                client_secret,
            } => {
                let secret = client_secret.resolve().map_err(|e| {
                    RtError::ConfigError(format!("sink.zerobus.auth.client_secret: {e}"))
                })?;
                builder.oauth(client_id.trim(), secret)
            }
            ZerobusAuthConfig::NoAuth => builder.headers_provider(Arc::new(NoAuthHeadersProvider)),
        };

        let stream = builder
            .build()
            .await
            .map_err(|e| RtError::SourceError(format!("failed to open a Zerobus stream: {e}")))?;

        self.accounting.stream_opens += 1;
        tracing::info!(
            table = %self.config.table,
            "Zerobus ingest stream opened"
        );
        self.stream = Some(stream);
        Ok(())
    }

    /// Queue one event. Returns as soon as the SDK accepts it — durability comes at flush.
    pub(crate) async fn append_event(&mut self, event: &Event) -> Result<(), RtError> {
        if self.closed {
            return Err(RtError::StateError("Zerobus sink is closed".to_string()));
        }

        let row = serde_json::to_string(event)
            .map_err(|e| RtError::SourceError(format!("failed to encode an event as JSON: {e}")))?;

        // Enforced on the bytes that go on the wire. This sink produces the transmitted
        // payload itself, so `SinkBinding` does not encode a second copy to measure — see
        // `BuiltSink::enforces_own_size_limit`.
        if row.len() > self.max_event_bytes {
            return Err(RtError::ConfigError(format!(
                "encoded event payload size {} exceeds runtime.max_event_bytes {}",
                row.len(),
                self.max_event_bytes
            )));
        }

        let stream = self.stream.as_ref().ok_or_else(|| {
            RtError::StateError(
                "the Zerobus stream is not open; preflight_check must run first".to_string(),
            )
        })?;

        let offset = stream
            .ingest_record_offset(JsonString(row))
            .await
            .map_err(map_zerobus_error)?;

        self.pending_offset = Some(offset);
        self.pending_records += 1;
        self.accounting.records_ingested += 1;
        Ok(())
    }

    /// Wait until everything queued since the last flush is durable.
    ///
    /// This is the whole contract. `ingest_record_offset` returning does **not** mean the
    /// record is in the table — it means the SDK has it. Returning here without the
    /// acknowledgement would let the pipeline checkpoint past records that a process exit
    /// would lose.
    pub(crate) async fn flush_pending(&mut self) -> Result<(), RtError> {
        let Some(offset) = self.pending_offset.take() else {
            return Ok(());
        };
        let stream = self
            .stream
            .as_ref()
            .ok_or_else(|| RtError::StateError("the Zerobus stream is not open".to_string()))?;

        let started = std::time::Instant::now();
        self.accounting.ack_waits += 1;

        // One wait for the newest offset, not one per record: the acknowledgement is
        // cumulative — `durability_ack_up_to_offset` covers every record at or below it.
        let outcome = tokio::time::timeout(
            Duration::from_millis(self.config.ack_timeout_ms),
            stream.wait_for_offset(offset),
        )
        .await;

        self.accounting.ack_wait_ms_total += started.elapsed().as_millis() as u64;
        self.pending_records = 0;

        match outcome {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => {
                // Put the offset back: the batch is retried, and a later flush must wait for
                // this same acknowledgement rather than assume it arrived.
                self.pending_offset = Some(offset);
                Err(map_zerobus_error(error))
            }
            Err(_) => {
                self.pending_offset = Some(offset);
                Err(RtError::TimeoutError(format!(
                    "Zerobus did not acknowledge durability within {} ms for table '{}'. \
                     Failing the flush rather than letting the checkpoint advance past \
                     records that are not durable.",
                    self.config.ack_timeout_ms, self.config.table
                )))
            }
        }
    }

    pub(crate) async fn close(&mut self) -> Result<(), RtError> {
        if self.closed {
            return Ok(());
        }
        self.flush_pending().await?;
        if let Some(stream) = self.stream.as_mut() {
            stream.close().await.map_err(map_zerobus_error)?;
        }
        self.closed = true;
        Ok(())
    }
}

/// Map an SDK error onto this project's recoverable/terminal split.
///
/// Matched on **variants**, not on the text of the message. The first draft lowercased
/// `to_string()` and looked for substrings like `"unauthenticated"` — which silently
/// reclassifies every error the day upstream rewords one, and which this project has
/// already been bitten by elsewhere.
///
/// The split decides retry-or-halt and both wrong answers cost: a revoked service principal
/// retried forever drains the change stream against a wall, and a dropped connection treated
/// as terminal turns a blip into an outage.
fn map_zerobus_error(error: databricks_zerobus_ingest_sdk::ZerobusError) -> RtError {
    use databricks_zerobus_ingest_sdk::ZerobusError as Z;

    let terminal = |text: String| RtError::ConfigError(text);
    let retry = |text: String| RtError::SourceError(text);

    match &error {
        // Configuration and identity. Time does not fix any of these.
        Z::InvalidZerobusEndpointError(_)
        | Z::InvalidUCEndpointError(_)
        | Z::InvalidUCTokenError(_)
        | Z::InvalidTableName(_)
        | Z::InvalidArgument(_)
        | Z::InvalidSchema { .. } => terminal(format!(
            "Zerobus rejected the stream and retrying will not change it: {error}"
        )),

        // A gRPC status carries the answer; ask it rather than guessing from prose.
        Z::CreateStreamError(status) | Z::StreamClosedError(status) => {
            if is_terminal_status(status.code()) {
                terminal(format!(
                    "Zerobus refused the stream ({}): {error}",
                    status.code()
                ))
            } else {
                retry(format!("Zerobus stream error ({}): {error}", status.code()))
            }
        }

        // Transport, timing and token refresh — all transient by nature.
        Z::ChannelCreationError(_)
        | Z::FailedToEstablishTlsConnectionError
        | Z::ConnectionTimeout(_)
        | Z::TokenFetchError(_)
        | Z::UnexpectedStreamResponseError(_)
        | Z::InvalidStateError(_) => retry(format!("Zerobus ingest error: {error}")),

        // `ZerobusError` is not `#[non_exhaustive]` today, so this arm is unreachable — and
        // it is here anyway, defaulting to *retry*, because the alternative on a future
        // upstream variant is a pipeline that halts on something transient.
        _ => retry(format!("Zerobus ingest error: {error}")),
    }
}

/// Is this gRPC status the operator's problem rather than time's?
fn is_terminal_status(code: tonic::Code) -> bool {
    matches!(
        code,
        tonic::Code::Unauthenticated
            | tonic::Code::PermissionDenied
            | tonic::Code::NotFound
            | tonic::Code::InvalidArgument
            | tonic::Code::FailedPrecondition
            | tonic::Code::Unimplemented
    )
}

#[cfg(test)]
mod tests {
    /// The error split decides retry-or-halt, and both wrong answers cost: a revoked service
    /// principal retried forever drains the change stream against a wall, and a dropped
    /// connection treated as terminal turns a blip into an outage.
    #[test]
    fn configuration_failures_are_terminal_and_transport_failures_retry() {
        use databricks_zerobus_ingest_sdk::ZerobusError as Z;

        let terminal = [
            Z::InvalidTableName("a.b".to_string()),
            Z::InvalidUCTokenError("expired".to_string()),
            Z::InvalidArgument("bad".to_string()),
            Z::CreateStreamError(tonic::Status::unauthenticated("token expired")),
            Z::StreamClosedError(tonic::Status::permission_denied("no grant on pipe")),
        ];
        for error in terminal {
            let mapped = super::map_zerobus_error(error.clone());
            assert!(
                matches!(mapped, rustcdc::core::Error::ConfigError(_)),
                "{error:?} must be terminal, got {mapped:?}"
            );
        }

        let recoverable = [
            Z::ConnectionTimeout("10s".to_string()),
            Z::ChannelCreationError("connect refused".to_string()),
            Z::TokenFetchError("idp 503".to_string()),
            Z::StreamClosedError(tonic::Status::unavailable("server restarting")),
            Z::CreateStreamError(tonic::Status::deadline_exceeded("slow")),
        ];
        for error in recoverable {
            let mapped = super::map_zerobus_error(error.clone());
            assert!(
                matches!(mapped, rustcdc::core::Error::SourceError(_)),
                "{error:?} must be retried, got {mapped:?}"
            );
        }
    }
}
