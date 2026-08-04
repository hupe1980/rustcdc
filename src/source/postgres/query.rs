use std::time::Duration;

use tokio_postgres::Client;

#[cfg(feature = "tls")]
use rustls::pki_types::{pem::PemObject as _, CertificateDer, PrivateKeyDer};

use crate::core::{Error, Result};

use super::parser::{format_pg_lsn, parse_pg_lsn};

/// Abstraction over the two Postgres I/O operations required by startup slot
/// reconciliation.  Allows the self-heal logic to be unit-tested without a
/// live database connection.
pub(super) trait ReconcileOps {
    async fn query_confirmed_lsn(&self, slot_name: &str) -> Result<u64>;
    async fn advance_slot(&self, slot_name: &str, lsn: u64) -> Result<()>;
}

impl ReconcileOps for Client {
    async fn query_confirmed_lsn(&self, slot_name: &str) -> Result<u64> {
        query_slot_confirmed_lsn(self, slot_name).await
    }

    async fn advance_slot(&self, slot_name: &str, lsn: u64) -> Result<()> {
        advance_replication_slot(self, slot_name, lsn).await
    }
}

pub(super) async fn query_primary_key_columns_and_types(
    client: &Client,
    schema: &str,
    table: &str,
) -> Result<(Vec<String>, Vec<String>)> {
    let rows = client
        .query(
            "
            SELECT
              attribute.attname,
              pg_catalog.format_type(attribute.atttypid, attribute.atttypmod)
            FROM pg_catalog.pg_index index_def
            JOIN pg_catalog.pg_class class_def ON class_def.oid = index_def.indrelid
            JOIN pg_catalog.pg_namespace namespace_def ON namespace_def.oid = class_def.relnamespace
            JOIN LATERAL unnest(index_def.indkey) WITH ORDINALITY AS key_attnum(attnum, ord) ON TRUE
            JOIN pg_catalog.pg_attribute attribute
              ON attribute.attrelid = index_def.indrelid
             AND attribute.attnum = key_attnum.attnum
            WHERE index_def.indisprimary
              AND namespace_def.nspname = $1
              AND class_def.relname = $2
            ORDER BY key_attnum.ord
            ",
            &[&schema, &table],
        )
        .await
        .map_err(|error| {
            Error::SourceError(format!(
                "failed querying primary key columns for table '{schema}.{table}': {error}"
            ))
        })?;

    let mut columns = Vec::with_capacity(rows.len());
    let mut types = Vec::with_capacity(rows.len());
    for row in rows {
        columns.push(row.get::<usize, String>(0));
        types.push(row.get::<usize, String>(1));
    }

    Ok((columns, types))
}

pub(super) async fn reconcile_stream_resume_lsn_with_retry(
    client: &Client,
    checkpoint_lsn: u64,
    slot_name: &str,
    attempts: usize,
    retry_delay: Duration,
) -> Result<u64> {
    reconcile_with_ops(client, checkpoint_lsn, slot_name, attempts, retry_delay).await
}

/// Core reconciliation logic, decoupled from I/O for unit-testability.
/// See [`reconcile_stream_resume_lsn_with_retry`] for the production entry point.
async fn reconcile_with_ops(
    ops: &impl ReconcileOps,
    checkpoint_lsn: u64,
    slot_name: &str,
    attempts: usize,
    retry_delay: Duration,
) -> Result<u64> {
    let attempts = attempts.max(1);
    let mut last_slot_lsn = 0_u64;

    for attempt in 0..attempts {
        let slot_lsn = ops.query_confirmed_lsn(slot_name).await?;
        last_slot_lsn = slot_lsn;
        if checkpoint_lsn <= slot_lsn {
            return Ok(checkpoint_lsn);
        }

        if attempt + 1 < attempts {
            tokio::time::sleep(retry_delay).await;
        }
    }

    // The checkpoint is ahead of the slot's confirmed_flush_lsn.  This happens
    // when a previous `confirm_lsn` call succeeded at the checkpoint layer but
    // failed to advance the replication slot (e.g. transient network error,
    // Postgres restart, or the type-casting bug fixed in 0.6.4).  Rather than
    // returning a fatal "operator intervention required" error that causes an
    // infinite restart loop, self-heal by advancing the slot to the checkpoint
    // position.  The checkpoint guarantees those events were durably processed,
    // so advancing the slot is safe and correct.
    tracing::warn!(
        target: "rustcdc::source::postgres",
        slot_name,
        checkpoint_lsn = %format_pg_lsn(checkpoint_lsn),
        slot_confirmed_lsn = %format_pg_lsn(last_slot_lsn),
        "replication slot behind checkpoint after confirm_lsn failure; \
         self-healing by advancing slot to checkpoint LSN",
    );
    ops.advance_slot(slot_name, checkpoint_lsn).await?;
    Ok(checkpoint_lsn)
}

/// Advance a replication slot to the given LSN.  Used both during startup
/// self-healing (see `reconcile_stream_resume_lsn_with_retry`) and by
/// [`super::decoder::LivePgOutputMessageProvider::confirm_lsn`].
pub(super) async fn advance_replication_slot(
    client: &Client,
    slot_name: &str,
    lsn: u64,
) -> Result<()> {
    let lsn_str = format_pg_lsn(lsn);
    client
        .query(
            "SELECT 1 FROM pg_replication_slot_advance($1::text::name, $2::text::pg_lsn)",
            &[&slot_name.to_string(), &lsn_str],
        )
        .await
        .map_err(|error| {
            Error::SourceError(format!(
                "failed to advance replication slot '{slot_name}' to LSN {lsn_str}: {error}"
            ))
        })?;
    Ok(())
}

pub(super) async fn query_current_wal_lsn(client: &Client) -> Result<u64> {
    let lsn: String = client
        .query_one("SELECT pg_current_wal_lsn()::text", &[])
        .await
        .map_err(|error| Error::SourceError(format!("failed querying WAL LSN: {error}")))?
        .get(0);
    parse_pg_lsn(&lsn)
}

async fn query_slot_confirmed_lsn(client: &Client, slot_name: &str) -> Result<u64> {
    let row = client
        .query_opt(
            "SELECT confirmed_flush_lsn::text FROM pg_catalog.pg_replication_slots WHERE slot_name = $1",
            &[&slot_name],
        )
        .await
        .map_err(|error| {
            Error::SourceError(format!(
                "failed querying replication slot state for '{slot_name}': {error}"
            ))
        })?
        .ok_or_else(|| {
            Error::SourceError(format!(
                "replication slot '{slot_name}' not found while validating checkpoint alignment"
            ))
        })?;

    let lsn_text = row.get::<usize, Option<String>>(0).ok_or_else(|| {
        Error::SourceError(format!(
            "replication slot '{slot_name}' has no confirmed_flush_lsn"
        ))
    })?;
    parse_pg_lsn(&lsn_text)
}

#[cfg(feature = "tls")]
pub(super) fn build_tls_root_store(ca_cert_path: Option<&str>) -> Result<rustls::RootCertStore> {
    let mut root_store = rustls::RootCertStore::empty();

    if let Some(path) = ca_cert_path {
        let pem_bytes = std::fs::read(path).map_err(|error| {
            Error::ConfigError(format!(
                "failed to read TLS CA certificate file '{path}': {error}"
            ))
        })?;
        // `rustls_pki_types::pem`, not the archived `rustls-pemfile`: the latter has
        // been unmaintained since August 2025 (RUSTSEC-2025-0134) and its last release is
        // a thin wrapper over exactly this code.
        let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_slice_iter(&pem_bytes)
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| {
                Error::ConfigError(format!(
                    "failed to parse TLS CA certificate PEM in '{path}': {error}"
                ))
            })?;

        if certs.is_empty() {
            return Err(Error::ConfigError(format!(
                "TLS CA certificate file '{path}' contains no valid PEM certificates"
            )));
        }

        for cert in certs {
            root_store.add(cert).map_err(|error| {
                Error::ConfigError(format!(
                    "TLS CA certificate in '{path}' is invalid: {error}"
                ))
            })?;
        }
    } else {
        let native_certs = rustls_native_certs::load_native_certs();
        for err in &native_certs.errors {
            tracing::warn!(
                target: "rustcdc::source::postgres",
                "failed to load a native root certificate: {err}"
            );
        }
        for cert in native_certs.certs {
            if let Err(err) = root_store.add(cert) {
                tracing::debug!(
                    target: "rustcdc::source::postgres",
                    "skipping invalid native root certificate: {err}"
                );
            }
        }
    }

    Ok(root_store)
}

/// Return the process-level `CryptoProvider` if the embedder has registered one
/// via [`rustls::crypto::CryptoProvider::install_default`], otherwise fall back
/// to `ring`. Using a registered default lets embedders choose `aws-lc-rs` (e.g.
/// for FIPS) without requiring source-level changes in rustcdc.
///
/// This function must never call `install_default()` — that is process-global
/// mutation that belongs solely to the embedder / binary entry point.
#[cfg(feature = "tls")]
fn resolve_crypto_provider() -> std::sync::Arc<rustls::crypto::CryptoProvider> {
    rustls::crypto::CryptoProvider::get_default()
        .cloned()
        .unwrap_or_else(|| rustls::crypto::ring::default_provider().into())
}

/// Build a `rustls::ClientConfig` with optional mTLS client certificate.
///
/// When `client_cert_path` and `client_key_path` are both `Some`, mutual TLS
/// authentication is configured using the supplied PEM-encoded certificate and
/// private key. Otherwise, server-auth-only TLS is used.
#[cfg(feature = "tls")]
pub(super) fn build_tls_client_config(
    ca_cert_path: Option<&str>,
    client_cert_path: Option<&str>,
    client_key_path: Option<&str>,
) -> Result<rustls::ClientConfig> {
    let root_store = build_tls_root_store(ca_cert_path)?;

    match (client_cert_path, client_key_path) {
        (Some(cert_path), Some(key_path)) => {
            let cert_pem = std::fs::read(cert_path).map_err(|error| {
                Error::ConfigError(format!(
                    "failed to read mTLS client certificate '{cert_path}': {error}"
                ))
            })?;
            let key_pem = std::fs::read(key_path).map_err(|error| {
                Error::ConfigError(format!(
                    "failed to read mTLS client private key '{key_path}': {error}"
                ))
            })?;

            let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_slice_iter(&cert_pem)
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|error| {
                    Error::ConfigError(format!(
                        "failed to parse mTLS client certificate PEM '{cert_path}': {error}"
                    ))
                })?;

            if certs.is_empty() {
                return Err(Error::ConfigError(format!(
                    "mTLS client certificate file '{cert_path}' contains no valid certificates"
                )));
            }

            let key = PrivateKeyDer::from_pem_slice(&key_pem).map_err(|error| {
                Error::ConfigError(format!(
                    "failed to parse mTLS private key PEM '{key_path}': {error}. The file \
                     must contain exactly one PKCS#8, PKCS#1 or SEC1 private key."
                ))
            })?;

            rustls::ClientConfig::builder_with_provider(resolve_crypto_provider())
                .with_safe_default_protocol_versions()
                .map_err(|error| {
                    Error::ConfigError(format!("TLS protocol configuration failed: {error}"))
                })?
                .with_root_certificates(root_store)
                .with_client_auth_cert(certs, key)
                .map_err(|error| {
                    Error::ConfigError(format!(
                        "mTLS client certificate configuration failed: {error}"
                    ))
                })
        }
        (Some(_), None) => Err(Error::ConfigError(
            "mTLS requires both client_cert_path and client_key_path".into(),
        )),
        (None, Some(_)) => Err(Error::ConfigError(
            "mTLS requires both client_cert_path and client_key_path".into(),
        )),
        (None, None) => {
            let config = rustls::ClientConfig::builder_with_provider(resolve_crypto_provider())
                .with_safe_default_protocol_versions()
                .map_err(|error| {
                    Error::ConfigError(format!("TLS protocol configuration failed: {error}"))
                })?
                .with_root_certificates(root_store)
                .with_no_client_auth();
            Ok(config)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;

    // ── Mock ReconcileOps ─────────────────────────────────────────────────────

    struct MockReconcileOps {
        /// LSN values returned by successive `query_confirmed_lsn` calls (FIFO).
        slot_lsn_sequence: Arc<Mutex<Vec<u64>>>,
        /// Records each `(slot_name, lsn)` pair passed to `advance_slot`.
        advance_calls: Arc<Mutex<Vec<(String, u64)>>>,
    }

    impl MockReconcileOps {
        fn new(slot_lsns: Vec<u64>) -> Self {
            Self {
                slot_lsn_sequence: Arc::new(Mutex::new(slot_lsns)),
                advance_calls: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn advance_calls_snapshot(&self) -> Vec<(String, u64)> {
            self.advance_calls.lock().unwrap().clone()
        }
    }

    impl ReconcileOps for MockReconcileOps {
        async fn query_confirmed_lsn(&self, _slot_name: &str) -> Result<u64> {
            let mut seq = self.slot_lsn_sequence.lock().unwrap();
            if seq.is_empty() {
                return Err(Error::SourceError(
                    "mock: no more slot LSN values configured".into(),
                ));
            }
            Ok(seq.remove(0))
        }

        async fn advance_slot(&self, slot_name: &str, lsn: u64) -> Result<()> {
            self.advance_calls
                .lock()
                .unwrap()
                .push((slot_name.to_string(), lsn));
            Ok(())
        }
    }

    // ── reconcile_with_ops tests ──────────────────────────────────────────────

    /// Normal path: checkpoint == slot_lsn → returns immediately, no advance.
    #[tokio::test]
    async fn reconcile_returns_checkpoint_when_slot_equals_checkpoint() {
        let ops = MockReconcileOps::new(vec![100]);
        let result = reconcile_with_ops(&ops, 100, "demo_slot", 3, Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(result, 100);
        assert!(
            ops.advance_calls_snapshot().is_empty(),
            "no advance when slot == checkpoint"
        );
    }

    /// Normal path: slot ahead of checkpoint → returns checkpoint, no advance.
    #[tokio::test]
    async fn reconcile_returns_checkpoint_when_slot_is_ahead() {
        let ops = MockReconcileOps::new(vec![200]); // slot at 200, checkpoint at 100
        let result = reconcile_with_ops(&ops, 100, "demo_slot", 1, Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(result, 100);
        assert!(
            ops.advance_calls_snapshot().is_empty(),
            "no advance when slot is ahead of checkpoint"
        );
    }

    /// Self-heal path: checkpoint > slot after all retries → advance is called.
    #[tokio::test]
    async fn reconcile_self_heals_when_checkpoint_ahead_of_slot() {
        let ops = MockReconcileOps::new(vec![50]); // slot at 50, checkpoint at 100
        let result = reconcile_with_ops(&ops, 100, "demo_slot", 1, Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(result, 100, "self-heal must return checkpoint_lsn");
        let calls = ops.advance_calls_snapshot();
        assert_eq!(calls.len(), 1, "advance must be called exactly once");
        assert_eq!(
            calls[0],
            ("demo_slot".to_string(), 100),
            "advance must target checkpoint_lsn"
        );
    }

    /// Retry path: slot catches up on second attempt → returns without advancing.
    #[tokio::test]
    async fn reconcile_short_circuits_when_slot_catches_up_during_retry() {
        // First query: slot behind. Second query: slot caught up.
        let ops = MockReconcileOps::new(vec![50, 100]);
        let result = reconcile_with_ops(&ops, 100, "demo_slot", 3, Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(result, 100);
        assert!(
            ops.advance_calls_snapshot().is_empty(),
            "no advance when slot eventually catches up within retry budget"
        );
    }

    /// Retry exhaustion: slot stays behind across all attempts → single advance.
    #[tokio::test]
    async fn reconcile_advances_once_after_all_retries_fail() {
        let ops = MockReconcileOps::new(vec![50, 50, 50]);
        let result = reconcile_with_ops(&ops, 100, "demo_slot", 3, Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(result, 100);
        let calls = ops.advance_calls_snapshot();
        assert_eq!(
            calls.len(),
            1,
            "advance must be called exactly once after retries"
        );
        assert_eq!(calls[0].1, 100);
    }
}
