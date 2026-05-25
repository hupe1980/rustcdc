use std::time::Duration;

use tokio_postgres::Client;

use crate::core::{Error, Result};

use super::parser::{parse_pg_lsn, reconcile_stream_resume_lsn};

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
    let attempts = attempts.max(1);
    let mut last_slot_lsn = 0_u64;

    for attempt in 0..attempts {
        let slot_lsn = query_slot_confirmed_lsn(client, slot_name).await?;
        last_slot_lsn = slot_lsn;
        if checkpoint_lsn <= slot_lsn {
            return Ok(checkpoint_lsn);
        }

        if attempt + 1 < attempts {
            tokio::time::sleep(retry_delay).await;
        }
    }

    reconcile_stream_resume_lsn(
        checkpoint_lsn,
        last_slot_lsn,
        slot_name,
    )
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
        let mut cursor = std::io::Cursor::new(&pem_bytes);
        let certs: Vec<_> = rustls_pemfile::certs(&mut cursor)
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
                target: "cdc_rs::source::postgres",
                "failed to load a native root certificate: {err}"
            );
        }
        for cert in native_certs.certs {
            if let Err(err) = root_store.add(cert) {
                tracing::debug!(
                    target: "cdc_rs::source::postgres",
                    "skipping invalid native root certificate: {err}"
                );
            }
        }
    }

    Ok(root_store)
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
    use std::io::Cursor;

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

            let certs: Vec<rustls::pki_types::CertificateDer<'static>> =
                rustls_pemfile::certs(&mut Cursor::new(&cert_pem))
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

            let key = rustls_pemfile::private_key(&mut Cursor::new(&key_pem))
                .map_err(|error| {
                    Error::ConfigError(format!(
                        "failed to parse mTLS private key PEM '{key_path}': {error}"
                    ))
                })?
                .ok_or_else(|| {
                    Error::ConfigError(format!(
                        "mTLS private key file '{key_path}' contains no private key"
                    ))
                })?;

            rustls::ClientConfig::builder()
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
        (None, None) => Ok(rustls::ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth()),
    }
}
