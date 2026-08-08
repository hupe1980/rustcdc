//! Resolving a [`TransportConfig`](crate::core::TransportConfig) into a rustls client config.
//!
//! Lives beside `TransportConfig` rather than inside a connector because the mapping is not
//! connector-specific, and because an embedder opening one extra connection to the same
//! source — a lag sampler, a schema probe, an operator tool — must reach exactly the same
//! decisions this makes. Reimplementing it means diverging on which root store is used when
//! `ca_cert_path` is absent, on refusing `allow_invalid_certificates` rather than honouring
//! it, and on requiring `client_cert_path` and `client_key_path` together.
//!
//! It also installs the crypto provider explicitly. `rustls::ClientConfig::builder()`
//! **panics** rather than erroring when no process-wide provider has been installed, which
//! happens whenever a dependency graph links more than one provider — and a panic on a
//! background task takes out a worker thread.

use rustls::pki_types::{pem::PemObject as _, CertificateDer, PrivateKeyDer};

use crate::core::{Error, Result};

pub(crate) fn build_tls_root_store(ca_cert_path: Option<&str>) -> Result<rustls::RootCertStore> {
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
/// Resolve a [`TransportConfig`](crate::core::TransportConfig) to a rustls client config.
///
/// Used by the replication transport, which does its own TLS handshake rather than going
/// through `tokio-postgres-rustls`. It shares this function so both transports trust
/// exactly the same roots and present the same client certificate — two connections to the
/// same server disagreeing about what they verify would be a security surprise, not a
/// detail.
///
/// `allow_invalid_certificates` / `allow_invalid_hostnames` are not handled here because
/// `PostgresConnection::connect` rejects them before any connection is opened; see the
/// error it raises for the reasoning.
pub fn rustls_client_config(
    transport: &crate::core::TransportConfig,
) -> Result<rustls::ClientConfig> {
    use crate::core::TransportConfig;

    match transport {
        TransportConfig::Tls {
            ca_cert_path,
            client_cert_path,
            client_key_path,
            ..
        } => build_tls_client_config(
            ca_cert_path.as_deref(),
            client_cert_path.as_deref(),
            client_key_path.as_deref(),
        ),
        // An injected config is used as-is, custom verifier and all. The replication
        // transport builds its own connector, so unlike a third-party client it has no
        // reason to refuse one.
        TransportConfig::RustlsConfig { config } => Ok((*config.0).clone()),
        TransportConfig::Plaintext => Err(Error::ConfigError(
            "cannot build a TLS configuration for a plaintext transport".into(),
        )),
    }
}

pub(crate) fn build_tls_client_config(
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
