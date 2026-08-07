//! TLS for the admin listener.
//!
//! Split out of `admin/mod.rs` because it shares nothing with the request handlers: it
//! reads two files at startup and produces a `rustls::ServerConfig`. Keeping it here means
//! the certificate-handling code can be reviewed on its own, which is the review a
//! reader most often wants to do in isolation.

use std::io::BufReader;
use std::sync::Arc;

use rustls::pki_types::pem::PemObject;

use crate::config::schema::AdminTlsConfig;
use crate::error::AppError;

pub(super) fn build_tls_server_config(
    tls: &AdminTlsConfig,
) -> Result<rustls::ServerConfig, AppError> {
    let cert_chain = read_certs(&tls.cert_file)?;
    let private_key = read_private_key(&tls.key_file)?;

    let builder = rustls::ServerConfig::builder();
    let mut config = if tls.require_client_cert {
        let ca_path = tls.client_ca_file.as_ref().ok_or_else(|| {
            AppError::Other(
                "admin.tls.client_ca_file is required when require_client_cert=true".to_string(),
            )
        })?;
        let client_ca_certs = read_certs(ca_path)?;
        let mut roots = rustls::RootCertStore::empty();
        let (_, rejected) = roots.add_parsable_certificates(client_ca_certs);
        if rejected > 0 {
            return Err(AppError::Other(format!(
                "admin.tls.client_ca_file contains {rejected} invalid certificates"
            )));
        }

        let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(roots))
            .build()
            .map_err(|e| {
                AppError::Other(format!("failed to build admin TLS client verifier: {e}"))
            })?;

        builder
            .with_client_cert_verifier(verifier)
            .with_single_cert(cert_chain, private_key)
            .map_err(|e| AppError::Other(format!("failed to build admin TLS config: {e}")))?
    } else {
        builder
            .with_no_client_auth()
            .with_single_cert(cert_chain, private_key)
            .map_err(|e| AppError::Other(format!("failed to build admin TLS config: {e}")))?
    };

    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    Ok(config)
}

fn read_certs(
    path: &std::path::Path,
) -> Result<Vec<rustls::pki_types::CertificateDer<'static>>, AppError> {
    let file = std::fs::File::open(path).map_err(|e| {
        AppError::Other(format!(
            "failed to open certificate file {}: {e}",
            path.display()
        ))
    })?;
    let mut reader = BufReader::new(file);
    let certs = rustls::pki_types::CertificateDer::pem_reader_iter(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| {
            AppError::Other(format!(
                "failed to parse PEM certificates from {}: {e}",
                path.display()
            ))
        })?;

    if certs.is_empty() {
        return Err(AppError::Other(format!(
            "no certificates found in {}",
            path.display()
        )));
    }

    Ok(certs)
}

/// Load the admin TLS private key.
///
/// `PrivateKeyDer::from_pem_file` accepts PKCS#8, PKCS#1 *and* SEC1 in one pass and
/// takes the **first** key in the file. The previous hand-rolled parser knew nothing
/// about SEC1, so an `openssl ecparam`-generated EC key — a perfectly ordinary choice
/// for an internal admin endpoint — was rejected as "no supported private key found".
fn read_private_key(
    path: &std::path::Path,
) -> Result<rustls::pki_types::PrivateKeyDer<'static>, AppError> {
    rustls::pki_types::PrivateKeyDer::from_pem_file(path).map_err(|e| {
        AppError::Other(format!(
            "failed to load a private key from {} (expected PKCS#8, PKCS#1 or SEC1 PEM): {e}",
            path.display()
        ))
    })
}
