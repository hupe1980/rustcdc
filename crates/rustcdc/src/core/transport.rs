use serde::{Deserialize, Serialize};

#[cfg(feature = "tls")]
use std::sync::Arc;

/// A type-erased `rustls::ClientConfig` that can be embedded in [`TransportConfig`].
///
/// Equality is pointer-based (two handles are equal only if they wrap the same
/// `Arc` allocation), which is consistent with the immutable nature of a fully
/// constructed `ClientConfig`.
///
/// # Serialization
///
/// This type is serializable **only as a non-round-trippable placeholder**.
/// Serialization emits the string `"<RustlsClientConfig>"` so that structs
/// containing a `TransportConfig` (e.g. admin snapshots, tracing spans) do
/// not panic or error during JSON serialization. Deserialization always
/// returns an error — a `RustlsClientConfig` must be injected at runtime
/// (e.g. from a pre-built `Arc<rustls::ClientConfig>`) and cannot be
/// reconstructed from serialized text.
#[cfg(feature = "tls")]
#[derive(Clone, Debug)]
pub struct RustlsClientConfig(pub Arc<rustls::ClientConfig>);

#[cfg(feature = "tls")]
impl PartialEq for RustlsClientConfig {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

#[cfg(feature = "tls")]
impl Eq for RustlsClientConfig {}

#[cfg(feature = "tls")]
impl Serialize for RustlsClientConfig {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        // RustlsClientConfig cannot be serialized to text; emit a placeholder
        // string so JSON/tracing snapshots do not panic.
        serializer.serialize_str("<RustlsClientConfig>")
    }
}

#[cfg(feature = "tls")]
impl<'de> Deserialize<'de> for RustlsClientConfig {
    fn deserialize<D: serde::Deserializer<'de>>(_deserializer: D) -> Result<Self, D::Error> {
        Err(serde::de::Error::custom(
            "RustlsClientConfig cannot be deserialized from text; inject it at runtime",
        ))
    }
}

/// Transport configuration for a connector instance.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum TransportConfig {
    /// Use TLS transport.
    ///
    /// Set `ca_cert_path` to verify the server against a custom CA bundle;
    /// set `client_cert_path` + `client_key_path` to enable mutual TLS (mTLS).
    Tls {
        /// Optional PEM-encoded CA certificate file used to verify the server.
        /// When absent the system trust store is used.
        ca_cert_path: Option<String>,
        /// Optional PEM-encoded client certificate file for mTLS authentication.
        /// Must be paired with `client_key_path`.
        client_cert_path: Option<String>,
        /// Optional PEM-encoded client private key file for mTLS authentication.
        /// Must be paired with `client_cert_path`.
        client_key_path: Option<String>,
        /// When true, accept invalid or unknown CA certificates.
        ///
        /// This is intended only for local testing or tightly controlled
        /// private environments where certificate distribution is not practical.
        #[serde(default)]
        allow_invalid_certificates: bool,
        /// When true, skip TLS hostname verification.
        ///
        /// This is intended only for local testing or tightly controlled
        /// private environments where DNS/SAN validation is not practical.
        #[serde(default)]
        allow_invalid_hostnames: bool,
    },
    /// Use plaintext (unencrypted) transport.
    ///
    /// # Security Warning
    ///
    /// Plaintext transport transmits credentials and data in the clear.
    /// Only use this for localhost or fully-trusted private-network deployments
    /// (e.g., VPC-internal clusters) where all traffic is already isolated.
    /// Do **not** use plaintext over public or shared networks.
    Plaintext,

    /// Use a fully-constructed `rustls::ClientConfig` injected at runtime.
    ///
    /// This variant is intended for advanced use-cases where the caller needs
    /// complete control over TLS configuration that cannot be expressed through
    /// the file-path-based [`TransportConfig::Tls`] variant — for example:
    ///
    /// - Custom certificate verifiers or pinning logic.
    /// - Client certificates loaded from a hardware security module (HSM).
    /// - Integration tests that generate ephemeral CA certificates in-process.
    ///
    /// Because `rustls::ClientConfig` is not serializable, this variant
    /// serializes as the string `"<RustlsClientConfig>"` and **cannot** be
    /// round-tripped through a config file. Inject it programmatically only.
    ///
    /// Requires the `tls` Cargo feature.
    #[cfg(feature = "tls")]
    RustlsConfig {
        /// The pre-built rustls client configuration.
        config: RustlsClientConfig,
    },
}

impl Default for TransportConfig {
    fn default() -> Self {
        Self::tls()
    }
}

impl TransportConfig {
    /// Construct a TLS transport configuration using the system trust store
    /// and no client certificate (server-auth-only TLS).
    pub fn tls() -> Self {
        Self::Tls {
            ca_cert_path: None,
            client_cert_path: None,
            client_key_path: None,
            allow_invalid_certificates: false,
            allow_invalid_hostnames: false,
        }
    }

    /// Construct a TLS transport configuration with an optional CA bundle.
    pub fn tls_with_ca_cert_path(ca_cert_path: Option<String>) -> Self {
        Self::Tls {
            ca_cert_path,
            client_cert_path: None,
            client_key_path: None,
            allow_invalid_certificates: false,
            allow_invalid_hostnames: false,
        }
    }

    /// Construct a mutual TLS (mTLS) configuration.
    ///
    /// Both `client_cert_path` and `client_key_path` are required.
    /// `ca_cert_path` is optional and falls back to the system trust store.
    pub fn mtls(
        ca_cert_path: Option<String>,
        client_cert_path: String,
        client_key_path: String,
    ) -> Self {
        Self::Tls {
            ca_cert_path,
            client_cert_path: Some(client_cert_path),
            client_key_path: Some(client_key_path),
            allow_invalid_certificates: false,
            allow_invalid_hostnames: false,
        }
    }

    /// Construct TLS transport that skips certificate and hostname validation.
    ///
    /// Use only for local testing or tightly controlled private environments.
    pub fn tls_insecure_skip_verify() -> Self {
        Self::Tls {
            ca_cert_path: None,
            client_cert_path: None,
            client_key_path: None,
            allow_invalid_certificates: true,
            allow_invalid_hostnames: true,
        }
    }

    /// Construct a plaintext (unencrypted) transport configuration.
    ///
    /// See the [`TransportConfig::Plaintext`] variant for security guidance.
    pub const fn plaintext() -> Self {
        Self::Plaintext
    }

    /// Inject a pre-built `rustls::ClientConfig` directly.
    ///
    /// Use this when the file-path-based [`TransportConfig::Tls`] variant does
    /// not provide the level of control you need (custom verifiers, HSM keys,
    /// ephemeral in-process CAs, etc.).
    ///
    /// Requires the `tls` Cargo feature.
    #[cfg(feature = "tls")]
    pub fn rustls_config(config: Arc<rustls::ClientConfig>) -> Self {
        Self::RustlsConfig {
            config: RustlsClientConfig(config),
        }
    }

    /// Return true when TLS transport is configured.
    pub fn is_tls(&self) -> bool {
        #[cfg(feature = "tls")]
        if matches!(self, Self::RustlsConfig { .. }) {
            return true;
        }
        matches!(self, Self::Tls { .. })
    }

    /// Return true when mutual TLS (mTLS) is configured (client cert + key both set).
    pub fn is_mtls(&self) -> bool {
        matches!(
            self,
            Self::Tls {
                client_cert_path: Some(_),
                client_key_path: Some(_),
                ..
            }
        )
    }

    /// Return the configured CA bundle path, if any.
    pub fn ca_cert_path(&self) -> Option<&str> {
        match self {
            Self::Tls {
                ca_cert_path: Some(path),
                ..
            } => Some(path.as_str()),
            _ => None,
        }
    }

    /// Return true when TLS certificate verification is disabled.
    ///
    /// Always returns `false` for the `tls`-gated `TransportConfig::RustlsConfig`
    /// variant — the injected `ClientConfig` is assumed to already encode the
    /// desired verification policy.
    pub fn allow_invalid_certificates(&self) -> bool {
        match self {
            Self::Tls {
                allow_invalid_certificates,
                ..
            } => *allow_invalid_certificates,
            _ => false,
        }
    }

    /// Return true when TLS hostname verification is disabled.
    ///
    /// Always returns `false` for the `tls`-gated `TransportConfig::RustlsConfig`
    /// variant — the injected `ClientConfig` is assumed to already encode the
    /// desired hostname policy.
    pub fn allow_invalid_hostnames(&self) -> bool {
        match self {
            Self::Tls {
                allow_invalid_hostnames,
                ..
            } => *allow_invalid_hostnames,
            _ => false,
        }
    }

    /// Return the configured client certificate path, if any.
    pub fn client_cert_path(&self) -> Option<&str> {
        match self {
            Self::Tls {
                client_cert_path: Some(path),
                ..
            } => Some(path.as_str()),
            _ => None,
        }
    }

    /// Return the configured client private key path, if any.
    pub fn client_key_path(&self) -> Option<&str> {
        match self {
            Self::Tls {
                client_key_path: Some(path),
                ..
            } => Some(path.as_str()),
            _ => None,
        }
    }

    /// Emit `tracing::warn!` events for any insecure TLS flags.
    ///
    /// Call this once per connection attempt. When `allow_invalid_certificates`
    /// or `allow_invalid_hostnames` is set, a structured warning is emitted so
    /// that log-aggregation pipelines and alerting rules can detect accidental
    /// production use of insecure TLS configuration.
    pub fn warn_if_insecure(&self, source_label: &str) {
        if self.allow_invalid_certificates() {
            tracing::warn!(
                target: "rustcdc::transport::security",
                source = source_label,
                flag = "allow_invalid_certificates",
                "TLS certificate verification is disabled — do not use in production"
            );
        }
        if self.allow_invalid_hostnames() {
            tracing::warn!(
                target: "rustcdc::transport::security",
                source = source_label,
                flag = "allow_invalid_hostnames",
                "TLS hostname verification is disabled — do not use in production"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::TransportConfig;

    #[test]
    fn tls_defaults_to_strict_verification() {
        let transport = TransportConfig::tls();
        assert!(transport.is_tls());
        assert!(!transport.allow_invalid_certificates());
        assert!(!transport.allow_invalid_hostnames());
    }

    #[test]
    fn tls_insecure_skip_verify_sets_insecure_flags() {
        let transport = TransportConfig::tls_insecure_skip_verify();
        assert!(transport.is_tls());
        assert!(transport.allow_invalid_certificates());
        assert!(transport.allow_invalid_hostnames());
    }
}
