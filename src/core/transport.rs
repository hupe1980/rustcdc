use serde::{Deserialize, Serialize};

/// Transport configuration for a connector instance.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum TransportConfig {
	/// Use TLS transport with an optional CA bundle.
	Tls {
		/// Optional PEM-encoded CA certificate file used to verify the server.
		ca_cert_path: Option<String>,
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
}

impl Default for TransportConfig {
	fn default() -> Self {
		Self::tls()
	}
}

impl TransportConfig {
	/// Construct a TLS transport configuration using the system trust store.
	pub fn tls() -> Self {
		Self::Tls {
			ca_cert_path: None,
		}
	}

	/// Construct a TLS transport configuration with an optional CA bundle.
	pub fn tls_with_ca_cert_path(ca_cert_path: Option<String>) -> Self {
		Self::Tls { ca_cert_path }
	}

	/// Construct a plaintext (unencrypted) transport configuration.
	///
	/// See the [`TransportConfig::Plaintext`] variant for security guidance.
	pub const fn plaintext() -> Self {
		Self::Plaintext
	}

	/// Return true when TLS transport is configured.
	pub const fn is_tls(&self) -> bool {
		matches!(self, Self::Tls { .. })
	}

	/// Return the configured CA bundle path, if any.
	pub fn ca_cert_path(&self) -> Option<&str> {
		match self {
			Self::Tls {
				ca_cert_path: Some(path),
			} => Some(path.as_str()),
			_ => None,
		}
	}
}
