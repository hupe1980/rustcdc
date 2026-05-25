use serde::{Deserialize, Serialize};

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
	/// Construct a TLS transport configuration using the system trust store
	/// and no client certificate (server-auth-only TLS).
	pub fn tls() -> Self {
		Self::Tls {
			ca_cert_path: None,
			client_cert_path: None,
			client_key_path: None,
		}
	}

	/// Construct a TLS transport configuration with an optional CA bundle.
	pub fn tls_with_ca_cert_path(ca_cert_path: Option<String>) -> Self {
		Self::Tls {
			ca_cert_path,
			client_cert_path: None,
			client_key_path: None,
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
		}
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
}
