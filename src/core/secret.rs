use std::{fmt, sync::Arc};

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use zeroize::Zeroizing;

use crate::core::{Error, Result};

type SecretCallback = dyn Fn() -> Result<String> + Send + Sync + 'static;

/// Provider interface for loading secrets from an external system.
pub trait SecretProvider: Send + Sync {
    fn resolve_secret(&self, reference: &str) -> Result<String>;
}

#[derive(Clone)]
enum SecretValue {
    /// Inline secret — zeroed on drop via `Zeroizing<String>`.
    Inline(Zeroizing<String>),
    Provider {
        provider_name: String,
        reference: String,
        provider: Arc<dyn SecretProvider>,
    },
    Callback {
        label: String,
        callback: Arc<SecretCallback>,
    },
}

/// Redacted secret wrapper for runtime and connector configuration.
#[derive(Clone)]
pub struct SecretString {
    value: SecretValue,
}

impl Default for SecretString {
    fn default() -> Self {
        Self::new("")
    }
}

impl PartialEq for SecretString {
    fn eq(&self, other: &Self) -> bool {
        self.kind_and_descriptor() == other.kind_and_descriptor()
    }
}

impl Eq for SecretString {}

impl SecretString {
    /// Create a new secret wrapper from an owned string.
    pub fn new(value: impl Into<String>) -> Self {
        Self {
            value: SecretValue::Inline(Zeroizing::new(value.into())),
        }
    }

    /// Resolve a secret from an external provider at connect time.
    pub fn from_provider(
        provider_name: impl Into<String>,
        reference: impl Into<String>,
        provider: Arc<dyn SecretProvider>,
    ) -> Self {
        Self {
            value: SecretValue::Provider {
                provider_name: provider_name.into(),
                reference: reference.into(),
                provider,
            },
        }
    }

    /// Resolve a secret through a callback at connect time.
    pub fn from_callback(
        label: impl Into<String>,
        callback: impl Fn() -> Result<String> + Send + Sync + 'static,
    ) -> Self {
        Self {
            value: SecretValue::Callback {
                label: label.into(),
                callback: Arc::new(callback),
            },
        }
    }

    /// Return the inline secret value.
    ///
    /// Deferred secrets must be resolved with `resolve`.
    pub fn expose_secret(&self) -> Result<&str> {
        match &self.value {
            SecretValue::Inline(value) => Ok(value.as_str()),
            SecretValue::Provider { .. } | SecretValue::Callback { .. } => Err(Error::ConfigError(
                "attempted to expose a deferred secret directly; use resolve()".into(),
            )),
        }
    }

    /// Resolve the secret value for connector internals.
    pub fn resolve(&self) -> Result<String> {
        match &self.value {
            SecretValue::Inline(value) => Ok(value.as_str().to_owned()),
            SecretValue::Provider {
                provider_name,
                reference,
                provider,
            } => provider.resolve_secret(reference).map_err(|error| {
                Error::ConfigError(format!(
                    "secret provider '{provider_name}' failed for reference '{reference}': {error}"
                ))
            }),
            SecretValue::Callback { label, callback } => callback().map_err(|error| {
                Error::ConfigError(format!(
                    "secret callback '{label}' failed to resolve: {error}"
                ))
            }),
        }
    }

    /// Consume the wrapper and return the inline secret.
    pub fn into_inner(self) -> Result<String> {
        match self.value {
            SecretValue::Inline(value) => Ok(value.as_str().to_owned()),
            SecretValue::Provider { .. } | SecretValue::Callback { .. } => Err(Error::ConfigError(
                "attempted to consume a deferred secret directly; use resolve()".into(),
            )),
        }
    }

    /// Whether the resolved secret is empty.
    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.resolve()?.is_empty())
    }

    fn kind_and_descriptor(&self) -> (&'static str, &str) {
        match &self.value {
            SecretValue::Inline(value) => ("inline", value.as_str()),
            SecretValue::Provider {
                provider_name,
                reference,
                ..
            } => {
                if reference.is_empty() {
                    ("provider", provider_name)
                } else {
                    ("provider", reference)
                }
            }
            SecretValue::Callback { label, .. } => ("callback", label),
        }
    }
}

impl From<String> for SecretString {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

impl From<&str> for SecretString {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl fmt::Debug for SecretString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (kind, descriptor) = self.kind_and_descriptor();
        match kind {
            "inline" => f.write_str("SecretString(***redacted***)"),
            "provider" => write!(f, "SecretString(provider:{descriptor}, ***redacted***)"),
            "callback" => write!(f, "SecretString(callback:{descriptor}, ***redacted***)"),
            _ => f.write_str("SecretString(***redacted***)"),
        }
    }
}

impl fmt::Display for SecretString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("***redacted***")
    }
}

impl Serialize for SecretString {
    /// Serializes as the literal string `"[REDACTED]"`.
    ///
    /// Secret values are **never** included in serialized output regardless of
    /// variant.  Configs serialized to JSON, TOML, or any other format will
    /// always see `"[REDACTED]"` in the password field, making it safe to log
    /// config snapshots for diagnostics without leaking credentials.
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str("[REDACTED]")
    }
}

impl<'de> Deserialize<'de> for SecretString {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let inline = String::deserialize(deserializer)?;
        tracing::warn!(
            target: "rustcdc::security",
            "SecretString deserialized from inline plaintext value. \
             Prefer SecretString::from_provider or SecretString::from_callback for production \
             deployments to avoid embedding plaintext credentials in configuration files."
        );
        Ok(SecretString::new(inline))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    use super::{SecretProvider, SecretString};
    use crate::core::Error;

    struct StaticProvider;

    impl SecretProvider for StaticProvider {
        fn resolve_secret(&self, reference: &str) -> crate::core::Result<String> {
            Ok(format!("provider:{reference}"))
        }
    }

    #[test]
    fn debug_and_display_are_redacted() {
        let secret = SecretString::new("top-secret");
        assert!(!format!("{secret:?}").contains("top-secret"));
        assert_eq!(format!("{secret}"), "***redacted***");
    }

    #[test]
    fn deferred_secret_debug_is_redacted_without_resolved_value() {
        let secret = SecretString::from_callback("rotating", || Ok("super-secret-value".into()));
        let debug = format!("{secret:?}");
        assert!(debug.contains("callback:rotating"));
        assert!(debug.contains("***redacted***"));
        assert!(!debug.contains("super-secret-value"));
        assert_eq!(format!("{secret}"), "***redacted***");
    }

    #[test]
    fn expose_secret_returns_original_value() {
        let secret = SecretString::new("top-secret");
        assert_eq!(secret.expose_secret().unwrap(), "top-secret");
    }

    #[test]
    fn expose_secret_rejects_deferred_values() {
        let secret = SecretString::from_callback("runtime", || Ok("from-callback".to_string()));
        assert!(matches!(
            secret.expose_secret(),
            Err(Error::ConfigError(message)) if message.contains("use resolve")
        ));
    }

    #[test]
    fn into_inner_rejects_deferred_values() {
        let secret = SecretString::from_callback("runtime", || Ok("from-callback".to_string()));
        assert!(matches!(
            secret.into_inner(),
            Err(Error::ConfigError(message)) if message.contains("use resolve")
        ));
    }

    #[test]
    fn provider_secret_resolves_from_provider() {
        let secret = SecretString::from_provider("static", "db/password", Arc::new(StaticProvider));
        assert_eq!(secret.resolve().unwrap(), "provider:db/password");
    }

    #[test]
    fn callback_secret_resolves_from_callback() {
        let secret = SecretString::from_callback("runtime", || Ok("from-callback".to_string()));
        assert_eq!(secret.resolve().unwrap(), "from-callback");
    }

    #[test]
    fn callback_secret_supports_rotation_across_resolves() {
        let counter = Arc::new(AtomicUsize::new(0));
        let secret = {
            let counter = counter.clone();
            SecretString::from_callback("rotation", move || {
                let next = counter.fetch_add(1, Ordering::Relaxed) + 1;
                Ok(format!("rotated-{next}"))
            })
        };

        assert_eq!(secret.resolve().unwrap(), "rotated-1");
        assert_eq!(secret.resolve().unwrap(), "rotated-2");
    }

    #[test]
    fn callback_failures_are_wrapped_as_config_errors() {
        let secret = SecretString::from_callback("runtime", || {
            Err(Error::StateError("vault unavailable".into()))
        });
        assert!(
            matches!(secret.resolve(), Err(Error::ConfigError(message)) if message.contains("vault unavailable"))
        );
    }

    #[test]
    fn failure_is_isolated_to_the_failing_secret_instance() {
        let failing = SecretString::from_callback("failing", || {
            Err(Error::StateError("provider offline".into()))
        });
        let healthy = SecretString::from_callback("healthy", || Ok("healthy-secret".into()));

        assert!(
            matches!(failing.resolve(), Err(Error::ConfigError(message)) if message.contains("provider offline"))
        );
        assert_eq!(healthy.resolve().unwrap(), "healthy-secret");
    }

    #[test]
    fn secret_deserializes_inline_string() {
        let secret: SecretString = serde_json::from_str(r#""plain-secret""#).unwrap();
        assert_eq!(secret.expose_secret().unwrap(), "plain-secret");
    }

    #[test]
    fn inline_secret_serialization_is_rejected() {
        let secret = SecretString::new("top-secret");
        let error = serde_json::to_string(&secret).unwrap_err().to_string();
        assert!(error.contains("inline secrets cannot be serialized"));
    }

    #[test]
    fn provider_secret_serialization_is_rejected() {
        let secret = SecretString::from_provider("static", "db/password", Arc::new(StaticProvider));
        let error = serde_json::to_string(&secret).unwrap_err().to_string();
        assert!(error.contains("provider-backed secrets cannot be serialized"));
    }
}
