use std::{fmt, sync::Arc};

use serde::{ser::SerializeMap, Deserialize, Deserializer, Serialize, Serializer};

use crate::core::{Error, Result};

type SecretCallback = dyn Fn() -> Result<String> + Send + Sync + 'static;

/// Provider interface for loading secrets from an external system.
pub trait SecretProvider: Send + Sync {
    fn resolve_secret(&self, reference: &str) -> Result<String>;
}

#[derive(Clone)]
enum SecretValue {
    Inline(String),
    Environment {
        variable: String,
    },
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
            value: SecretValue::Inline(value.into()),
        }
    }

    /// Resolve a secret from an environment variable at connect time.
    pub fn from_env(variable: impl Into<String>) -> Self {
        Self {
            value: SecretValue::Environment {
                variable: variable.into(),
            },
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
            SecretValue::Inline(value) => Ok(value),
            SecretValue::Environment { .. }
            | SecretValue::Provider { .. }
            | SecretValue::Callback { .. } => Err(Error::ConfigError(
                "attempted to expose a deferred secret directly; use resolve()".into(),
            )),
        }
    }

    /// Resolve the secret value for connector internals.
    pub fn resolve(&self) -> Result<String> {
        match &self.value {
            SecretValue::Inline(value) => Ok(value.clone()),
            SecretValue::Environment { variable } => std::env::var(variable).map_err(|error| {
                Error::ConfigError(format!(
                    "failed to load secret from environment variable '{variable}': {error}"
                ))
            }),
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
            SecretValue::Inline(value) => Ok(value),
            SecretValue::Environment { .. }
            | SecretValue::Provider { .. }
            | SecretValue::Callback { .. } => Err(Error::ConfigError(
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
            SecretValue::Inline(value) => ("inline", value),
            SecretValue::Environment { variable } => ("env", variable),
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
            "env" => write!(f, "SecretString(env:{descriptor}, ***redacted***)"),
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
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match &self.value {
            SecretValue::Inline(_) => Err(serde::ser::Error::custom(
                "inline secrets cannot be serialized; use env/provider/callback references",
            )),
            SecretValue::Environment { variable } => {
                let mut map = serializer.serialize_map(Some(1))?;
                map.serialize_entry("env", variable)?;
                map.end()
            }
            SecretValue::Provider { .. } => Err(serde::ser::Error::custom(
                "provider-backed secrets cannot be serialized; store a provider reference in code",
            )),
            SecretValue::Callback { .. } => Err(serde::ser::Error::custom(
                "callback-backed secrets cannot be serialized; store the callback in code",
            )),
        }
    }
}

impl<'de> Deserialize<'de> for SecretString {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Repr {
            Inline(String),
            Environment { env: String },
        }

        match Repr::deserialize(deserializer)? {
            Repr::Inline(value) => Ok(SecretString::new(value)),
            Repr::Environment { env } => Ok(SecretString::from_env(env)),
        }
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
        let secret = SecretString::from_env("HOME");
        assert!(matches!(
            secret.expose_secret(),
            Err(Error::ConfigError(message)) if message.contains("use resolve")
        ));
    }

    #[test]
    fn into_inner_rejects_deferred_values() {
        let secret = SecretString::from_env("HOME");
        assert!(matches!(
            secret.into_inner(),
            Err(Error::ConfigError(message)) if message.contains("use resolve")
        ));
    }

    #[test]
    fn env_secret_resolves_from_environment() {
        let expected = std::env::var("HOME").expect("HOME should be present for test execution");
        let secret = SecretString::from_env("HOME");
        assert_eq!(secret.resolve().unwrap(), expected);
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
    fn secret_deserializes_env_form() {
        let secret: SecretString = serde_json::from_str(r#"{"env":"CDC_RS_PASSWORD"}"#).unwrap();
        assert!(format!("{secret:?}").contains("env:CDC_RS_PASSWORD"));
    }

    #[test]
    fn inline_secret_serialization_is_rejected() {
        let secret = SecretString::new("top-secret");
        let error = serde_json::to_string(&secret).unwrap_err().to_string();
        assert!(error.contains("inline secrets cannot be serialized"));
    }

    #[test]
    fn env_secret_serializes_as_env_reference() {
        let secret = SecretString::from_env("CDC_RS_PASSWORD");
        let json = serde_json::to_string(&secret).unwrap();
        assert_eq!(json, r#"{"env":"CDC_RS_PASSWORD"}"#);
    }
}
