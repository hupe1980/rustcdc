use serde::{Deserialize, Serialize};

// ─────────────────────────────────────────────────────────────────────────────
// Subject naming strategy
// ─────────────────────────────────────────────────────────────────────────────

/// Determines how the schema subject name is constructed for a given event.
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum SubjectNameStrategy {
    /// `<topic>-value` / `<topic>-key` — the Confluent default.
    #[default]
    TopicName,

    /// The fully-qualified record (table) name, e.g. `public.orders`.
    RecordName,

    /// `<topic>-<fully_qualified_record_name>`.
    TopicRecordName,
}

// ─────────────────────────────────────────────────────────────────────────────
// Confluent registry config
// ─────────────────────────────────────────────────────────────────────────────

/// Confluent-compatible schema registry configuration.
///
/// Credentials are **never** stored inline; they are resolved at runtime from
/// the environment variables named by `username_env` / `password_env`.
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
pub struct ConfluentRegistryConfig {
    /// Schema registry base URL, e.g. `https://sr.example.com`.
    pub url: String,

    /// Name of the environment variable that holds the basic-auth username
    /// (or Confluent Cloud API key).  `null` disables authentication.
    #[serde(default)]
    pub username_env: Option<String>,

    /// Name of the environment variable that holds the basic-auth password
    /// (or Confluent Cloud API secret).  Required when `username_env` is set.
    #[serde(default)]
    pub password_env: Option<String>,

    /// How schema subjects are named for registered schemas
    /// (default: `topic_name`).
    #[serde(default)]
    pub subject_name_strategy: SubjectNameStrategy,

    /// Allow plaintext (non-TLS) connections to the schema registry.
    ///
    /// **Security warning**: set `true` only for local development.
    /// Production deployments must use `https://`.
    #[serde(default)]
    pub allow_insecure: bool,

    /// HTTP request timeout for schema registry API calls, in milliseconds
    /// (default: 30 000).  Set to 0 to use the client's built-in default.
    #[serde(default = "default_registry_request_timeout_ms")]
    pub request_timeout_ms: u64,
}

fn default_registry_request_timeout_ms() -> u64 {
    30_000
}

impl ConfluentRegistryConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.url.trim().is_empty() {
            return Err("registry.url must not be empty".to_string());
        }
        if !self.allow_insecure && self.url.starts_with("http://") {
            return Err(format!(
                "registry.url '{}' uses plaintext HTTP; \
                 set allow_insecure = true for local/dev use only",
                self.url
            ));
        }
        if self.username_env.is_some() && self.password_env.is_none() {
            return Err("registry.password_env is required when username_env is set".to_string());
        }
        Ok(())
    }

    /// Resolve registry credentials from environment variables at runtime.
    ///
    /// Returns `(username, password)`.  Both are `None` when no credentials
    /// are configured.
    pub fn resolve_credentials(&self) -> Result<(Option<String>, Option<String>), String> {
        let username = self
            .username_env
            .as_deref()
            .map(|var| {
                std::env::var(var).map_err(|_| {
                    format!("registry.username_env: environment variable '{var}' is not set")
                })
            })
            .transpose()?;

        let password = self
            .password_env
            .as_deref()
            .map(|var| {
                std::env::var(var).map_err(|_| {
                    format!("registry.password_env: environment variable '{var}' is not set")
                })
            })
            .transpose()?;

        Ok((username, password))
    }
}
