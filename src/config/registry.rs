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
// Registry flavour
// ─────────────────────────────────────────────────────────────────────────────

/// Which registry API the client speaks.
///
/// Both flavours emit the **same Confluent 5-byte wire framing**; they differ only in
/// the HTTP API used to register and resolve schemas.
#[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum RegistryFlavor {
    /// Confluent Schema Registry, or any endpoint that serves its API — Confluent
    /// Platform/Cloud, Karapace, Redpanda, or Apicurio's `/apis/ccompat/v7` path.
    #[default]
    Confluent,

    /// Apicurio Registry's **native v3 API**.
    ///
    /// `url` is the server root; the client appends `/apis/registry/v3` itself. Passing
    /// the full path produces a doubled URL and a 404 — use `flavor = "confluent"` with
    /// `http://apicurio:8080/apis/ccompat/v7` if you want the compatibility API instead.
    Apicurio,
}

// ─────────────────────────────────────────────────────────────────────────────
// Registry config
// ─────────────────────────────────────────────────────────────────────────────

/// Schema registry configuration shared by every registry-backed codec.
///
/// Credentials are **never** stored inline; they are resolved at runtime from the
/// environment variables named by `username_env` / `password_env` / `token_env`.
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
pub struct ConfluentRegistryConfig {
    /// Schema registry URL.
    ///
    /// For `flavor = "confluent"` this is the API root that serves `/subjects`
    /// (`https://sr.example.com`). For `flavor = "apicurio"` it is the server root.
    pub url: String,

    /// Which registry API to speak (default: `confluent`).
    #[serde(default)]
    pub flavor: RegistryFlavor,

    /// Name of the environment variable that holds the basic-auth username
    /// (or Confluent Cloud API key).  `null` disables authentication.
    #[serde(default)]
    pub username_env: Option<String>,

    /// Name of the environment variable that holds the basic-auth password
    /// (or Confluent Cloud API secret).  Required when `username_env` is set.
    #[serde(default)]
    pub password_env: Option<String>,

    /// Name of the environment variable holding an OAuth/IAM **bearer token**.
    ///
    /// Mutually exclusive with `username_env`; managed registries that issue short-lived
    /// tokens use this instead of basic auth.
    #[serde(default)]
    pub token_env: Option<String>,

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

    /// TCP connect timeout in milliseconds. `0` uses the client default.
    #[serde(default)]
    pub connect_timeout_ms: u64,

    /// Register the schema on first use (default: `true`).
    ///
    /// Setting `false` requires the subject to already carry **rustcdc's** schema.
    /// The encoder compares the registered schema against the one it will write and
    /// refuses to start if they differ — an id that resolves to a different schema
    /// yields plausible-looking wrong field values downstream rather than an error,
    /// because Avro binary is positional and untagged.
    #[serde(default = "default_true")]
    pub auto_register: bool,

    /// Ask the registry to normalise schemas on registration (default: `false`).
    ///
    /// Prevents schema-id churn when equivalent schemas differ only in field ordering
    /// or formatting.
    #[serde(default)]
    pub normalize_schemas: bool,

    /// Maximum schema entries retained in the in-memory cache. `0` uses the client
    /// default (1 000).
    #[serde(default)]
    pub max_cache_entries: usize,

    /// Maximum idle keep-alive connections per host. `0` uses the `reqwest` default.
    ///
    /// Confluent flavour only; the Apicurio client does not expose it.
    #[serde(default)]
    pub pool_max_idle_per_host: usize,

    /// Retry policy for **transient** registry failures (transport errors, HTTP 429,
    /// HTTP 5xx). Not-found, auth and invalid-schema fail immediately.
    ///
    /// Schema resolution sits on the encode path, so without retries a single 503 fails
    /// the event and takes the pipeline down for something that clears itself in seconds.
    #[serde(default)]
    pub retry: RegistryRetryConfig,

    /// Verify the registry at startup instead of on the first event.
    ///
    /// Checks reachability, then either that the subjects carry rustcdc's schema
    /// (`auto_register = false`) or that rustcdc's schema is compatible with what is
    /// already registered (`auto_register = true`). Endpoints a registry does not
    /// implement are skipped rather than treated as failures.
    ///
    /// Confluent flavour only — the Apicurio native client is built lazily and makes no
    /// connection until first use.
    #[serde(default = "default_true")]
    pub preflight: bool,

    /// Schemas this one depends on, registered as Confluent **schema references**.
    ///
    /// rustcdc's envelope has no dependencies, so this is empty by default. It exists
    /// for deployments that register the envelope in a subject namespace where types
    /// are shared rather than inlined; without them, registration fails to resolve.
    #[serde(default)]
    pub references: Vec<SchemaReferenceConfig>,
}

/// One Confluent schema reference.
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
pub struct SchemaReferenceConfig {
    /// The name used inside the schema to refer to the dependency
    /// (e.g. `com.example.Address`).
    pub name: String,
    /// The subject the referenced schema is registered under.
    pub subject: String,
    /// The version of that subject to bind to.
    pub version: i32,
}

/// Jittered exponential back-off for transient registry failures.
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
pub struct RegistryRetryConfig {
    /// Set `false` to disable retries entirely — use this when an outer layer already
    /// retries and you do not want the two to multiply.
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Maximum retry attempts after the initial try.
    #[serde(default = "default_retry_max_retries")]
    pub max_retries: u32,
    /// First back-off delay, in milliseconds.
    #[serde(default = "default_retry_base_backoff_ms")]
    pub base_backoff_ms: u64,
    /// Ceiling for the back-off delay, in milliseconds.
    #[serde(default = "default_retry_max_backoff_ms")]
    pub max_backoff_ms: u64,
}

impl Default for RegistryRetryConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_retries: default_retry_max_retries(),
            base_backoff_ms: default_retry_base_backoff_ms(),
            max_backoff_ms: default_retry_max_backoff_ms(),
        }
    }
}

fn default_true() -> bool {
    true
}

fn default_registry_request_timeout_ms() -> u64 {
    30_000
}

fn default_retry_max_retries() -> u32 {
    3
}

fn default_retry_base_backoff_ms() -> u64 {
    100
}

fn default_retry_max_backoff_ms() -> u64 {
    5_000
}

/// Credentials resolved from the environment.
pub enum ResolvedRegistryAuth {
    /// No credentials configured.
    None,
    /// HTTP basic auth.
    Basic {
        /// Registry username or API key.
        username: String,
        /// Registry password or API secret.
        password: String,
    },
    /// OAuth / IAM bearer token.
    Bearer(String),
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
        if self.token_env.is_some() && self.username_env.is_some() {
            return Err(
                "registry.token_env and registry.username_env are mutually exclusive; \
                 pick bearer-token or basic auth"
                    .to_string(),
            );
        }
        if self.flavor == RegistryFlavor::Apicurio {
            // The Apicurio client appends `/apis/registry/v3` itself. A URL that already
            // carries it produces a doubled path and a 404 that reads like the registry
            // is down, so reject it here where the remedy is obvious.
            let trimmed = self.url.trim_end_matches('/');
            if trimmed.ends_with("/apis/registry/v3") || trimmed.contains("/apis/ccompat/") {
                return Err(format!(
                    "registry.url '{}' already carries an API path, but flavor = \"apicurio\" \
                     expects the server root and appends /apis/registry/v3 itself. Use the \
                     server root, or set flavor = \"confluent\" to drive Apicurio through its \
                     Confluent-compatible API at /apis/ccompat/v7.",
                    self.url
                ));
            }
        }
        if self.retry.enabled && self.retry.max_backoff_ms < self.retry.base_backoff_ms {
            return Err(format!(
                "registry.retry.max_backoff_ms ({}) must be >= base_backoff_ms ({})",
                self.retry.max_backoff_ms, self.retry.base_backoff_ms
            ));
        }
        for (i, reference) in self.references.iter().enumerate() {
            if reference.name.trim().is_empty() || reference.subject.trim().is_empty() {
                return Err(format!(
                    "registry.references[{i}]: both `name` and `subject` are required"
                ));
            }
        }
        Ok(())
    }

    /// Resolve registry credentials from environment variables at runtime.
    pub fn resolve_auth(&self) -> Result<ResolvedRegistryAuth, String> {
        if let Some(var) = self.token_env.as_deref() {
            let token = std::env::var(var).map_err(|_| {
                format!("registry.token_env: environment variable '{var}' is not set")
            })?;
            return Ok(ResolvedRegistryAuth::Bearer(token));
        }

        let Some(user_var) = self.username_env.as_deref() else {
            return Ok(ResolvedRegistryAuth::None);
        };
        let username = std::env::var(user_var).map_err(|_| {
            format!("registry.username_env: environment variable '{user_var}' is not set")
        })?;

        // `validate()` already rejects a username without a password, so this is a
        // config that skipped validation rather than an operator mistake reachable
        // through the normal path.
        let pass_var = self.password_env.as_deref().ok_or_else(|| {
            "registry.password_env is required when username_env is set".to_string()
        })?;
        let password = std::env::var(pass_var).map_err(|_| {
            format!("registry.password_env: environment variable '{pass_var}' is not set")
        })?;

        Ok(ResolvedRegistryAuth::Basic { username, password })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base() -> ConfluentRegistryConfig {
        ConfluentRegistryConfig {
            url: "https://sr.example.com".to_string(),
            flavor: RegistryFlavor::Confluent,
            username_env: None,
            password_env: None,
            token_env: None,
            subject_name_strategy: SubjectNameStrategy::TopicName,
            allow_insecure: false,
            request_timeout_ms: 30_000,
            connect_timeout_ms: 0,
            auto_register: true,
            normalize_schemas: false,
            max_cache_entries: 0,
            pool_max_idle_per_host: 0,
            retry: RegistryRetryConfig::default(),
            preflight: true,
            references: Vec::new(),
        }
    }

    #[test]
    fn rejects_bearer_and_basic_together() {
        let mut cfg = base();
        cfg.token_env = Some("SR_TOKEN".to_string());
        cfg.username_env = Some("SR_USER".to_string());
        cfg.password_env = Some("SR_PASS".to_string());
        let err = cfg.validate().expect_err("must reject");
        assert!(err.contains("mutually exclusive"), "unexpected: {err}");
    }

    /// The Apicurio native client appends its own API path; a URL that already
    /// carries one produces a 404 that looks like an outage.
    #[test]
    fn rejects_apicurio_url_with_api_path() {
        let mut cfg = base();
        cfg.flavor = RegistryFlavor::Apicurio;
        cfg.url = "https://apicurio.example.com/apis/registry/v3".to_string();
        let err = cfg.validate().expect_err("must reject");
        assert!(err.contains("server root"), "unexpected: {err}");
    }

    #[test]
    fn accepts_ccompat_path_for_confluent_flavor() {
        let mut cfg = base();
        cfg.url = "https://apicurio.example.com/apis/ccompat/v7".to_string();
        cfg.validate().expect("ccompat is a valid confluent URL");
    }

    #[test]
    fn rejects_inverted_backoff_bounds() {
        let mut cfg = base();
        cfg.retry.base_backoff_ms = 5_000;
        cfg.retry.max_backoff_ms = 100;
        let err = cfg.validate().expect_err("must reject");
        assert!(err.contains("max_backoff_ms"), "unexpected: {err}");
    }
}
