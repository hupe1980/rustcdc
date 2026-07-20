use figment::{
    providers::{Env, Format, Toml},
    Figment,
};
use std::path::Path;

use super::schema::{AdminProbeAuthMode, AppConfig, DeliveryContract, SinkConfig, StateBackend};
use super::source_profile::validate_source_config;
use crate::error::ConfigError;
use crate::token_manifest_policy;

/// Load the configuration from (in ascending priority):
///
/// 1. The TOML file at `config_path`
/// 2. Environment variables prefixed with `RUSTCDC_`, using `__ as the nested-
///    key separator (e.g. `RUSTCDC_SOURCE__POSTGRES__HOST` → `source.postgres.host`)
///
/// The caller is responsible for applying any additional CLI overrides on top of
/// the returned `AppConfig`.
pub fn load(config_path: &Path) -> Result<AppConfig, ConfigError> {
    load_and_migrate(config_path)
}

pub fn load_and_migrate(config_path: &Path) -> Result<AppConfig, ConfigError> {
    let raw: serde_json::Value = Figment::new()
        .merge(Toml::file(config_path))
        .merge(Env::prefixed("RUSTCDC_").map(|key| {
            // RUSTCDC_SOURCE__POSTGRES__HOST  →  source.postgres.host
            key.as_str().to_lowercase().replace("__", ".").into()
        }))
        .extract()
        .map_err(|e| ConfigError::Load(Box::new(e)))?;

    let mut migrated_raw =
        super::migrations::load_and_migrate_value(raw, AppConfig::SUPPORTED_API_VERSION)?;

    // Token-like credentials must never appear as plaintext literals in the
    // config file. This is checked on the raw document because after
    // `resolve_env_secret_references` an env-sourced value and a hardcoded one
    // are indistinguishable.
    enforce_deferred_secret_literals(&migrated_raw)?;

    // Resolve `{ env = "VAR" }` references into their environment values so
    // every secret-bearing field can be populated without embedding plaintext
    // credentials in the file.
    resolve_env_secret_references(&mut migrated_raw, "")?;

    let config: AppConfig = serde_json::from_value(migrated_raw).map_err(|e| {
        ConfigError::InvalidState(format!("failed to deserialize migrated configuration: {e}"))
    })?;

    validate(&config)?;
    Ok(config)
}

/// Is this JSON value an `{ env = "VAR" }` reference?
fn as_env_reference(value: &serde_json::Value) -> Option<&str> {
    let obj = value.as_object()?;
    if obj.len() != 1 {
        return None;
    }
    obj.get("env").and_then(serde_json::Value::as_str)
}

/// Recursively replace `{ env = "VAR" }` objects with the value of `$VAR`.
///
/// Runs on the raw (already-migrated) document, so it applies uniformly to every
/// secret-bearing field — source passwords, sink tokens, state-backend URLs —
/// without each field needing its own deserializer. An unset variable is a hard
/// error naming both the variable and the config path, because silently
/// defaulting a credential to an empty string produces a far less debuggable
/// failure at connect time.
fn resolve_env_secret_references(
    value: &mut serde_json::Value,
    path: &str,
) -> Result<(), ConfigError> {
    if let Some(var_name) = as_env_reference(value) {
        let resolved = std::env::var(var_name).map_err(|_| {
            ConfigError::InvalidState(format!(
                "environment variable '{var_name}' referenced by '{path}' is not set"
            ))
        })?;
        *value = serde_json::Value::String(resolved);
        return Ok(());
    }

    match value {
        serde_json::Value::Object(map) => {
            for (key, child) in map.iter_mut() {
                let child_path = if path.is_empty() {
                    key.clone()
                } else {
                    format!("{path}.{key}")
                };
                resolve_env_secret_references(child, &child_path)?;
            }
        }
        serde_json::Value::Array(items) => {
            for (index, child) in items.iter_mut().enumerate() {
                resolve_env_secret_references(child, &format!("{path}[{index}]"))?;
            }
        }
        _ => {}
    }
    Ok(())
}

/// Reject plaintext literals for token-like credentials in the config file.
///
/// Checked on the raw document (before `resolve_env_secret_references`) because
/// afterwards an env-sourced token and a hardcoded one are indistinguishable.
/// Passwords deliberately allow inline values (with a loud warning from
/// rustcdc); bearer tokens and catalog credentials are long-lived and routinely
/// pasted into version control, so those stay hard-rejected.
fn enforce_deferred_secret_literals(raw: &serde_json::Value) -> Result<(), ConfigError> {
    let mut sink_values: Vec<&serde_json::Value> = Vec::new();
    if let Some(sink) = raw.get("sink") {
        sink_values.push(sink);
    }
    if let Some(sinks) = raw.get("sinks").and_then(serde_json::Value::as_array) {
        sink_values.extend(sinks.iter());
    }

    for sink in sink_values {
        let sink_type = sink.get("type").and_then(serde_json::Value::as_str);

        if sink_type == Some("http") {
            if let Some(token) = sink.get("bearer_token") {
                if token.is_string() {
                    return Err(ConfigError::InvalidState(
                        "sink.http.bearer_token must use deferred secret references (for example { env = \"VAR\" })"
                            .to_string(),
                    ));
                }
            }
        }

        if sink_type == Some("iceberg") {
            for field in ["token", "credential"] {
                let value = sink
                    .get("catalog")
                    .and_then(|c| c.get("rest"))
                    .and_then(|r| r.get(field));
                if let Some(value) = value {
                    if value.is_string() {
                        return Err(ConfigError::InvalidState(format!(
                            "sink.iceberg.catalog.rest.{field} must use deferred secret references (for example {{ env = \"VAR\" }})"
                        )));
                    }
                }
            }
        }
    }

    Ok(())
}

/// Apply CLI-level overrides to an already-loaded config.
pub fn apply_run_overrides(
    config: &mut AppConfig,
    state_dir: Option<std::path::PathBuf>,
    snapshot_tables: Vec<String>,
) {
    if let Some(dir) = state_dir {
        config.state.offset.dir = dir.clone();
        config.state.schema_history.dir = dir;
    }
    if !snapshot_tables.is_empty() {
        config.snapshot_tables = snapshot_tables;
    }
}

fn validate(config: &AppConfig) -> Result<(), ConfigError> {
    if config.api_version != AppConfig::SUPPORTED_API_VERSION {
        return Err(ConfigError::InvalidApiVersion(config.api_version.clone()));
    }

    validate_source_config(config)?;

    // State dir must be a non-empty path.
    if config.state.offset.dir.as_os_str().is_empty() {
        return Err(ConfigError::InvalidState(
            "state.offset.dir must not be empty".to_string(),
        ));
    }

    if config.admin.timeout_ms == 0 {
        return Err(ConfigError::InvalidState(
            "admin.timeout_ms must be > 0".to_string(),
        ));
    }

    if config.admin.metrics_rate_limit_rps == 0 {
        return Err(ConfigError::InvalidState(
            "admin.metrics_rate_limit_rps must be > 0".to_string(),
        ));
    }

    if config.admin.metrics_rate_limit_burst == 0 {
        return Err(ConfigError::InvalidState(
            "admin.metrics_rate_limit_burst must be > 0".to_string(),
        ));
    }

    if config.admin.metrics_rate_limit_burst < config.admin.metrics_rate_limit_rps {
        return Err(ConfigError::InvalidState(
            "admin.metrics_rate_limit_burst must be >= admin.metrics_rate_limit_rps".to_string(),
        ));
    }

    if config.admin.readyz_rate_limit_rps == 0 {
        return Err(ConfigError::InvalidState(
            "admin.readyz_rate_limit_rps must be > 0".to_string(),
        ));
    }

    if config.admin.readyz_rate_limit_burst == 0 {
        return Err(ConfigError::InvalidState(
            "admin.readyz_rate_limit_burst must be > 0".to_string(),
        ));
    }

    if config.admin.readyz_rate_limit_burst < config.admin.readyz_rate_limit_rps {
        return Err(ConfigError::InvalidState(
            "admin.readyz_rate_limit_burst must be >= admin.readyz_rate_limit_rps".to_string(),
        ));
    }

    if config.admin.status_rate_limit_rps == 0 {
        return Err(ConfigError::InvalidState(
            "admin.status_rate_limit_rps must be > 0".to_string(),
        ));
    }

    if config.admin.status_rate_limit_burst == 0 {
        return Err(ConfigError::InvalidState(
            "admin.status_rate_limit_burst must be > 0".to_string(),
        ));
    }

    if config.admin.status_rate_limit_burst < config.admin.status_rate_limit_rps {
        return Err(ConfigError::InvalidState(
            "admin.status_rate_limit_burst must be >= admin.status_rate_limit_rps".to_string(),
        ));
    }

    if config.admin.enabled {
        let admin_addr: std::net::SocketAddr = config.admin.bind.parse().map_err(|e| {
            ConfigError::InvalidState(format!("admin.bind must be a valid socket address: {e}"))
        })?;

        let has_manifest_auth = config.admin.token_manifest_file.is_some();

        if matches!(
            config.admin.probe_auth_mode,
            AdminProbeAuthMode::AllowUnauthenticatedLoopback
        ) && !admin_addr.ip().is_loopback()
        {
            return Err(ConfigError::InvalidState(
                "admin.probe_auth_mode=allow_unauthenticated_loopback requires loopback admin.bind"
                    .to_string(),
            ));
        }

        if config
            .admin
            .read_token_env
            .as_ref()
            .is_some_and(|name| name.trim().is_empty())
        {
            return Err(ConfigError::InvalidState(
                "admin.read_token_env must not be empty when configured".to_string(),
            ));
        }

        if config
            .admin
            .write_token_env
            .as_ref()
            .is_some_and(|name| name.trim().is_empty())
        {
            return Err(ConfigError::InvalidState(
                "admin.write_token_env must not be empty when configured".to_string(),
            ));
        }

        if config
            .admin
            .audit_signing_key_env
            .as_ref()
            .is_some_and(|name| name.trim().is_empty())
        {
            return Err(ConfigError::InvalidState(
                "admin.audit_signing_key_env must not be empty when configured".to_string(),
            ));
        }

        if let Some(audit_log_file) = &config.admin.audit_log_file {
            if audit_log_file.as_os_str().is_empty() {
                return Err(ConfigError::InvalidState(
                    "admin.audit_log_file must not be empty when configured".to_string(),
                ));
            }

            if audit_log_file.exists() && !audit_log_file.is_file() {
                return Err(ConfigError::InvalidState(format!(
                    "admin.audit_log_file exists but is not a file: {}",
                    audit_log_file.display()
                )));
            }

            if let Some(parent) = audit_log_file.parent() {
                if !parent.as_os_str().is_empty() && !parent.is_dir() {
                    return Err(ConfigError::InvalidState(format!(
                        "admin.audit_log_file parent directory does not exist: {}",
                        parent.display()
                    )));
                }
            }
        }

        if let Some(notification_log_file) = &config.admin.notification_log_file {
            if notification_log_file.as_os_str().is_empty() {
                return Err(ConfigError::InvalidState(
                    "admin.notification_log_file must not be empty when configured".to_string(),
                ));
            }

            if notification_log_file.exists() && !notification_log_file.is_file() {
                return Err(ConfigError::InvalidState(format!(
                    "admin.notification_log_file exists but is not a file: {}",
                    notification_log_file.display()
                )));
            }

            if let Some(parent) = notification_log_file.parent() {
                if !parent.as_os_str().is_empty() && !parent.is_dir() {
                    return Err(ConfigError::InvalidState(format!(
                        "admin.notification_log_file parent directory does not exist: {}",
                        parent.display()
                    )));
                }
            }
        }

        if let Some(signal_ingress_file) = &config.admin.signal_ingress_file {
            if signal_ingress_file.as_os_str().is_empty() {
                return Err(ConfigError::InvalidState(
                    "admin.signal_ingress_file must not be empty when configured".to_string(),
                ));
            }

            if signal_ingress_file.exists() && !signal_ingress_file.is_file() {
                return Err(ConfigError::InvalidState(format!(
                    "admin.signal_ingress_file exists but is not a file: {}",
                    signal_ingress_file.display()
                )));
            }

            if let Some(parent) = signal_ingress_file.parent() {
                if !parent.as_os_str().is_empty() && !parent.is_dir() {
                    return Err(ConfigError::InvalidState(format!(
                        "admin.signal_ingress_file parent directory does not exist: {}",
                        parent.display()
                    )));
                }
            }
        }

        if let Some(notification_kafka) = &config.admin.notification_kafka {
            notification_kafka
                .validate()
                .map_err(ConfigError::InvalidState)?;
        }

        if let Some(signal_ingress_kafka) = &config.admin.signal_ingress_kafka {
            signal_ingress_kafka
                .validate()
                .map_err(ConfigError::InvalidState)?;
        }

        if (config.admin.write_token_env.is_some() || has_manifest_auth)
            && config.admin.notification_log_file.is_none()
            && config.admin.notification_kafka.is_none()
        {
            return Err(ConfigError::InvalidState(
                "admin.notification_log_file or admin.notification_kafka is required when write-capable admin signaling is enabled"
                    .to_string(),
            ));
        }

        if let Some(manifest_file) = &config.admin.token_manifest_file {
            if config.admin.token_manifest_max_staleness_ms.is_none() {
                return Err(ConfigError::InvalidState(
                        "admin.token_manifest_max_staleness_ms is required when admin.token_manifest_file is configured"
                            .to_string(),
                    ));
            }
            if manifest_file.as_os_str().is_empty() {
                return Err(ConfigError::InvalidState(
                    "admin.token_manifest_file must not be empty when configured".to_string(),
                ));
            }
            if !manifest_file.is_file() {
                return Err(ConfigError::InvalidState(format!(
                    "admin.token_manifest_file does not exist or is not a file: {}",
                    manifest_file.display()
                )));
            }
            let trusted_keys = token_manifest_policy::parse_trusted_manifest_keys(
                &config.admin.token_manifest_trusted_public_keys_hex,
            )
            .map_err(ConfigError::InvalidState)?;
            let manifest =
                token_manifest_policy::load_signed_token_manifest(manifest_file, &trusted_keys)
                    .map_err(ConfigError::InvalidState)?;
            if !token_manifest_policy::has_write_scope(&manifest.tokens) {
                return Err(ConfigError::InvalidState(
                    "admin.token_manifest_file must contain at least one token with write scope"
                        .to_string(),
                ));
            }
        }

        if !config
            .admin
            .token_manifest_trusted_public_keys_hex
            .is_empty()
            && config.admin.token_manifest_file.is_none()
        {
            return Err(ConfigError::InvalidState(
                "admin.token_manifest_trusted_public_keys_hex requires admin.token_manifest_file"
                    .to_string(),
            ));
        }

        if config.admin.token_manifest_refresh_ms == 0 {
            return Err(ConfigError::InvalidState(
                "admin.token_manifest_refresh_ms must be > 0".to_string(),
            ));
        }

        for proxy_ip in &config.admin.trusted_proxy_ips {
            if proxy_ip.trim().is_empty() {
                return Err(ConfigError::InvalidState(
                    "admin.trusted_proxy_ips entries must not be empty".to_string(),
                ));
            }

            if proxy_ip.parse::<std::net::IpAddr>().is_err() {
                return Err(ConfigError::InvalidState(format!(
                    "admin.trusted_proxy_ips contains invalid IP address '{proxy_ip}'"
                )));
            }
        }

        if let Some(max_staleness_ms) = config.admin.token_manifest_max_staleness_ms {
            if max_staleness_ms == 0 {
                return Err(ConfigError::InvalidState(
                    "admin.token_manifest_max_staleness_ms must be > 0 when configured".to_string(),
                ));
            }

            if config.admin.token_manifest_file.is_none() {
                return Err(ConfigError::InvalidState(
                    "admin.token_manifest_max_staleness_ms requires admin.token_manifest_file"
                        .to_string(),
                ));
            }
        }

        if !admin_addr.ip().is_loopback() {
            if config.admin.read_token_env.is_none() && !has_manifest_auth {
                return Err(ConfigError::InvalidState(
                    "admin.read_token_env or admin.token_manifest_file is required when admin.bind is non-loopback"
                        .to_string(),
                ));
            }
            if config.admin.write_token_env.is_none() && !has_manifest_auth {
                return Err(ConfigError::InvalidState(
                    "admin.write_token_env or admin.token_manifest_file is required when admin.bind is non-loopback"
                        .to_string(),
                ));
            }
            if config.admin.tls.is_none() {
                return Err(ConfigError::InvalidState(
                    "admin.tls is required when admin.bind is non-loopback".to_string(),
                ));
            }
        }

        if let Some(tls) = &config.admin.tls {
            if tls.cert_file.as_os_str().is_empty() {
                return Err(ConfigError::InvalidState(
                    "admin.tls.cert_file must not be empty".to_string(),
                ));
            }
            if tls.key_file.as_os_str().is_empty() {
                return Err(ConfigError::InvalidState(
                    "admin.tls.key_file must not be empty".to_string(),
                ));
            }
            if !tls.cert_file.is_file() {
                return Err(ConfigError::InvalidState(format!(
                    "admin.tls.cert_file does not exist or is not a file: {}",
                    tls.cert_file.display()
                )));
            }
            if !tls.key_file.is_file() {
                return Err(ConfigError::InvalidState(format!(
                    "admin.tls.key_file does not exist or is not a file: {}",
                    tls.key_file.display()
                )));
            }

            if tls.require_client_cert && tls.client_ca_file.is_none() {
                return Err(ConfigError::InvalidState(
                    "admin.tls.client_ca_file is required when admin.tls.require_client_cert=true"
                        .to_string(),
                ));
            }

            if let Some(ca_file) = &tls.client_ca_file {
                if ca_file.as_os_str().is_empty() {
                    return Err(ConfigError::InvalidState(
                        "admin.tls.client_ca_file must not be empty when configured".to_string(),
                    ));
                }
                if !ca_file.is_file() {
                    return Err(ConfigError::InvalidState(format!(
                        "admin.tls.client_ca_file does not exist or is not a file: {}",
                        ca_file.display()
                    )));
                }
            }
        }
    }

    if let SinkConfig::Http(http) = &config.sink {
        if http.url.trim().is_empty() {
            return Err(ConfigError::InvalidState(
                "sink.http.url must not be empty".to_string(),
            ));
        }
        if !http.verify_tls {
            return Err(ConfigError::InvalidState(
                "sink.http.verify_tls must be true; insecure HTTP TLS bypass is unsupported"
                    .to_string(),
            ));
        }
        validate_http_sink_url_policy(&http.url)?;
        if http.timeout_ms == 0 {
            return Err(ConfigError::InvalidState(
                "sink.http.timeout_ms must be > 0".to_string(),
            ));
        }
        if http.batch_max_events == 0 {
            return Err(ConfigError::InvalidState(
                "sink.http.batch_max_events must be > 0".to_string(),
            ));
        }
        if http.batch_max_delay_ms == 0 {
            return Err(ConfigError::InvalidState(
                "sink.http.batch_max_delay_ms must be > 0".to_string(),
            ));
        }
        if http.max_pending_bytes == 0 {
            return Err(ConfigError::InvalidState(
                "sink.http.max_pending_bytes must be > 0".to_string(),
            ));
        }
        if http.backoff_multiplier < 1.0 {
            return Err(ConfigError::InvalidState(
                "sink.http.backoff_multiplier must be >= 1.0".to_string(),
            ));
        }
        if !http.backoff_multiplier.is_finite() {
            return Err(ConfigError::InvalidState(
                "sink.http.backoff_multiplier must be finite".to_string(),
            ));
        }
        if http.backoff_initial_ms == 0 || http.backoff_max_ms == 0 {
            return Err(ConfigError::InvalidState(
                "sink.http.backoff_initial_ms and backoff_max_ms must be > 0".to_string(),
            ));
        }
        if http.backoff_initial_ms > http.backoff_max_ms {
            return Err(ConfigError::InvalidState(
                "sink.http.backoff_initial_ms must be <= backoff_max_ms".to_string(),
            ));
        }
        if http.batch_retry_time_budget_ms == 0 {
            return Err(ConfigError::InvalidState(
                "sink.http.batch_retry_time_budget_ms must be > 0".to_string(),
            ));
        }
        if http.dlq_max_bytes == 0 {
            return Err(ConfigError::InvalidState(
                "sink.http.dlq_max_bytes must be > 0".to_string(),
            ));
        }
        if let Some(dlq_path) = &http.dlq_path {
            if dlq_path.as_os_str().is_empty() {
                return Err(ConfigError::InvalidState(
                    "sink.http.dlq_path must not be empty when configured".to_string(),
                ));
            }

            if dlq_path.exists() && !dlq_path.is_file() {
                return Err(ConfigError::InvalidState(format!(
                    "sink.http.dlq_path exists but is not a file: {}",
                    dlq_path.display()
                )));
            }

            if let Some(parent) = dlq_path.parent() {
                if !parent.as_os_str().is_empty() && !parent.is_dir() {
                    return Err(ConfigError::InvalidState(format!(
                        "sink.http.dlq_path parent directory does not exist: {}",
                        parent.display()
                    )));
                }
            }
        }
        if let Some(token) = &http.bearer_token {
            // Plaintext literals in the config file are rejected earlier, on the
            // raw document (`enforce_deferred_secret_literals`) — by this point
            // an inline value is the resolved form of an `{ env = … }` reference.
            let resolved = token.resolve().map_err(|e| {
                ConfigError::InvalidState(format!(
                    "sink.http.bearer_token could not be resolved: {e}"
                ))
            })?;
            if resolved.trim().is_empty() {
                return Err(ConfigError::InvalidState(
                    "sink.http.bearer_token must not be empty when configured".to_string(),
                ));
            }
        }
    }

    if let SinkConfig::Kafka(kafka) = &config.sink {
        kafka.validate().map_err(ConfigError::InvalidState)?;
    }

    if let SinkConfig::Iceberg(iceberg) = &config.sink {
        iceberg.validate().map_err(ConfigError::InvalidState)?;
    }

    if let SinkConfig::Fan(fan) = &config.sink {
        validate_fan_sink_config(fan)?;
    }

    validate_delivery_contract(config)?;

    match &config.state.offset.backend {
        StateBackend::LocalFs => {}
        StateBackend::KafkaTopic(kafka_state) => {
            kafka_state.validate().map_err(ConfigError::InvalidState)?;
        }
        StateBackend::Redis(redis_config) => {
            let resolved_url = redis_config.url.resolve().map_err(|e| {
                ConfigError::InvalidState(format!(
                    "state.offset.redis.url could not be resolved: {e}"
                ))
            })?;
            if resolved_url.trim().is_empty() {
                return Err(ConfigError::InvalidState(
                    "state.offset.redis.url must not be empty".to_string(),
                ));
            }
        }
        StateBackend::Postgresql(pg_config) => {
            let resolved_url = pg_config.url.resolve().unwrap_or_default();
            if resolved_url.trim().is_empty() {
                return Err(ConfigError::InvalidState(
                    "state.offset.postgresql.url must not be empty".to_string(),
                ));
            }
        }
    }

    for rule in &config.pipeline.transforms {
        rule.validate().map_err(ConfigError::InvalidState)?;
    }

    config
        .runtime
        .validate()
        .map_err(ConfigError::InvalidState)?;

    config
        .pipeline
        .transform_runtime
        .validate()
        .map_err(ConfigError::InvalidState)?;

    Ok(())
}

fn validate_delivery_contract(config: &AppConfig) -> Result<(), ConfigError> {
    if let SinkConfig::Kafka(kafka) = &config.sink {
        if matches!(
            kafka.delivery_mode,
            super::schema::KafkaDeliveryMode::Transactional
        ) && config.delivery_contract != DeliveryContract::EffectivelyOnce
        {
            return Err(ConfigError::InvalidState(
                "delivery_contract must be \"effectively_once\" when sink.kafka.delivery_mode=\"transactional\""
                    .to_string(),
            ));
        }
    }

    let sink_name = sink_name(&config.sink);
    let idempotent_delivery_capable = sink_idempotent_delivery_capable(&config.sink);
    let transactional_checkpoint_barrier_capable =
        sink_transactional_checkpoint_barrier_capable(&config.sink);

    if !config.delivery_contract.is_satisfied_by(
        idempotent_delivery_capable,
        transactional_checkpoint_barrier_capable,
    ) {
        let mut requirements = Vec::new();
        if config.delivery_contract.requires_idempotent_delivery() {
            requirements.push("idempotent delivery");
        }
        if config
            .delivery_contract
            .requires_transactional_checkpoint_barrier()
        {
            requirements.push("transactional checkpoint barrier coupling");
        }

        return Err(ConfigError::InvalidState(format!(
            "delivery_contract='{}' is incompatible with sink.type='{}': missing {}",
            config.delivery_contract.as_label(),
            sink_name,
            requirements.join(" and ")
        )));
    }

    Ok(())
}

fn sink_name(sink: &SinkConfig) -> &'static str {
    match sink {
        SinkConfig::Stdout(_) => "stdout",
        SinkConfig::FileJsonl(_) => "file_jsonl",
        SinkConfig::Http(_) => "http",
        SinkConfig::Kafka(_) => "kafka",
        SinkConfig::Iceberg(_) => "iceberg",
        SinkConfig::Fan(_) => "fan_out",
    }
}

fn sink_idempotent_delivery_capable(sink: &SinkConfig) -> bool {
    match sink {
        SinkConfig::Kafka(_) => true,
        SinkConfig::Fan(fan) => fan.sinks.iter().all(sink_idempotent_delivery_capable),
        _ => false,
    }
}

fn sink_transactional_checkpoint_barrier_capable(sink: &SinkConfig) -> bool {
    match sink {
        SinkConfig::Kafka(kafka) => {
            matches!(
                kafka.delivery_mode,
                super::schema::KafkaDeliveryMode::Transactional
            )
        }
        SinkConfig::Fan(fan) => fan
            .sinks
            .iter()
            .all(sink_transactional_checkpoint_barrier_capable),
        _ => false,
    }
}

fn validate_fan_sink_config(fan: &super::schema::FanSinkConfig) -> Result<(), ConfigError> {
    if fan.sinks.is_empty() {
        return Err(ConfigError::InvalidState(
            "sink.fan.sinks must contain at least one child sink".to_string(),
        ));
    }
    for (i, child) in fan.sinks.iter().enumerate() {
        if matches!(child, SinkConfig::Fan(_)) {
            return Err(ConfigError::InvalidState(format!(
                "sink.fan.sinks[{i}]: nested fan-out sinks are not supported"
            )));
        }
    }
    Ok(())
}

pub(crate) fn validate_http_sink_url_policy(url: &str) -> Result<(), ConfigError> {
    let parsed = reqwest::Url::parse(url).map_err(|e| {
        ConfigError::InvalidState(format!("sink.http.url must be a valid URL: {e}"))
    })?;

    match parsed.scheme() {
        "https" => Ok(()),
        "http" => {
            let host = parsed.host_str().unwrap_or_default();
            if matches!(host, "localhost" | "127.0.0.1" | "::1") {
                Ok(())
            } else {
                Err(ConfigError::InvalidState(
                    "sink.http.url must use https except for localhost loopback testing"
                        .to_string(),
                ))
            }
        }
        other => Err(ConfigError::InvalidState(format!(
            "sink.http.url scheme must be https (or http on localhost); found '{other}'"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use super::{load, load_and_migrate};
    use crate::config::schema::AppConfig;
    use crate::token_manifest_policy::{
        canonical_signing_payload, TokenManifestFile, TokenManifestSignature, TokenManifestToken,
        TokenManifestUnsigned,
    };
    use ed25519_dalek::{Signer, SigningKey};

    fn write_signed_manifest(path: &Path, tokens: Vec<TokenManifestToken>) -> String {
        let signing_key = SigningKey::from_bytes(&[0x11; 32]);
        let public_key_hex = hex::encode(signing_key.verifying_key().to_bytes());

        let unsigned = TokenManifestUnsigned {
            tokens: tokens.clone(),
        };
        let unsigned_bytes =
            canonical_signing_payload(&unsigned).expect("serialize unsigned manifest");
        let signature = signing_key.sign(&unsigned_bytes);

        let manifest = TokenManifestFile {
            tokens,
            signature: TokenManifestSignature {
                algorithm: "ed25519".to_string(),
                public_key_hex: public_key_hex.clone(),
                signature_hex: hex::encode(signature.to_bytes()),
            },
        };

        std::fs::write(
            path,
            serde_json::to_vec_pretty(&manifest).expect("serialize signed manifest"),
        )
        .expect("write manifest");

        public_key_hex
    }

    #[test]
    fn loads_valid_config_without_admin_or_observability_sections() {
        let dir = tempfile::tempdir().expect("tempdir");
        let state_dir = dir.path().join("state");
        let config_path = dir.path().join("cdc.toml");

        std::fs::write(
            &config_path,
            format!(
                r#"
api_version = "v1alpha1"
delivery_contract = "at_least_once"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = "secret"
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "kafka"
brokers = "localhost:9092"
topic = "cdc-events"

[state]
dir = "{}"
"#,
                state_dir.display()
            ),
        )
        .expect("write config");

        let cfg = load(&config_path).expect("config should load");
        assert_eq!(cfg.admin.bind, "127.0.0.1:8080");
        assert_eq!(cfg.observability.service_name, "rustcdc-server");
        assert!(cfg.observability.otlp_endpoint.is_none());
    }

    #[test]
    fn rejects_unsupported_older_api_version() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config_path = dir.path().join("cdc.toml");

        std::fs::write(
            &config_path,
            r#"
api_version = "v1alpha0"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = "secret"
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-state"
"#,
        )
        .expect("write config");

        let err = load(&config_path).expect_err("unsupported older api version should fail closed");
        assert!(
            err.to_string()
                .contains("api_version must be \"v1\", got \"v1alpha0\""),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn rejects_unknown_api_version_when_no_migration_path_exists() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config_path = dir.path().join("cdc.toml");

        std::fs::write(
            &config_path,
            r#"
api_version = "v9"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = "secret"
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-state"
"#,
        )
        .expect("write config");

        let err = load(&config_path).expect_err("unknown api version should fail closed");
        assert!(
            err.to_string()
                .contains("api_version must be \"v1\", got \"v9\""),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn load_and_migrate_uses_supported_target_only() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config_path = dir.path().join("cdc.toml");

        std::fs::write(
            &config_path,
            r#"
api_version = "v1alpha1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = "secret"
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-state"
"#,
        )
        .expect("write config");

        let cfg = load_and_migrate(&config_path)
            .expect("load_and_migrate should succeed for supported version");
        assert_eq!(cfg.api_version, AppConfig::SUPPORTED_API_VERSION);
    }

    #[test]
    fn accepts_non_loopback_plaintext_postgres_source_transport() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config_path: PathBuf = dir.path().join("cdc.toml");

        std::fs::write(
            &config_path,
            r#"
api_version = "v1alpha1"

[source.postgres]
host = "postgres.internal"
port = 5432
user = "cdc_user"
password = "secret"
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-state"
"#,
        )
        .expect("write config");

        load(&config_path).expect("non-loopback plaintext should be allowed for DX");
    }

    #[test]
    fn accepts_non_loopback_postgres_source_with_tls_transport() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config_path: PathBuf = dir.path().join("cdc.toml");

        std::fs::write(
            &config_path,
            r#"
api_version = "v1alpha1"

[source.postgres]
host = "postgres.internal"
port = 5432
user = "cdc_user"
password = "secret"
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "tls"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-state"
"#,
        )
        .expect("write config");

        load(&config_path).expect("tls transport should pass for non-loopback source host");
    }

    #[test]
    fn accepts_mysql_source_profile() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config_path: PathBuf = dir.path().join("cdc.toml");

        std::fs::write(
            &config_path,
            r#"
api_version = "v1alpha1"

[source.mysql]
host = "localhost"
port = 3306
user = "cdc_user"
password = "secret"
database = "mydb"
server_id = 101
gtid_mode_enabled = false
binlog_format_check = true
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.mysql.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-state"
"#,
        )
        .expect("write config");

        load(&config_path).expect("mysql source should be accepted");
    }

    #[test]
    fn accepts_mariadb_source_profile() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config_path: PathBuf = dir.path().join("cdc.toml");

        std::fs::write(
            &config_path,
            r#"
api_version = "v1alpha1"

[source.mariadb]
host = "localhost"
port = 3306
user = "cdc_user"
password = "secret"
database = "mydb"
server_id = 202
gtid_mode_enabled = false
binlog_format_check = true
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.mariadb.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-state"
"#,
        )
        .expect("write config");

        load(&config_path).expect("mariadb source should be accepted");
    }

    #[test]
    fn accepts_mssql_source_profile_alias() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config_path: PathBuf = dir.path().join("cdc.toml");

        std::fs::write(
            &config_path,
            r#"
api_version = "v1alpha1"

[source.mssql]
host = "localhost"
port = 1433
user = "sa"
password = "secret"
database = "mydb"
instance_name = "SQLEXPRESS"
conn_timeout_secs = 10
cdc_enabled = true
cdc_schema = "cdc"
prereq_pool_size = 2
stream_poll_interval_ms = 1000
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.mssql.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-state"
"#,
        )
        .expect("write config");

        load(&config_path).expect("mssql alias source should be accepted");
    }

    #[test]
    fn rejects_multiple_source_profiles() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config_path: PathBuf = dir.path().join("cdc.toml");

        std::fs::write(
            &config_path,
            r#"
api_version = "v1alpha1"
delivery_contract = "at_least_once"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = "secret"
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[source.mysql]
host = "mysql.internal"
port = 3306

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-state"
"#,
        )
        .expect("write config");

        let err = load(&config_path).expect_err("multiple sources should fail closed");
        assert!(
            err.to_string()
                .contains("exactly one source block must be configured"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn rejects_effectively_once_contract_for_non_idempotent_sink() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config_path: PathBuf = dir.path().join("cdc.toml");

        std::fs::write(
            &config_path,
            r#"
api_version = "v1alpha1"
delivery_contract = "effectively_once"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = "secret"
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-state"
"#,
        )
        .expect("write config");

        let err = load(&config_path).expect_err("expected delivery contract rejection");
        assert!(
            err.to_string().contains(
                "delivery_contract='effectively_once' is incompatible with sink.type='stdout'"
            ),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn rejects_effectively_once_contract_for_non_transactional_kafka_sink() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config_path: PathBuf = dir.path().join("cdc.toml");

        std::fs::write(
            &config_path,
            r#"
api_version = "v1alpha1"
delivery_contract = "effectively_once"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = "secret"
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "kafka"
brokers = "localhost:9092"
topic = "cdc-events"

[state]
dir = "/tmp/cdc-state"
"#,
        )
        .expect("write config");

        let err = load(&config_path)
            .expect_err("expected effectively_once contract rejection without transactional mode");
        assert!(
            err.to_string().contains(
                "delivery_contract='effectively_once' is incompatible with sink.type='kafka': missing idempotent delivery and transactional checkpoint barrier coupling"
            ) || err.to_string().contains(
                "delivery_contract='effectively_once' is incompatible with sink.type='kafka': missing transactional checkpoint barrier coupling"
            ),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn accepts_effectively_once_contract_with_transactional_kafka_sink() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config_path: PathBuf = dir.path().join("cdc.toml");

        std::fs::write(
            &config_path,
            r#"
api_version = "v1alpha1"
delivery_contract = "effectively_once"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = "secret"
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "kafka"
brokers = "localhost:9092"
topic = "cdc-events"
delivery_mode = "transactional"
transactional_id = "cdc-eos-1"

[state]
dir = "/tmp/cdc-state"
"#,
        )
        .expect("write config");

        load(&config_path).expect("transactional kafka sink should satisfy effectively_once");
    }

    #[test]
    fn rejects_transactional_kafka_sink_when_delivery_contract_is_at_least_once() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config_path: PathBuf = dir.path().join("cdc.toml");

        std::fs::write(
            &config_path,
            r#"
api_version = "v1alpha1"
delivery_contract = "at_least_once"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = "secret"
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "kafka"
brokers = "localhost:9092"
topic = "cdc-events"
delivery_mode = "transactional"
transactional_id = "cdc-eos-1"

[state]
dir = "/tmp/cdc-state"
"#,
        )
        .expect("write config");

        let err = load(&config_path)
            .expect_err("expected delivery_contract mismatch for transactional kafka mode");
        assert!(
            err.to_string().contains(
                "delivery_contract must be \"effectively_once\" when sink.kafka.delivery_mode=\"transactional\""
            ),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn accepts_source_kind_when_matching_configured_profile() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config_path: PathBuf = dir.path().join("cdc.toml");

        std::fs::write(
            &config_path,
            r#"
api_version = "v1alpha1"

[source]
kind = "postgres"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = "secret"
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-state"
"#,
        )
        .expect("write config");

        load(&config_path).expect("matching source.kind should load");
    }

    #[test]
    fn rejects_source_kind_when_not_matching_configured_profile() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config_path: PathBuf = dir.path().join("cdc.toml");

        std::fs::write(
            &config_path,
            r#"
api_version = "v1alpha1"

[source]
kind = "postgres"

[source.mysql]
host = "mysql.internal"
port = 3306

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-state"
"#,
        )
        .expect("write config");

        let err = load(&config_path).expect_err("mismatched source.kind should fail");
        assert!(
            err.to_string().contains("missing field")
                || err.to_string().contains("failed to deserialize"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn rejects_iceberg_upsert_mode() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config_path: PathBuf = dir.path().join("cdc.toml");
        let table_path = dir.path().join("iceberg-table");

        std::fs::write(
            &config_path,
            format!(
                r#"
    api_version = "v1alpha1"

    [source.postgres]
    host = "localhost"
    port = 5432
    user = "cdc_user"
    password = "secret"
    database = "mydb"
    replication_slot_name = "cdc_slot"
    publication_name = "cdc_pub"
    conn_timeout_secs = 10
    stream_poll_interval_ms = 100
    max_events_per_poll = 1000
    table_include_list = []
    table_exclude_list = []

    [source.postgres.transport]
    mode = "plaintext"

    [sink]
    type = "iceberg"
    table_path = "{}"
    write_mode = "upsert"

    [sink.catalog.rest]
    uri = "http://127.0.0.1:8181"
    warehouse = "file:///tmp/cdc-iceberg-warehouse"

    [state]
    dir = "/tmp/cdc-state"
    "#,
                table_path.display()
            ),
        )
        .expect("write config");

        let err = load(&config_path).expect_err("expected invalid iceberg sink config");
        assert!(err.to_string().contains("unknown variant"));
    }

    #[test]
    fn rejects_wrong_api_version() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config_path: PathBuf = dir.path().join("cdc.toml");

        std::fs::write(
            &config_path,
            r#"
api_version = "v9"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = "secret"
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-state"
"#,
        )
        .expect("write config");

        let err = load(&config_path).expect_err("expected invalid api version error");
        assert!(err.to_string().contains("v9"));
    }

    #[test]
    fn rejects_non_loopback_admin_without_tls() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config_path: PathBuf = dir.path().join("cdc.toml");

        std::fs::write(
            &config_path,
            r#"
api_version = "v1alpha1"
delivery_contract = "at_least_once"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = "secret"
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-state"

[admin]
bind = "0.0.0.0:8080"
read_token_env = "RUSTCDC_ADMIN_READ_TOKEN"
write_token_env = "RUSTCDC_ADMIN_WRITE_TOKEN"
notification_log_file = "/tmp/cdc-test-notifications.jsonl"
"#,
        )
        .expect("write config");

        let err = load(&config_path).expect_err("expected non-loopback TLS requirement failure");
        assert!(err.to_string().contains("admin.tls is required"));
    }

    #[test]
    fn rejects_empty_admin_audit_log_file_when_configured() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config_path: PathBuf = dir.path().join("cdc.toml");

        std::fs::write(
            &config_path,
            r#"
api_version = "v1alpha1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = "secret"
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-state"

[admin]
audit_log_file = ""
"#,
        )
        .expect("write config");

        let err = load(&config_path).expect_err("expected empty audit log file rejection");
        assert!(
            err.to_string()
                .contains("admin.audit_log_file must not be empty when configured"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn rejects_admin_audit_log_file_when_parent_directory_is_missing() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config_path: PathBuf = dir.path().join("cdc.toml");
        let missing_parent = dir.path().join("missing");
        let audit_log_file = missing_parent.join("admin-audit.jsonl");

        std::fs::write(
            &config_path,
            format!(
                r#"
api_version = "v1alpha1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = "secret"
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-state"

[admin]
audit_log_file = "{}"
"#,
                audit_log_file.display()
            ),
        )
        .expect("write config");

        let err = load(&config_path).expect_err("expected missing audit log parent rejection");
        assert!(
            err.to_string()
                .contains("admin.audit_log_file parent directory does not exist"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn accepts_admin_audit_log_file_when_parent_directory_exists() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config_path: PathBuf = dir.path().join("cdc.toml");
        let log_dir = dir.path().join("logs");
        let audit_log_file = log_dir.join("admin-audit.jsonl");
        std::fs::create_dir_all(&log_dir).expect("create log directory");

        std::fs::write(
            &config_path,
            format!(
                r#"
api_version = "v1alpha1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = "secret"
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-state"

[admin]
audit_log_file = "{}"
"#,
                audit_log_file.display()
            ),
        )
        .expect("write config");

        load(&config_path).expect("expected valid audit log file config");
    }

    #[test]
    fn rejects_empty_admin_notification_log_file_when_configured() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config_path: PathBuf = dir.path().join("cdc.toml");

        std::fs::write(
            &config_path,
            r#"
api_version = "v1alpha1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = "secret"
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-state"

[admin]
notification_log_file = ""
"#,
        )
        .expect("write config");

        let err = load(&config_path).expect_err("expected empty notification log file rejection");
        assert!(
            err.to_string()
                .contains("admin.notification_log_file must not be empty when configured"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn rejects_admin_notification_log_file_when_parent_directory_is_missing() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config_path: PathBuf = dir.path().join("cdc.toml");
        let missing_parent = dir.path().join("missing");
        let notification_log_file = missing_parent.join("admin-notifications.jsonl");

        std::fs::write(
            &config_path,
            format!(
                r#"
api_version = "v1alpha1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = "secret"
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-state"

[admin]
notification_log_file = "{}"
"#,
                notification_log_file.display()
            ),
        )
        .expect("write config");

        let err =
            load(&config_path).expect_err("expected missing notification log parent rejection");
        assert!(
            err.to_string()
                .contains("admin.notification_log_file parent directory does not exist"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn accepts_admin_notification_log_file_when_parent_directory_exists() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config_path: PathBuf = dir.path().join("cdc.toml");
        let log_dir = dir.path().join("logs");
        let notification_log_file = log_dir.join("admin-notifications.jsonl");
        std::fs::create_dir_all(&log_dir).expect("create log directory");

        std::fs::write(
            &config_path,
            format!(
                r#"
api_version = "v1alpha1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = "secret"
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-state"

[admin]
notification_log_file = "{}"
"#,
                notification_log_file.display()
            ),
        )
        .expect("write config");

        load(&config_path).expect("expected valid notification log file config");
    }

    #[test]
    fn rejects_admin_signal_ingress_file_when_parent_directory_is_missing() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config_path: PathBuf = dir.path().join("cdc.toml");
        let missing_parent = dir.path().join("missing");
        let signal_ingress_file = missing_parent.join("admin-signal-ingress.jsonl");

        std::fs::write(
            &config_path,
            format!(
                r#"
api_version = "v1alpha1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = "secret"
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-state"

[admin]
signal_ingress_file = "{}"
"#,
                signal_ingress_file.display()
            ),
        )
        .expect("write config");

        let err = load(&config_path)
            .expect_err("expected missing signal ingress parent directory rejection");
        assert!(
            err.to_string()
                .contains("admin.signal_ingress_file parent directory does not exist"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn accepts_admin_signal_ingress_file_when_parent_directory_exists() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config_path: PathBuf = dir.path().join("cdc.toml");
        let ingress_dir = dir.path().join("ingress");
        let signal_ingress_file = ingress_dir.join("admin-signal-ingress.jsonl");
        std::fs::create_dir_all(&ingress_dir).expect("create ingress directory");

        std::fs::write(
            &config_path,
            format!(
                r#"
api_version = "v1alpha1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = "secret"
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-state"

[admin]
signal_ingress_file = "{}"
"#,
                signal_ingress_file.display()
            ),
        )
        .expect("write config");

        load(&config_path).expect("expected valid signal ingress file config");
    }

    #[test]
    fn rejects_admin_notification_kafka_with_empty_brokers() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config_path: PathBuf = dir.path().join("cdc.toml");

        std::fs::write(
            &config_path,
            r#"
api_version = "v1alpha1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = "secret"
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-state"

[admin.notification_kafka]
brokers = ""
topic = "cdc-admin-notifications"
"#,
        )
        .expect("write config");

        let err = load(&config_path).expect_err("expected invalid admin notification kafka config");
        assert!(
            err.to_string()
                .contains("admin.notification_kafka.brokers must not be empty"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn accepts_admin_notification_kafka_with_valid_plaintext_config() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config_path: PathBuf = dir.path().join("cdc.toml");

        std::fs::write(
            &config_path,
            r#"
api_version = "v1alpha1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = "secret"
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-state"

[admin.notification_kafka]
brokers = "kafka-1:9092"
topic = "cdc-admin-notifications"
client_id = "cdc-admin-test"
"#,
        )
        .expect("write config");

        load(&config_path).expect("expected valid admin notification kafka config");
    }

    #[test]
    fn rejects_admin_signal_ingress_kafka_with_empty_brokers() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config_path: PathBuf = dir.path().join("cdc.toml");

        std::fs::write(
            &config_path,
            r#"
api_version = "v1alpha1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = "secret"
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-state"

[admin.signal_ingress_kafka]
brokers = ""
topic = "cdc-admin-signals"
group_id = "cdc-admin-signals-group"
"#,
        )
        .expect("write config");

        let err =
            load(&config_path).expect_err("expected invalid admin signal ingress kafka config");
        assert!(
            err.to_string()
                .contains("admin.signal_ingress_kafka.brokers must not be empty"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn accepts_admin_signal_ingress_kafka_with_valid_plaintext_config() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config_path: PathBuf = dir.path().join("cdc.toml");

        std::fs::write(
            &config_path,
            r#"
api_version = "v1alpha1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = "secret"
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-state"

[admin.signal_ingress_kafka]
brokers = "localhost:9092"
topic = "cdc-admin-signals"
group_id = "cdc-admin-signals-group"
"#,
        )
        .expect("write config");

        load(&config_path).expect("expected valid admin signal ingress kafka config");
    }

    #[test]
    fn rejects_write_capable_admin_without_non_admin_notification_channels() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config_path: PathBuf = dir.path().join("cdc.toml");

        std::fs::write(
            &config_path,
            r#"
api_version = "v1alpha1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = "secret"
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-state"

[admin]
write_token_env = "RUSTCDC_ADMIN_WRITE_TOKEN"
"#,
        )
        .expect("write config");

        let err = load(&config_path)
            .expect_err("expected rejection for write-capable admin without notification channels");
        assert!(
            err.to_string().contains(
                "admin.notification_log_file or admin.notification_kafka is required when write-capable admin signaling is enabled"
            ),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn accepts_write_capable_admin_with_non_admin_notification_log_channel() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config_path: PathBuf = dir.path().join("cdc.toml");
        let log_dir = dir.path().join("logs");
        let notification_log_file = log_dir.join("admin-notifications.jsonl");
        std::fs::create_dir_all(&log_dir).expect("create log directory");

        std::fs::write(
            &config_path,
            format!(
                r#"
api_version = "v1alpha1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = "secret"
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-state"

[admin]
write_token_env = "RUSTCDC_ADMIN_WRITE_TOKEN"
notification_log_file = "{}"
"#,
                notification_log_file.display()
            ),
        )
        .expect("write config");

        load(&config_path).expect("expected valid write-capable admin with notification channel");
    }

    #[test]
    fn rejects_admin_tls_mtls_without_client_ca_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config_path: PathBuf = dir.path().join("cdc.toml");
        let cert_path = dir.path().join("admin-cert.pem");
        let key_path = dir.path().join("admin-key.pem");

        std::fs::write(&cert_path, "dummy cert").expect("write cert");
        std::fs::write(&key_path, "dummy key").expect("write key");

        std::fs::write(
            &config_path,
            format!(
                r#"
api_version = "v1alpha1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = "secret"
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-state"

[admin]
bind = "127.0.0.1:8080"

[admin.tls]
cert_file = "{}"
key_file = "{}"
require_client_cert = true
"#,
                cert_path.display(),
                key_path.display()
            ),
        )
        .expect("write config");

        let err = load(&config_path).expect_err("expected mTLS client CA requirement failure");
        assert!(err
            .to_string()
            .contains("admin.tls.client_ca_file is required"));
    }

    #[test]
    fn rejects_invalid_admin_token_manifest_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config_path: PathBuf = dir.path().join("cdc.toml");
        let manifest_path = dir.path().join("admin-tokens.json");

        let trusted_public_key_hex = write_signed_manifest(
            &manifest_path,
            vec![TokenManifestToken {
                id: "ops".to_string(),
                token_sha256_hex: "bad".to_string(),
                scopes: vec!["read".to_string()],
                not_before: None,
                expires_at: None,
                revoked: false,
            }],
        );

        std::fs::write(
            &config_path,
            format!(
                r#"
api_version = "v1alpha1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = "secret"
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-state"

[admin]
token_manifest_file = "{}"
token_manifest_trusted_public_keys_hex = ["{}"]
token_manifest_max_staleness_ms = 60000
notification_log_file = "/tmp/cdc-test-notifications.jsonl"
"#,
                manifest_path.display(),
                trusted_public_key_hex
            ),
        )
        .expect("write config");

        let err = load(&config_path).expect_err("expected invalid manifest hash failure");
        assert!(err.to_string().contains("token_sha256_hex"));
    }

    #[test]
    fn rejects_empty_admin_token_manifest_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config_path: PathBuf = dir.path().join("cdc.toml");
        let manifest_path = dir.path().join("admin-tokens.json");

        let trusted_public_key_hex = write_signed_manifest(&manifest_path, vec![]);

        std::fs::write(
            &config_path,
            format!(
                r#"
api_version = "v1alpha1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = "secret"
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-state"

[admin]
token_manifest_file = "{}"
token_manifest_trusted_public_keys_hex = ["{}"]
token_manifest_max_staleness_ms = 60000
notification_log_file = "/tmp/cdc-test-notifications.jsonl"
"#,
                manifest_path.display(),
                trusted_public_key_hex
            ),
        )
        .expect("write config");

        let err = load(&config_path).expect_err("expected empty manifest rejection");
        assert!(err
            .to_string()
            .contains("must contain at least one token entry"));
    }

    #[test]
    fn rejects_admin_token_manifest_without_trusted_public_keys() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config_path: PathBuf = dir.path().join("cdc.toml");
        let manifest_path = dir.path().join("admin-tokens.json");

        write_signed_manifest(
            &manifest_path,
            vec![TokenManifestToken {
                id: "ops".to_string(),
                token_sha256_hex:
                    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
                scopes: vec!["read".to_string()],
                not_before: None,
                expires_at: None,
                revoked: false,
            }],
        );

        std::fs::write(
            &config_path,
            format!(
                r#"
api_version = "v1alpha1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = "secret"
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-state"

[admin]
token_manifest_file = "{}"
token_manifest_max_staleness_ms = 60000
notification_log_file = "/tmp/cdc-test-notifications.jsonl"
"#,
                manifest_path.display()
            ),
        )
        .expect("write config");

        let err = load(&config_path).expect_err("expected trusted key requirement failure");
        assert!(err
            .to_string()
            .contains("token_manifest_trusted_public_keys_hex"));
    }

    #[test]
    fn rejects_non_loopback_admin_without_write_token_env() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config_path: PathBuf = dir.path().join("cdc.toml");
        let cert_path = dir.path().join("admin-cert.pem");
        let key_path = dir.path().join("admin-key.pem");

        std::fs::write(&cert_path, "dummy cert").expect("write cert");
        std::fs::write(&key_path, "dummy key").expect("write key");

        std::fs::write(
            &config_path,
            format!(
                r#"
api_version = "v1alpha1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = "secret"
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-state"

[admin]
bind = "0.0.0.0:8080"
read_token_env = "RUSTCDC_ADMIN_READ_TOKEN"

[admin.tls]
cert_file = "{}"
key_file = "{}"
"#,
                cert_path.display(),
                key_path.display()
            ),
        )
        .expect("write config");

        let err = load(&config_path).expect_err("expected write token requirement failure");
        assert!(err.to_string().contains("admin.write_token_env"));
    }

    #[test]
    fn accepts_non_loopback_admin_with_token_manifest_and_tls() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config_path: PathBuf = dir.path().join("cdc.toml");
        let manifest_path = dir.path().join("admin-tokens.json");
        let cert_path = dir.path().join("admin-cert.pem");
        let key_path = dir.path().join("admin-key.pem");

        let trusted_public_key_hex = write_signed_manifest(
            &manifest_path,
            vec![TokenManifestToken {
                id: "ops-read".to_string(),
                token_sha256_hex:
                    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
                scopes: vec!["read".to_string(), "write".to_string()],
                not_before: None,
                expires_at: Some(
                    chrono::DateTime::parse_from_rfc3339("2099-01-01T00:00:00Z")
                        .expect("parse expires_at")
                        .with_timezone(&chrono::Utc),
                ),
                revoked: false,
            }],
        );
        std::fs::write(&cert_path, "dummy cert").expect("write cert");
        std::fs::write(&key_path, "dummy key").expect("write key");

        std::fs::write(
            &config_path,
            format!(
                r#"
api_version = "v1alpha1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = "secret"
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-state"

[admin]
bind = "0.0.0.0:8080"
token_manifest_file = "{}"
token_manifest_trusted_public_keys_hex = ["{}"]
token_manifest_max_staleness_ms = 60000
notification_log_file = "/tmp/cdc-test-notifications.jsonl"

[admin.tls]
cert_file = "{}"
key_file = "{}"
"#,
                manifest_path.display(),
                trusted_public_key_hex,
                cert_path.display(),
                key_path.display()
            ),
        )
        .expect("write config");

        let cfg = load(&config_path).expect("expected config to load");
        assert_eq!(cfg.admin.bind, "0.0.0.0:8080");
        assert_eq!(cfg.admin.token_manifest_file, Some(manifest_path));
    }

    #[test]
    fn rejects_zero_admin_token_manifest_refresh_interval() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config_path: PathBuf = dir.path().join("cdc.toml");

        std::fs::write(
            &config_path,
            r#"
api_version = "v1alpha1"
delivery_contract = "effectively_once"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = "secret"
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-state"

[admin]
token_manifest_refresh_ms = 0
"#,
        )
        .expect("write config");

        let err = load(&config_path).expect_err("expected invalid manifest refresh interval");
        assert!(err.to_string().contains("token_manifest_refresh_ms"));
    }

    #[test]
    fn rejects_admin_token_manifest_without_max_staleness_policy() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config_path: PathBuf = dir.path().join("cdc.toml");
        let manifest_path = dir.path().join("admin-tokens.json");

        let trusted_public_key_hex = write_signed_manifest(
            &manifest_path,
            vec![TokenManifestToken {
                id: "ops".to_string(),
                token_sha256_hex:
                    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
                scopes: vec!["read".to_string(), "write".to_string()],
                not_before: None,
                expires_at: None,
                revoked: false,
            }],
        );

        std::fs::write(
            &config_path,
            format!(
                r#"
api_version = "v1alpha1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = "secret"
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-state"

[admin]
token_manifest_file = "{}"
token_manifest_trusted_public_keys_hex = ["{}"]
notification_log_file = "/tmp/cdc-test-notifications.jsonl"
"#,
                manifest_path.display(),
                trusted_public_key_hex
            ),
        )
        .expect("write config");

        let err = load(&config_path)
            .expect_err("expected staleness policy requirement for manifest-backed auth");
        assert!(err
            .to_string()
            .contains("token_manifest_max_staleness_ms is required"));
    }

    #[test]
    fn rejects_admin_manifest_max_staleness_without_manifest_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config_path: PathBuf = dir.path().join("cdc.toml");

        std::fs::write(
            &config_path,
            r#"
api_version = "v1alpha1"
delivery_contract = "effectively_once"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = "secret"
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-state"

[admin]
token_manifest_max_staleness_ms = 1000
"#,
        )
        .expect("write config");

        let err =
            load(&config_path).expect_err("expected staleness policy to require manifest file");
        assert!(err
            .to_string()
            .contains("token_manifest_max_staleness_ms requires admin.token_manifest_file"));
    }

    #[test]
    fn accepts_loopback_admin_with_unauthenticated_probe_mode() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config_path: PathBuf = dir.path().join("cdc.toml");

        std::fs::write(
            &config_path,
            r#"
api_version = "v1alpha1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = "secret"
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-state"

[admin]
probe_auth_mode = "allow_unauthenticated_loopback"
"#,
        )
        .expect("write config");

        let cfg = load(&config_path).expect("expected config to load");
        assert_eq!(cfg.admin.bind, "127.0.0.1:8080");
    }

    #[test]
    fn rejects_non_loopback_admin_with_unauthenticated_probe_mode() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config_path: PathBuf = dir.path().join("cdc.toml");
        let cert_path = dir.path().join("admin-cert.pem");
        let key_path = dir.path().join("admin-key.pem");

        std::fs::write(&cert_path, "dummy cert").expect("write cert");
        std::fs::write(&key_path, "dummy key").expect("write key");

        std::fs::write(
            &config_path,
            format!(
                r#"
api_version = "v1alpha1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = "secret"
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-state"

[admin]
bind = "0.0.0.0:8080"
read_token_env = "RUSTCDC_ADMIN_READ_TOKEN"
write_token_env = "RUSTCDC_ADMIN_WRITE_TOKEN"
probe_auth_mode = "allow_unauthenticated_loopback"

[admin.tls]
cert_file = "{}"
key_file = "{}"
"#,
                cert_path.display(),
                key_path.display()
            ),
        )
        .expect("write config");

        let err =
            load(&config_path).expect_err("expected loopback-only probe auth mode enforcement");
        assert!(err.to_string().contains(
            "admin.probe_auth_mode=allow_unauthenticated_loopback requires loopback admin.bind"
        ));
    }

    #[test]
    fn rejects_invalid_admin_rate_limit_configuration() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config_path: PathBuf = dir.path().join("cdc.toml");

        std::fs::write(
            &config_path,
            r#"
api_version = "v1alpha1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = "secret"
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-state"

[admin]
metrics_rate_limit_rps = 10
metrics_rate_limit_burst = 5
"#,
        )
        .expect("write config");

        let err = load(&config_path).expect_err("expected admin rate-limit validation failure");
        assert!(err
            .to_string()
            .contains("admin.metrics_rate_limit_burst must be >= admin.metrics_rate_limit_rps"));
    }

    #[test]
    fn rejects_invalid_admin_trusted_proxy_ip() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config_path: PathBuf = dir.path().join("cdc.toml");

        std::fs::write(
            &config_path,
            r#"
api_version = "v1alpha1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = "secret"
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-state"

[admin]
trusted_proxy_ips = ["not-an-ip"]
"#,
        )
        .expect("write config");

        let err = load(&config_path).expect_err("expected invalid trusted proxy ip");
        assert!(err.to_string().contains("admin.trusted_proxy_ips"));
    }

    #[test]
    fn rejects_http_sink_with_empty_url() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config_path: PathBuf = dir.path().join("cdc.toml");

        std::fs::write(
            &config_path,
            r#"
api_version = "v1alpha1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = "secret"
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "http"
url = ""

[state]
dir = "/tmp/cdc-state"
"#,
        )
        .expect("write config");

        let err = load(&config_path).expect_err("expected invalid http sink URL");
        assert!(err.to_string().contains("sink.http.url"));
    }

    #[test]
    fn rejects_http_sink_with_invalid_backoff_range() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config_path: PathBuf = dir.path().join("cdc.toml");

        std::fs::write(
            &config_path,
            r#"
api_version = "v1alpha1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = "secret"
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "http"
url = "http://localhost:8080/events"
backoff_initial_ms = 5000
backoff_max_ms = 100

[state]
dir = "/tmp/cdc-state"
"#,
        )
        .expect("write config");

        let err = load(&config_path).expect_err("expected invalid backoff range");
        assert!(err.to_string().contains("backoff_initial_ms"));
    }

    #[test]
    fn rejects_runtime_zero_max_event_bytes() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config_path: PathBuf = dir.path().join("cdc.toml");

        std::fs::write(
            &config_path,
            r#"
api_version = "v1alpha1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = "secret"
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-state"

[runtime]
max_event_bytes = 0
"#,
        )
        .expect("write config");

        let err = load(&config_path).expect_err("expected invalid runtime max_event_bytes");
        assert!(err.to_string().contains("runtime.max_event_bytes"));
    }

    #[test]
    fn rejects_runtime_flush_interval_above_max_buffer_size() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config_path: PathBuf = dir.path().join("cdc.toml");

        std::fs::write(
            &config_path,
            r#"
api_version = "v1alpha1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = "secret"
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-state"

[runtime]
max_buffer_size = 50
sink_flush_interval_events = 100
"#,
        )
        .expect("write config");

        let err = load(&config_path).expect_err("expected invalid runtime flush interval");
        assert!(err
            .to_string()
            .contains("runtime.sink_flush_interval_events must be <= runtime.max_buffer_size"));
    }

    #[test]
    fn rejects_kafka_sink_with_empty_brokers_or_topic() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config_path: PathBuf = dir.path().join("cdc.toml");

        std::fs::write(
            &config_path,
            r#"
api_version = "v1alpha1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = "secret"
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "kafka"
brokers = " , "
topic = ""

[state]
dir = "/tmp/cdc-state"
"#,
        )
        .expect("write config");

        let err = load(&config_path).expect_err("expected invalid kafka sink config");
        assert!(
            err.to_string().contains("sink.kafka.brokers")
                || err.to_string().contains("sink.kafka.topic")
        );
    }

    #[test]
    fn rejects_kafka_topic_state_backend_with_invalid_thresholds() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config_path: PathBuf = dir.path().join("cdc.toml");

        std::fs::write(
            &config_path,
            r#"
api_version = "v1alpha1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = "secret"
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-state"

[state.backend]
kafka_topic = { brokers = "kafka-1:9092", topic = "cdc-state", min_replication_factor = 1, min_insync_replicas = 2, durability_profile = "development" }
"#,
        )
        .expect("write config");

        let err = load(&config_path).expect_err("expected invalid kafka topic state config");
        assert!(err.to_string().contains("min_insync_replicas"));
    }

    #[test]
    fn rejects_avro_sink_with_empty_path() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config_path: PathBuf = dir.path().join("cdc.toml");

        std::fs::write(
            &config_path,
            r#"
api_version = "v1alpha1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = "secret"
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "avro"
path = ""

[state]
dir = "/tmp/cdc-state"
"#,
        )
        .expect("write config");

        let err = load(&config_path).expect_err("expected invalid avro sink config");
        assert!(
            err.to_string().contains("avro") && err.to_string().contains("no longer supported"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn rejects_wasm_runtime_without_module_path() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config_path: PathBuf = dir.path().join("cdc.toml");

        std::fs::write(
            &config_path,
            r#"
api_version = "v1alpha1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = "secret"
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-state"

[transform_runtime]
mode = "wasm"
"#,
        )
        .expect("write config");

        let err = load(&config_path).expect_err("expected invalid wasm runtime config");
        assert!(err
            .to_string()
            .contains("transform_runtime.wasm.module_path"));
    }

    #[test]
    fn rejects_transform_rule_with_empty_actions() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config_path: PathBuf = dir.path().join("cdc.toml");

        std::fs::write(
            &config_path,
            r#"
api_version = "v1alpha1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = "secret"
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-state"

[[transforms]]
name = "bad"
"#,
        )
        .expect("write config");

        let err = load(&config_path).expect_err("expected invalid transform config");
        assert!(err.to_string().contains("at least one action"));
    }

    #[test]
    fn rejects_kafka_tls_with_verify_peer_disabled_and_missing_ca_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config_path: PathBuf = dir.path().join("cdc.toml");
        let missing_ca = dir.path().join("missing-ca.pem");

        std::fs::write(
            &config_path,
            format!(
                r#"
api_version = "v1alpha1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = "secret"
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "kafka"
brokers = "localhost:9092"
topic = "cdc-events"

[sink.security]
protocol = "tls"
verify_peer = false
ssl_ca_location = "{}"

[state]
dir = "/tmp/cdc-state"
"#,
                missing_ca.display()
            ),
        )
        .expect("write config");

        let err = load(&config_path).expect_err("expected invalid kafka tls config");
        let message = err.to_string();
        assert!(
            message.contains("verify_peer") || message.contains("ssl_ca_location"),
            "unexpected error: {message}"
        );
    }

    #[test]
    fn accepts_transactional_kafka_sink_mode() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config_path: PathBuf = dir.path().join("cdc.toml");

        std::fs::write(
            &config_path,
            r#"
api_version = "v1alpha1"
delivery_contract = "effectively_once"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = "secret"
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "kafka"
brokers = "localhost:9092"
topic = "cdc-events"
delivery_mode = "transactional"
transactional_id = "cdc-eos-1"

[state]
dir = "/tmp/cdc-state"
backend = "local_fs"
"#,
        )
        .expect("write config");

        load(&config_path).expect("transactional kafka sink config should load");
    }

    #[test]
    fn rejects_http_sink_with_verify_tls_disabled() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config_path: PathBuf = dir.path().join("cdc.toml");

        std::fs::write(
            &config_path,
            r#"
api_version = "v1alpha1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = "secret"
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "http"
url = "http://127.0.0.1:8081/events"
verify_tls = false

[state]
dir = "/tmp/cdc-state"
"#,
        )
        .expect("write config");

        let err = load(&config_path).expect_err("expected invalid insecure http sink config");
        assert!(err
            .to_string()
            .contains("sink.http.verify_tls must be true"));
    }

    #[test]
    fn rejects_http_sink_with_non_https_non_loopback_url() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config_path: PathBuf = dir.path().join("cdc.toml");

        std::fs::write(
            &config_path,
            r#"
api_version = "v1alpha1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = "secret"
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "http"
url = "http://example.com/events"
verify_tls = true

[state]
dir = "/tmp/cdc-state"
"#,
        )
        .expect("write config");

        let err = load(&config_path).expect_err("expected invalid non-https http sink url");
        assert!(err
            .to_string()
            .contains("sink.http.url must use https except for localhost loopback testing"));
    }

    #[test]
    fn rejects_http_sink_with_inline_bearer_secret_literal() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config_path: PathBuf = dir.path().join("cdc.toml");

        std::fs::write(
            &config_path,
            r#"
api_version = "v1alpha1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = "secret"
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "http"
url = "http://127.0.0.1:8080/events"
bearer_token = "top-secret-token"

[state]
dir = "/tmp/cdc-state"
"#,
        )
        .expect("write config");

        let err =
            load(&config_path).expect_err("expected inline bearer token literal to be rejected");
        assert!(err
            .to_string()
            .contains("sink.http.bearer_token must use deferred secret references"));
    }

    /// `{ env = "VAR" }` references resolve to the environment value for every
    /// secret-bearing field — the pattern the docs and `rustcdc init` templates
    /// use must actually load.
    #[test]
    fn resolves_env_secret_references_at_load_time() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config_path: PathBuf = dir.path().join("cdc.toml");

        // Unique names to avoid collisions with parallel tests.
        std::env::set_var("CDC_LOADER_TEST_PG_PASSWORD", "pg-secret-from-env");
        std::env::set_var("CDC_LOADER_TEST_BEARER", "bearer-from-env");

        std::fs::write(
            &config_path,
            r#"
api_version = "v1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = { env = "CDC_LOADER_TEST_PG_PASSWORD" }
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "http"
url = "http://127.0.0.1:8080/events"
bearer_token = { env = "CDC_LOADER_TEST_BEARER" }

[state]
dir = "/tmp/cdc-state"
"#,
        )
        .expect("write config");

        let config = load(&config_path).expect("env-referenced secrets must load");

        let crate::config::schema::SourceDriver::Postgres(pg) = &config.source.driver else {
            panic!("expected postgres source");
        };
        assert_eq!(
            pg.password.resolve().expect("resolve password"),
            "pg-secret-from-env"
        );
        let crate::config::schema::SinkConfig::Http(http) = &config.sink else {
            panic!("expected http sink");
        };
        assert_eq!(
            http.bearer_token
                .as_ref()
                .expect("bearer token present")
                .resolve()
                .expect("resolve bearer token"),
            "bearer-from-env"
        );
    }

    /// An unset env reference must fail loudly, naming the variable and the
    /// config path — not default to an empty credential.
    #[test]
    fn rejects_unset_env_secret_reference() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config_path: PathBuf = dir.path().join("cdc.toml");

        std::fs::write(
            &config_path,
            r#"
api_version = "v1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = { env = "CDC_LOADER_TEST_DEFINITELY_UNSET_VAR" }
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-state"
"#,
        )
        .expect("write config");

        let err = load(&config_path).expect_err("unset env reference must be rejected");
        let message = err.to_string();
        assert!(
            message.contains("CDC_LOADER_TEST_DEFINITELY_UNSET_VAR"),
            "error must name the variable: {message}"
        );
        assert!(
            message.contains("source.password"),
            "error must name the config path: {message}"
        );
    }

    #[test]
    fn rejects_iceberg_sink_with_inline_catalog_token_literal() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config_path: PathBuf = dir.path().join("cdc.toml");

        std::fs::write(
            &config_path,
            r#"
api_version = "v1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = "secret"
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "iceberg"
table_path = "/tmp/iceberg-table"
namespace = "cdc"
table_name = "events"

[sink.catalog.rest]
uri = "http://127.0.0.1:8181"
warehouse = "file:///tmp/iceberg-warehouse"
token = "inline-token-literal"

[state]
dir = "/tmp/cdc-state"
"#,
        )
        .expect("write config");

        let err =
            load(&config_path).expect_err("expected inline iceberg token literal to be rejected");
        assert!(err
            .to_string()
            .contains("sink.iceberg.catalog.rest.token must use deferred secret references"));
    }

    #[test]
    fn rejects_http_sink_with_zero_dlq_max_bytes() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config_path: PathBuf = dir.path().join("cdc.toml");
        let dlq_path = dir.path().join("http-dlq.jsonl");

        std::fs::write(
            &config_path,
            format!(
                r#"
api_version = "v1alpha1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = "secret"
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "http"
url = "http://127.0.0.1:8080/events"
verify_tls = true
dlq_path = "{}"
dlq_max_bytes = 0

[state]
dir = "/tmp/cdc-state"
"#,
                dlq_path.display()
            ),
        )
        .expect("write config");

        let err = load(&config_path).expect_err("expected dlq_max_bytes validation failure");
        assert!(
            err.to_string()
                .contains("sink.http.dlq_max_bytes must be > 0"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn rejects_http_sink_dlq_path_with_missing_parent_directory() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config_path: PathBuf = dir.path().join("cdc.toml");
        let dlq_path = dir.path().join("missing").join("http-dlq.jsonl");

        std::fs::write(
            &config_path,
            format!(
                r#"
api_version = "v1alpha1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = "secret"
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "http"
url = "http://127.0.0.1:8080/events"
verify_tls = true
dlq_path = "{}"

[state]
dir = "/tmp/cdc-state"
"#,
                dlq_path.display()
            ),
        )
        .expect("write config");

        let err = load(&config_path).expect_err("expected dlq path parent validation failure");
        assert!(
            err.to_string()
                .contains("sink.http.dlq_path parent directory does not exist"),
            "unexpected error: {err}"
        );
    }
}
