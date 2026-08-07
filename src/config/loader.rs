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
    let file_only: serde_json::Value = Figment::new()
        .merge(Toml::file(config_path))
        .extract()
        .map_err(|e| ConfigError::Load(Box::new(e)))?;

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

    reject_removed_delivery_contracts(&migrated_raw)?;
    reject_relocated_http_dlq(&migrated_raw)?;

    let mut config: AppConfig = serde_json::from_value(migrated_raw).map_err(|e| {
        ConfigError::InvalidState(format!("failed to deserialize migrated configuration: {e}"))
    })?;

    // Typo detection runs against the *file*, never the env-merged document. The
    // `RUSTCDC_` prefix is a shared namespace: `RUSTCDC_ADMIN_READ_TOKEN` and
    // `RUSTCDC_LOG_LEVEL` are read by name elsewhere and are not config keys at all,
    // so diffing the merged document would reject the project's own documented
    // environment variables. A key an operator exports deliberately is also a much
    // weaker typo signal than one they wrote into a config file.
    if let Ok(mut file_raw) =
        super::migrations::load_and_migrate_value(file_only, AppConfig::SUPPORTED_API_VERSION)
    {
        // Errors here were already reported by the merged pass above; a file that only
        // parses with the env overlay applied simply skips the check.
        if resolve_env_secret_references(&mut file_raw, "").is_ok() {
            reject_unknown_config_keys(&file_raw, &config)?;
        }
    }

    resolve_registry_refs(&mut config)?;
    validate(&config)?;
    Ok(config)
}

/// Reject configuration keys the schema does not recognise.
///
/// `#[serde(deny_unknown_fields)]` cannot be used here: the config leans on
/// `#[serde(flatten)]` for `SourceConfig`, `NamedSinkConfig` and `RegistryBinding`, and
/// the two attributes are mutually exclusive — serde cannot know which flattened target
/// a key belongs to, so it accepts everything.
///
/// So the check runs after parsing instead: re-serialise the parsed `AppConfig` and diff
/// its key paths against the raw document. Anything present in the input and absent from
/// the round trip was silently dropped.
///
/// This matters more than typo ergonomics. A misspelled `table_include_list` does not
/// fail — it captures **every table in the database**, which is a data-exposure change
/// the operator never asked for. Verified against the real defect this found: a
/// `snapshot_tables` key indented under `[state]` parsed cleanly and did nothing.
fn reject_unknown_config_keys(
    raw: &serde_json::Value,
    parsed: &AppConfig,
) -> Result<(), ConfigError> {
    let round_tripped = serde_json::to_value(parsed).map_err(|e| {
        ConfigError::InvalidState(format!("failed to re-serialise configuration: {e}"))
    })?;

    let mut unknown = Vec::new();
    collect_unknown_keys(raw, &round_tripped, String::new(), &mut unknown);

    if unknown.is_empty() {
        return Ok(());
    }
    unknown.sort();
    Err(ConfigError::InvalidState(format!(
        "unrecognised configuration key(s): {}. A key the schema does not know is \
         silently ignored, so a typo does not disable a setting — it leaves the default \
         in place. Check the spelling and the table it sits under against \
         https://hupe1980.github.io/rustcdc-server/docs/configuration/.",
        unknown.join(", ")
    )))
}

/// Walk `raw` against the round-tripped document, recording paths that only exist in
/// `raw`.
///
/// Arrays are compared element-wise by index; a length difference is not itself an
/// error, because `skip_serializing_if` can legitimately shorten the output.
fn collect_unknown_keys(
    raw: &serde_json::Value,
    known: &serde_json::Value,
    path: String,
    unknown: &mut Vec<String>,
) {
    match (raw, known) {
        (serde_json::Value::Object(raw_map), serde_json::Value::Object(known_map)) => {
            for (key, raw_child) in raw_map {
                let child_path = if path.is_empty() {
                    key.clone()
                } else {
                    format!("{path}.{key}")
                };
                match known_map.get(key) {
                    Some(known_child) => {
                        collect_unknown_keys(raw_child, known_child, child_path, unknown)
                    }
                    // A secret that resolved to a plain string, or a field the parsed
                    // form represents differently, still round-trips under its own key —
                    // so a genuinely missing key is a key the schema never had.
                    None => unknown.push(child_path),
                }
            }
        }
        (serde_json::Value::Array(raw_items), serde_json::Value::Array(known_items)) => {
            for (index, raw_item) in raw_items.iter().enumerate() {
                if let Some(known_item) = known_items.get(index) {
                    collect_unknown_keys(raw_item, known_item, format!("{path}[{index}]"), unknown);
                }
            }
        }
        _ => {}
    }
}

/// Reject `delivery_contract` values that were removed, naming the replacement.
///
/// Serde would otherwise report `unknown variant "at_most_once"` and list the valid
/// ones, which tells an operator *what* is wrong but not *why* it went away or what to
/// do — and the why matters here, because `at_most_once` used to be accepted and
/// silently behaved as `at_least_once`.
/// The HTTP sink's private dead-letter queue moved to the top-level `[dlq]` section.
///
/// Quarantining is a pipeline concern: every sink needs it, and whether an event is
/// permanently undeliverable is decided by the error classification rather than by the
/// transport. Leaving the old key to be silently ignored would have been the worst
/// outcome — an operator who configured a DLQ would get none, and would not find out
/// until the first poison event took the pipeline down.
fn reject_relocated_http_dlq(raw: &serde_json::Value) -> Result<(), ConfigError> {
    let uses_old_key = |sink: &serde_json::Value| -> bool {
        sink.get("dlq_path").is_some() || sink.get("dlq_max_bytes").is_some()
    };

    let mut offenders = Vec::new();
    if raw.get("sink").is_some_and(uses_old_key) {
        offenders.push("sink".to_string());
    }
    if let Some(sinks) = raw.get("sinks").and_then(serde_json::Value::as_array) {
        for (index, named) in sinks.iter().enumerate() {
            if named.get("sink").is_some_and(uses_old_key) {
                offenders.push(format!("sinks[{index}].sink"));
            }
        }
    }

    if offenders.is_empty() {
        return Ok(());
    }

    Err(ConfigError::InvalidState(format!(
        "`dlq_path` / `dlq_max_bytes` under {} were removed. The dead-letter queue is \
         now a top-level `[dlq]` section that applies to every sink and can target a \
         file or a Kafka topic:\n\n\
         \x20 [dlq]\n\
         \x20 enabled   = true\n\
         \x20 type      = \"file\"            # or \"kafka\"\n\
         \x20 path      = \"/var/lib/rustcdc/dlq.jsonl\"\n\
         \x20 max_bytes = 134217728\n\n\
         Note that it is now opt-in: quarantining advances the checkpoint past an event \
         that was never delivered, which is data loss — recorded rather than silent, but \
         loss. Without `[dlq]` the pipeline halts on a permanently undeliverable event.",
        offenders.join(", ")
    )))
}

fn reject_removed_delivery_contracts(raw: &serde_json::Value) -> Result<(), ConfigError> {
    if raw
        .get("delivery_contract")
        .and_then(serde_json::Value::as_str)
        == Some("at_most_once")
    {
        return Err(ConfigError::InvalidState(
            "delivery_contract = \"at_most_once\" was removed. It was accepted but never \
             implemented — the checkpoint still advanced after delivery, so deployments \
             that selected it silently received at_least_once. It is not being fixed \
             because delivery is batched: advancing the checkpoint before a batch skips \
             an arbitrary suffix of that batch on failure, not a single event, so the \
             loss boundary is unpredictable. Use delivery_contract = \"at_least_once\" \
             and deduplicate in the sink on a key you control."
                .to_string(),
        ));
    }
    Ok(())
}

/// Inline every `registry_ref = "<name>"` from the shared `[registries.<name>]` pool.
///
/// The pool existed in the schema and was documented, but nothing read it — a codec
/// could only carry its registry inline, so three sinks against one registry meant
/// three copies of the URL and credentials, and the copies are what drift apart.
/// Resolving here means everything downstream sees one shape.
fn resolve_registry_refs(config: &mut AppConfig) -> Result<(), ConfigError> {
    let pool = config.registries.clone();

    for (name, registry) in &pool {
        registry
            .validate()
            .map_err(|e| ConfigError::InvalidState(format!("registries.{name}: {e}")))?;
    }

    let mut sinks: Vec<(String, &mut SinkConfig)> = vec![("sink".to_string(), &mut config.sink)];
    for named in &mut config.sinks {
        sinks.push((format!("sinks.{}", named.name), &mut named.sink));
    }

    for (path, sink) in sinks {
        resolve_sink_registry_refs(&path, sink, &pool)?;
    }
    Ok(())
}

fn resolve_sink_registry_refs(
    path: &str,
    sink: &mut SinkConfig,
    pool: &std::collections::BTreeMap<String, super::registry::ConfluentRegistryConfig>,
) -> Result<(), ConfigError> {
    if let SinkConfig::Fan(fan) = sink {
        for (i, child) in fan.sinks.iter_mut().enumerate() {
            resolve_sink_registry_refs(&format!("{path}.sinks[{i}]"), child, pool)?;
        }
        return Ok(());
    }

    let codec = match sink {
        SinkConfig::Kafka(kafka) => kafka.codec.as_mut(),
        SinkConfig::Http(http) => http.codec.as_mut(),
        _ => None,
    };
    let Some(binding) = codec.and_then(|codec| codec.binding_mut()) else {
        return Ok(());
    };
    let Some(name) = binding.registry_ref.clone() else {
        return Ok(());
    };
    let resolved = pool.get(&name).ok_or_else(|| {
        let known = if pool.is_empty() {
            "no [registries.*] entries are defined".to_string()
        } else {
            format!("known: {}", pool.keys().cloned().collect::<Vec<_>>().join(", "))
        };
        ConfigError::InvalidState(format!(
            "{path}.codec.registry_ref = \"{name}\" does not match any [registries.*] entry ({known})"
        ))
    })?;
    binding.registry = Some(resolved.clone());
    // Clear the reference now that it is resolved, so the binding has exactly one
    // canonical shape downstream. Leaving both set would trip the "declares both an
    // inline table and registry_ref" check that runs right after this pass.
    binding.registry_ref = None;
    Ok(())
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
    // The source password is the credential with the widest blast radius in the file —
    // it grants replication-level read on the whole database. Sink tokens, Iceberg
    // credentials and masking keys already had to be deferred references; the source
    // password did not, which is the wrong way round.
    if let Some(source) = raw.get("source") {
        // The driver table is either flattened into `[source]` or nested under
        // `[source.<driver>]`; check both shapes so neither escapes the rule.
        let candidates = std::iter::once(source).chain(
            source
                .as_object()
                .into_iter()
                .flat_map(|obj| obj.values())
                .filter(|value| value.is_object()),
        );
        for candidate in candidates {
            if candidate
                .get("password")
                .is_some_and(serde_json::Value::is_string)
            {
                return Err(ConfigError::InvalidState(
                    "source password must use a deferred secret reference (for example \
                     { env = \"POSTGRES_PASSWORD\" }). A replication credential written \
                     as a literal is readable by anyone who can read the config file, \
                     and it grants read access to every captured table."
                        .to_string(),
                ));
            }
        }
    }

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

    // Masking keys are the credential whose leak is least recoverable: an HMAC key
    // published in the config file makes every pseudonymised value re-identifiable
    // retroactively, and an AES key decrypts everything already written downstream.
    if let Some(rules) = raw
        .get("pipeline")
        .and_then(|p| p.get("transforms"))
        .and_then(serde_json::Value::as_array)
    {
        for rule in rules {
            let Some(actions) = rule.get("actions").and_then(serde_json::Value::as_array) else {
                continue;
            };
            for action in actions {
                if action.get("type").and_then(serde_json::Value::as_str) != Some("mask") {
                    continue;
                }
                let mask_rules = action
                    .get("rules")
                    .and_then(serde_json::Value::as_object)
                    .into_iter()
                    .flatten()
                    .map(|(path, rule)| (path.as_str(), rule))
                    .chain(action.get("default_rule").map(|r| ("", r)));
                for (path, mask_rule) in mask_rules {
                    if mask_rule
                        .get("key")
                        .is_some_and(serde_json::Value::is_string)
                    {
                        let where_ = if path.is_empty() {
                            "default_rule".to_string()
                        } else {
                            format!("rules.\"{path}\"")
                        };
                        return Err(ConfigError::InvalidState(format!(
                            "pipeline.transforms[..].actions[..] mask {where_}.key must use a \
                             deferred secret reference (for example {{ env = \"VAR\" }}); a key \
                             written into the config file makes every value it masked \
                             re-identifiable for as long as that file exists"
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

    // Codec configuration was never validated: a codec naming a plaintext registry URL,
    // or a registry-backed codec with no registry at all, only failed when the sink was
    // built — after the source had already connected.
    validate_sink_codec("sink", &config.sink)?;
    for named in &config.sinks {
        validate_sink_codec(&format!("sinks.{}", named.name), &named.sink)?;
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

    config
        .incremental_snapshot
        .validate()
        .map_err(ConfigError::InvalidState)?;

    // Both bootstrap the same tables by different means. Accepting both would read
    // every listed table twice — once blocking, once through the watermark window —
    // and the duplicate would look like genuine change data downstream.
    if config.incremental_snapshot.is_enabled() && !config.snapshot_tables.is_empty() {
        return Err(ConfigError::InvalidState(
            "snapshot_tables and incremental_snapshot.tables are two bootstrapping paths \
             for the same job; set exactly one. incremental_snapshot does not block the \
             stream and resumes from its persisted chunk cursor after a restart."
                .to_string(),
        ));
    }

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

    if config.delivery_contract == DeliveryContract::EffectivelyOnce {
        validate_effectively_once_state_backend(config)?;
    }

    Ok(())
}

/// `effectively_once` is only exactly-once end to end when the checkpoint is written
/// inside the sink's Kafka transaction, and that is only possible when the checkpoint
/// lives in Kafka on the same cluster.
///
/// Rejecting the alternatives is the point. Any other state backend is a second durability
/// domain, so the batch and the position that describes it commit separately and a crash
/// between them replays the batch — the exact window this contract is chosen to avoid. The
/// combination used to be accepted and the residual window documented; requiring the
/// backend instead turns a caveat an operator has to read into a configuration they cannot
/// express.
fn validate_effectively_once_state_backend(config: &AppConfig) -> Result<(), ConfigError> {
    let SinkConfig::Kafka(sink) = &config.sink else {
        // A non-Kafka sink cannot satisfy the contract at all; the capability check above
        // has already rejected it.
        return Ok(());
    };

    let super::state::StateBackend::KafkaTopic(state) = &config.state.offset.backend else {
        return Err(ConfigError::InvalidState(format!(
            "delivery_contract='effectively_once' requires state.offset.backend=\"kafka_topic\", \
             but it is \"{}\". End-to-end exactly-once needs the checkpoint written inside the \
             sink's Kafka transaction, so the checkpoint has to live in Kafka; any other backend \
             commits the position separately from the data and a crash between the two replays \
             the batch. Either move the checkpoint to a compacted Kafka topic on the same \
             cluster, or set delivery_contract=\"at_least_once\".",
            state_backend_label(&config.state.offset.backend)
        )));
    };

    // A Kafka transaction cannot span two clusters. Compared as normalised sets because
    // broker lists are routinely written in a different order or with different spacing
    // for the same cluster, and rejecting that would be a false alarm.
    let sink_brokers: std::collections::BTreeSet<String> =
        sink.normalized_brokers().into_iter().collect();
    let state_brokers: std::collections::BTreeSet<String> =
        state.normalized_brokers().into_iter().collect();

    if sink_brokers != state_brokers {
        return Err(ConfigError::InvalidState(format!(
            "delivery_contract='effectively_once' requires the sink and the checkpoint topic to \
             be on the same Kafka cluster, because one transaction cannot span two. \
             sink.kafka.brokers is '{}' and state.offset.backend.kafka_topic.brokers is '{}'.",
            sink.brokers, state.brokers
        )));
    }

    Ok(())
}

fn state_backend_label(backend: &super::state::StateBackend) -> &'static str {
    match backend {
        super::state::StateBackend::LocalFs => "local_fs",
        super::state::StateBackend::KafkaTopic(_) => "kafka_topic",
        super::state::StateBackend::Redis(_) => "redis",
        super::state::StateBackend::Postgresql(_) => "postgresql",
    }
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

/// Validate the codec of a sink (and, recursively, of every fan-out child).
fn validate_sink_codec(path: &str, sink: &SinkConfig) -> Result<(), ConfigError> {
    match sink {
        SinkConfig::Fan(fan) => {
            for (i, child) in fan.sinks.iter().enumerate() {
                validate_sink_codec(&format!("{path}.sinks[{i}]"), child)?;
            }
            Ok(())
        }
        _ => {
            let codec = match sink {
                SinkConfig::Kafka(kafka) => kafka.codec.as_ref(),
                SinkConfig::Http(http) => http.codec.as_ref(),
                _ => None,
            };
            match codec {
                Some(codec) => codec
                    .validate()
                    .map_err(|e| ConfigError::InvalidState(format!("{path}.codec: {e}"))),
                None => Ok(()),
            }
        }
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

    // A credential in the URL is readable by anyone holding a *read*-scoped admin
    // token: `/status` returns the config snapshot, and a URL is not a `SecretString`,
    // so no amount of redaction downstream is guaranteed to catch it. Rejecting it here
    // means the credential never enters the process in a form that can leak.
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err(ConfigError::InvalidState(
            "sink.http.url must not embed credentials. A userinfo component travels \
             into every config snapshot, log line and diagnostic that touches the URL, \
             and it is readable by any holder of a read-scoped admin token. Use \
             [sink.http.headers] with an `authorization` entry, or `bearer_token`."
                .to_string(),
        ));
    }

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
#[path = "loader_tests.rs"]
mod tests;
