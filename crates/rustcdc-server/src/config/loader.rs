use figment::{
    Figment,
    providers::{Env, Format, Toml},
};
use std::path::Path;

use super::schema::{
    AdminProbeAuthMode, AppConfig, DeliveryContract, KNOWN_SOURCE_DRIVERS, SinkConfig, StateBackend,
};
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

    reject_uncompiled_source_driver(&migrated_raw)?;
    reject_removed_delivery_contracts(&migrated_raw)?;
    reject_relocated_http_dlq(&migrated_raw)?;

    let mut config: AppConfig = serde_json::from_value(migrated_raw).map_err(|e| {
        ConfigError::Invalid(format!("failed to deserialize migrated configuration: {e}"))
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
    let round_tripped = serde_json::to_value(parsed)
        .map_err(|e| ConfigError::Invalid(format!("failed to re-serialise configuration: {e}")))?;

    let mut unknown = Vec::new();
    collect_unknown_keys(raw, &round_tripped, String::new(), &mut unknown);

    if unknown.is_empty() {
        return Ok(());
    }
    unknown.sort();
    Err(ConfigError::Invalid(format!(
        "unrecognised configuration key(s): {}. A key the schema does not know is \
         silently ignored, so a typo does not disable a setting — it leaves the default \
         in place. Check the spelling and the table it sits under against \
         https://hupe1980.github.io/rustcdc/docs/configuration/.",
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

    Err(ConfigError::Invalid(format!(
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

/// Reject a `[source]` naming a connector this binary was not built with.
///
/// Connectors are cargo features (see the `[features]` block in `Cargo.toml`), so
/// `SourceDriver`'s variants are `#[cfg]`-gated and a config selecting an uncompiled one
/// would otherwise fail as serde's `unknown variant \`sqlserver\`, expected \`postgres\``.
/// That message is accurate and actively misleading: it reads as "there is no such
/// connector" when the truth is "this binary does not have it", and the fix — a rebuild
/// with a feature flag, or the published image, which has them all — is nowhere in it.
///
/// Running *before* deserialization is what makes the message reachable at all. Every
/// known driver is listed in [`KNOWN_SOURCE_DRIVERS`] whether or not it is compiled in,
/// which is also what keeps this honest: a name that is neither known nor compiled falls
/// through to serde, so a genuine typo still gets serde's list of what is valid here.
fn reject_uncompiled_source_driver(raw: &serde_json::Value) -> Result<(), ConfigError> {
    let Some(requested) = raw
        .get("source")
        .and_then(|source| source.get("type"))
        .and_then(serde_json::Value::as_str)
    else {
        return Ok(());
    };

    let Some(entry) = KNOWN_SOURCE_DRIVERS
        .iter()
        .find(|entry| entry.matches(requested))
    else {
        // Not a connector this project has ever had. Let serde report it against the
        // variants that *are* compiled in, which is the more useful list for a typo.
        return Ok(());
    };

    if entry.compiled {
        return Ok(());
    }

    let available: Vec<&str> = KNOWN_SOURCE_DRIVERS
        .iter()
        .filter(|entry| entry.compiled)
        .map(|entry| entry.name)
        .collect();
    let available = if available.is_empty() {
        "none — this binary was built with no source connectors at all".to_string()
    } else {
        available.join(", ")
    };

    Err(ConfigError::InvalidSource(format!(
        "source type \"{requested}\" is a connector this binary was not built with. \
         Connectors are opt-in cargo features so that a deployment does not link TLS \
         stacks and dependencies it never uses. Rebuild with `--features {feature}` \
         (or `--all-features`), or use the published container image, which carries \
         every connector. Compiled in this binary: {available}.",
        feature = entry.feature,
    )))
}

fn reject_removed_delivery_contracts(raw: &serde_json::Value) -> Result<(), ConfigError> {
    if raw
        .get("delivery_contract")
        .and_then(serde_json::Value::as_str)
        == Some("at_most_once")
    {
        return Err(ConfigError::Invalid(
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
            .map_err(|e| ConfigError::Invalid(format!("registries.{name}: {e}")))?;
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
        ConfigError::Invalid(format!(
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
            ConfigError::Invalid(format!(
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

/// Every sink document in the raw config, labelled the way the operator wrote it.
///
/// Three shapes, and the third is the one that was missing: the default `[sink]`, each
/// `[[sinks]]` entry, **and every fan-out child underneath either**. A literal
/// `bearer_token` or webhook signing key inside `[[sink.sinks]]` was accepted, because the
/// scan looked only at the two top-level positions and a fan sink's own `type` is `"fan"`
/// — so none of the per-type checks fired for its children.
///
/// That is the worst position for this particular hole. A leaked signing key does not just
/// expose data; it lets anyone forge events *as this pipeline*, which is the property the
/// signature exists to provide.
///
/// The path is carried rather than reconstructed so the error names the block to edit —
/// `sinks.warehouse.sinks[1].bearer_token`, not "the HTTP sink" when three are configured.
fn sink_documents(raw: &serde_json::Value) -> Vec<(String, &serde_json::Value)> {
    fn walk<'a>(
        path: String,
        sink: &'a serde_json::Value,
        into: &mut Vec<(String, &'a serde_json::Value)>,
    ) {
        if let Some(children) = sink.get("sinks").and_then(serde_json::Value::as_array) {
            for (index, child) in children.iter().enumerate() {
                walk(format!("{path}.sinks[{index}]"), child, into);
            }
        }
        into.push((path, sink));
    }

    let mut found = Vec::new();
    if let Some(sink) = raw.get("sink") {
        walk("sink".to_string(), sink, &mut found);
    }
    if let Some(sinks) = raw.get("sinks").and_then(serde_json::Value::as_array) {
        for (index, named) in sinks.iter().enumerate() {
            let label = named
                .get("name")
                .and_then(serde_json::Value::as_str)
                .map(|name| format!("sinks.{name}"))
                .unwrap_or_else(|| format!("sinks[{index}]"));
            walk(label, named, &mut found);
        }
    }
    found
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
                return Err(ConfigError::Invalid(
                    "source password must use a deferred secret reference (for example \
                     { env = \"POSTGRES_PASSWORD\" }). A replication credential written \
                     as a literal is readable by anyone who can read the config file, \
                     and it grants read access to every captured table."
                        .to_string(),
                ));
            }
        }
    }

    for (path, sink) in sink_documents(raw) {
        let sink_type = sink.get("type").and_then(serde_json::Value::as_str);

        if sink_type == Some("http")
            && let Some(token) = sink.get("bearer_token")
            && token.is_string()
        {
            return Err(ConfigError::Invalid(format!(
                "{path}.bearer_token must use deferred secret references (for example \
                 {{ env = \"VAR\" }})"
            )));
        }

        // A signing key written as a literal is a signing key in the config file, in the
        // `/status` snapshot, and in whatever backs them up. Same rule as `bearer_token`,
        // and it matters more: a leaked signing key lets anyone forge events *as this
        // pipeline*, which is the thing the signature exists to make impossible.
        if sink_type == Some("http")
            && let Some(signing) = sink.get("signing").and_then(serde_json::Value::as_object)
        {
            if signing.get("key").is_some_and(serde_json::Value::is_string) {
                return Err(ConfigError::Invalid(format!(
                    "{path}.signing.key must use deferred secret references (for \
                     example {{ env = \"WEBHOOK_SIGNING_KEY\" }})"
                )));
            }
            if let Some(previous) = signing
                .get("previous_keys")
                .and_then(serde_json::Value::as_array)
                && previous.iter().any(serde_json::Value::is_string)
            {
                return Err(ConfigError::Invalid(format!(
                    "{path}.signing.previous_keys entries must use deferred secret \
                     references (for example {{ env = \"WEBHOOK_SIGNING_KEY_PREVIOUS\" }})"
                )));
            }
        }

        if sink_type == Some("iceberg") {
            for field in ["token", "credential"] {
                let value = sink
                    .get("catalog")
                    .and_then(|c| c.get("rest"))
                    .and_then(|r| r.get(field));
                if let Some(value) = value
                    && value.is_string()
                {
                    return Err(ConfigError::Invalid(format!(
                        "{path}.catalog.rest.{field} must use deferred secret references (for example {{ env = \"VAR\" }})"
                    )));
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
                        return Err(ConfigError::Invalid(format!(
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
        return Err(ConfigError::Invalid(
            "state.offset.dir must not be empty".to_string(),
        ));
    }

    if config.admin.timeout_ms == 0 {
        return Err(ConfigError::Invalid(
            "admin.timeout_ms must be > 0".to_string(),
        ));
    }

    if config.admin.metrics_rate_limit_rps == 0 {
        return Err(ConfigError::Invalid(
            "admin.metrics_rate_limit_rps must be > 0".to_string(),
        ));
    }

    if config.admin.metrics_rate_limit_burst == 0 {
        return Err(ConfigError::Invalid(
            "admin.metrics_rate_limit_burst must be > 0".to_string(),
        ));
    }

    if config.admin.metrics_rate_limit_burst < config.admin.metrics_rate_limit_rps {
        return Err(ConfigError::Invalid(
            "admin.metrics_rate_limit_burst must be >= admin.metrics_rate_limit_rps".to_string(),
        ));
    }

    if config.admin.readyz_rate_limit_rps == 0 {
        return Err(ConfigError::Invalid(
            "admin.readyz_rate_limit_rps must be > 0".to_string(),
        ));
    }

    if config.admin.readyz_rate_limit_burst == 0 {
        return Err(ConfigError::Invalid(
            "admin.readyz_rate_limit_burst must be > 0".to_string(),
        ));
    }

    if config.admin.readyz_rate_limit_burst < config.admin.readyz_rate_limit_rps {
        return Err(ConfigError::Invalid(
            "admin.readyz_rate_limit_burst must be >= admin.readyz_rate_limit_rps".to_string(),
        ));
    }

    if config.admin.status_rate_limit_rps == 0 {
        return Err(ConfigError::Invalid(
            "admin.status_rate_limit_rps must be > 0".to_string(),
        ));
    }

    if config.admin.status_rate_limit_burst == 0 {
        return Err(ConfigError::Invalid(
            "admin.status_rate_limit_burst must be > 0".to_string(),
        ));
    }

    if config.admin.status_rate_limit_burst < config.admin.status_rate_limit_rps {
        return Err(ConfigError::Invalid(
            "admin.status_rate_limit_burst must be >= admin.status_rate_limit_rps".to_string(),
        ));
    }

    if config.admin.enabled {
        let admin_addr: std::net::SocketAddr = config.admin.bind.parse().map_err(|e| {
            ConfigError::Invalid(format!("admin.bind must be a valid socket address: {e}"))
        })?;

        let has_manifest_auth = config.admin.token_manifest_file.is_some();

        if matches!(
            config.admin.probe_auth_mode,
            AdminProbeAuthMode::AllowUnauthenticatedLoopback
        ) && !admin_addr.ip().is_loopback()
        {
            return Err(ConfigError::Invalid(
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
            return Err(ConfigError::Invalid(
                "admin.read_token_env must not be empty when configured".to_string(),
            ));
        }

        if config
            .admin
            .write_token_env
            .as_ref()
            .is_some_and(|name| name.trim().is_empty())
        {
            return Err(ConfigError::Invalid(
                "admin.write_token_env must not be empty when configured".to_string(),
            ));
        }

        if config
            .admin
            .audit_signing_key_env
            .as_ref()
            .is_some_and(|name| name.trim().is_empty())
        {
            return Err(ConfigError::Invalid(
                "admin.audit_signing_key_env must not be empty when configured".to_string(),
            ));
        }

        if let Some(audit_log_file) = &config.admin.audit_log_file {
            if audit_log_file.as_os_str().is_empty() {
                return Err(ConfigError::Invalid(
                    "admin.audit_log_file must not be empty when configured".to_string(),
                ));
            }

            if audit_log_file.exists() && !audit_log_file.is_file() {
                return Err(ConfigError::Invalid(format!(
                    "admin.audit_log_file exists but is not a file: {}",
                    audit_log_file.display()
                )));
            }

            if let Some(parent) = audit_log_file.parent()
                && !parent.as_os_str().is_empty()
                && !parent.is_dir()
            {
                return Err(ConfigError::Invalid(format!(
                    "admin.audit_log_file parent directory does not exist: {}",
                    parent.display()
                )));
            }
        }

        if let Some(notification_log_file) = &config.admin.notification_log_file {
            if notification_log_file.as_os_str().is_empty() {
                return Err(ConfigError::Invalid(
                    "admin.notification_log_file must not be empty when configured".to_string(),
                ));
            }

            if notification_log_file.exists() && !notification_log_file.is_file() {
                return Err(ConfigError::Invalid(format!(
                    "admin.notification_log_file exists but is not a file: {}",
                    notification_log_file.display()
                )));
            }

            if let Some(parent) = notification_log_file.parent()
                && !parent.as_os_str().is_empty()
                && !parent.is_dir()
            {
                return Err(ConfigError::Invalid(format!(
                    "admin.notification_log_file parent directory does not exist: {}",
                    parent.display()
                )));
            }
        }

        if let Some(signal_ingress_file) = &config.admin.signal_ingress_file {
            if signal_ingress_file.as_os_str().is_empty() {
                return Err(ConfigError::Invalid(
                    "admin.signal_ingress_file must not be empty when configured".to_string(),
                ));
            }

            if signal_ingress_file.exists() && !signal_ingress_file.is_file() {
                return Err(ConfigError::Invalid(format!(
                    "admin.signal_ingress_file exists but is not a file: {}",
                    signal_ingress_file.display()
                )));
            }

            if let Some(parent) = signal_ingress_file.parent()
                && !parent.as_os_str().is_empty()
                && !parent.is_dir()
            {
                return Err(ConfigError::Invalid(format!(
                    "admin.signal_ingress_file parent directory does not exist: {}",
                    parent.display()
                )));
            }
        }

        if let Some(notification_kafka) = &config.admin.notification_kafka {
            notification_kafka
                .validate()
                .map_err(ConfigError::Invalid)?;
        }

        if let Some(signal_ingress_kafka) = &config.admin.signal_ingress_kafka {
            signal_ingress_kafka
                .validate()
                .map_err(ConfigError::Invalid)?;
        }

        if (config.admin.write_token_env.is_some() || has_manifest_auth)
            && config.admin.notification_log_file.is_none()
            && config.admin.notification_kafka.is_none()
        {
            return Err(ConfigError::Invalid(
                "admin.notification_log_file or admin.notification_kafka is required when write-capable admin signaling is enabled"
                    .to_string(),
            ));
        }

        if let Some(manifest_file) = &config.admin.token_manifest_file {
            if config.admin.token_manifest_max_staleness_ms.is_none() {
                return Err(ConfigError::Invalid(
                        "admin.token_manifest_max_staleness_ms is required when admin.token_manifest_file is configured"
                            .to_string(),
                    ));
            }
            if manifest_file.as_os_str().is_empty() {
                return Err(ConfigError::Invalid(
                    "admin.token_manifest_file must not be empty when configured".to_string(),
                ));
            }
            if !manifest_file.is_file() {
                return Err(ConfigError::Invalid(format!(
                    "admin.token_manifest_file does not exist or is not a file: {}",
                    manifest_file.display()
                )));
            }
            let trusted_keys = token_manifest_policy::parse_trusted_manifest_keys(
                &config.admin.token_manifest_trusted_public_keys_hex,
            )
            .map_err(ConfigError::Invalid)?;
            let manifest =
                token_manifest_policy::load_signed_token_manifest(manifest_file, &trusted_keys)
                    .map_err(ConfigError::Invalid)?;
            if !token_manifest_policy::has_write_scope(&manifest.tokens) {
                return Err(ConfigError::Invalid(
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
            return Err(ConfigError::Invalid(
                "admin.token_manifest_trusted_public_keys_hex requires admin.token_manifest_file"
                    .to_string(),
            ));
        }

        if config.admin.token_manifest_refresh_ms == 0 {
            return Err(ConfigError::Invalid(
                "admin.token_manifest_refresh_ms must be > 0".to_string(),
            ));
        }

        for proxy_ip in &config.admin.trusted_proxy_ips {
            if proxy_ip.trim().is_empty() {
                return Err(ConfigError::Invalid(
                    "admin.trusted_proxy_ips entries must not be empty".to_string(),
                ));
            }

            if proxy_ip.parse::<std::net::IpAddr>().is_err() {
                return Err(ConfigError::Invalid(format!(
                    "admin.trusted_proxy_ips contains invalid IP address '{proxy_ip}'"
                )));
            }
        }

        if let Some(max_staleness_ms) = config.admin.token_manifest_max_staleness_ms {
            if max_staleness_ms == 0 {
                return Err(ConfigError::Invalid(
                    "admin.token_manifest_max_staleness_ms must be > 0 when configured".to_string(),
                ));
            }

            if config.admin.token_manifest_file.is_none() {
                return Err(ConfigError::Invalid(
                    "admin.token_manifest_max_staleness_ms requires admin.token_manifest_file"
                        .to_string(),
                ));
            }
        }

        if !admin_addr.ip().is_loopback() {
            if config.admin.read_token_env.is_none() && !has_manifest_auth {
                return Err(ConfigError::Invalid(
                    "admin.read_token_env or admin.token_manifest_file is required when admin.bind is non-loopback"
                        .to_string(),
                ));
            }
            if config.admin.write_token_env.is_none() && !has_manifest_auth {
                return Err(ConfigError::Invalid(
                    "admin.write_token_env or admin.token_manifest_file is required when admin.bind is non-loopback"
                        .to_string(),
                ));
            }
            if config.admin.tls.is_none() {
                return Err(ConfigError::Invalid(
                    "admin.tls is required when admin.bind is non-loopback".to_string(),
                ));
            }
        }

        if let Some(tls) = &config.admin.tls {
            if tls.cert_file.as_os_str().is_empty() {
                return Err(ConfigError::Invalid(
                    "admin.tls.cert_file must not be empty".to_string(),
                ));
            }
            if tls.key_file.as_os_str().is_empty() {
                return Err(ConfigError::Invalid(
                    "admin.tls.key_file must not be empty".to_string(),
                ));
            }
            if !tls.cert_file.is_file() {
                return Err(ConfigError::Invalid(format!(
                    "admin.tls.cert_file does not exist or is not a file: {}",
                    tls.cert_file.display()
                )));
            }
            if !tls.key_file.is_file() {
                return Err(ConfigError::Invalid(format!(
                    "admin.tls.key_file does not exist or is not a file: {}",
                    tls.key_file.display()
                )));
            }

            if tls.require_client_cert && tls.client_ca_file.is_none() {
                return Err(ConfigError::Invalid(
                    "admin.tls.client_ca_file is required when admin.tls.require_client_cert=true"
                        .to_string(),
                ));
            }

            if let Some(ca_file) = &tls.client_ca_file {
                if ca_file.as_os_str().is_empty() {
                    return Err(ConfigError::Invalid(
                        "admin.tls.client_ca_file must not be empty when configured".to_string(),
                    ));
                }
                if !ca_file.is_file() {
                    return Err(ConfigError::Invalid(format!(
                        "admin.tls.client_ca_file does not exist or is not a file: {}",
                        ca_file.display()
                    )));
                }
            }
        }
    }

    validate_sink_config("sink", &config.sink)?;
    for named in &config.sinks {
        validate_sink_config(&format!("sinks.{}", named.name), &named.sink)?;
    }

    validate_delivery_contract(config)?;

    match &config.state.offset.backend {
        StateBackend::LocalFs => {}
        StateBackend::KafkaTopic(kafka_state) => {
            kafka_state.validate().map_err(ConfigError::Invalid)?;
        }
        StateBackend::Redis(redis_config) => {
            let resolved_url = redis_config.url.resolve().map_err(|e| {
                ConfigError::Invalid(format!("state.offset.redis.url could not be resolved: {e}"))
            })?;
            if resolved_url.trim().is_empty() {
                return Err(ConfigError::Invalid(
                    "state.offset.redis.url must not be empty".to_string(),
                ));
            }
        }
        StateBackend::Postgresql(pg_config) => {
            let resolved_url = pg_config.url.resolve().unwrap_or_default();
            if resolved_url.trim().is_empty() {
                return Err(ConfigError::Invalid(
                    "state.offset.postgresql.url must not be empty".to_string(),
                ));
            }
        }
    }

    for rule in &config.pipeline.transforms {
        rule.validate().map_err(ConfigError::Invalid)?;
    }

    config.runtime.validate().map_err(ConfigError::Invalid)?;

    config
        .pipeline
        .transform_runtime
        .validate()
        .map_err(ConfigError::Invalid)?;

    config
        .observability
        .validate()
        .map_err(ConfigError::Invalid)?;

    if let Some(incremental) = config.incremental_snapshot.as_ref() {
        incremental.validate().map_err(ConfigError::Invalid)?;
    }

    // Both bootstrap the same tables by different means. Accepting both would read
    // every listed table twice — once blocking, once through the watermark window —
    // and the duplicate would look like genuine change data downstream.
    //
    // An empty `[incremental_snapshot]` bootstraps nothing, so it does not conflict:
    // it exists to make `execute_snapshot` reachable, and refusing it alongside
    // `snapshot_tables` would rule out a combination that has no overlap.
    if config
        .incremental_snapshot
        .as_ref()
        .is_some_and(crate::config::schema::IncrementalSnapshotConfig::backfills_at_startup)
        && !config.snapshot_tables.is_empty()
    {
        return Err(ConfigError::Invalid(
            "snapshot_tables and incremental_snapshot.tables are two bootstrapping paths \
             for the same job; set exactly one. incremental_snapshot does not block the \
             stream and resumes from its persisted chunk cursor after a restart."
                .to_string(),
        ));
    }

    Ok(())
}

/// Every sink an event can reach, labelled the way the operator wrote it.
///
/// `[[sinks]]` entries are routing destinations, not documentation: a `[[pipeline.routes]]`
/// entry sends real change events to one, so a contract checked only against `[sink]` is
/// checked against a sink the events may never reach.
fn routed_sinks(config: &AppConfig) -> Vec<(String, &SinkConfig)> {
    std::iter::once(("sink".to_string(), &config.sink))
        .chain(
            config
                .sinks
                .iter()
                .map(|named| (format!("sinks.{}", named.name), &named.sink)),
        )
        .collect()
}

/// How a sink reaches `effectively_once`, if it can at all.
///
/// # Why a mechanism rather than a pair of booleans
///
/// `sink.kafka` in **idempotent** mode and `sink.snowflake` both answer "idempotent
/// delivery, no transactional barrier" — the same pair — yet one reaches the contract and
/// the other does not. Snowflake's guarantee comes from a destination-side offset token
/// rather than a transaction this process opens, and no combination of those flags
/// separates them. Asking *which mechanism* does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EffectivelyOnceMechanism {
    /// The batch's records and its checkpoint commit in one Kafka transaction.
    ///
    /// Requires the checkpoint to live in Kafka on the same cluster, and requires this to
    /// be the **only** such sink in the pipeline.
    KafkaTransaction,

    /// A destination-side offset token the sink advances before `flush` returns, so a
    /// replayed batch is filtered at the destination rather than duplicated.
    DestinationOffsetToken,
}

fn effectively_once_mechanism(sink: &SinkConfig) -> Option<EffectivelyOnceMechanism> {
    match sink {
        SinkConfig::Kafka(kafka) => matches!(
            kafka.delivery_mode,
            super::schema::KafkaDeliveryMode::Transactional
        )
        .then_some(EffectivelyOnceMechanism::KafkaTransaction),
        SinkConfig::Snowflake(_) => Some(EffectivelyOnceMechanism::DestinationOffsetToken),
        // Fan-out is excluded even when every child is transactional: the children are
        // erased to `BoxedSink`, so no child's producer is reachable and no transaction can
        // span them. `BuiltSink::transaction_handle` returns `None` for that reason, and a
        // `None` handle checkpoints through a second producer, outside the transaction.
        _ => None,
    }
}

fn validate_delivery_contract(config: &AppConfig) -> Result<(), ConfigError> {
    let routed = routed_sinks(config);

    // Checked for every routed sink, not only the default one: `delivery_mode` is a
    // statement about the contract, and a named sink in transactional mode under
    // `at_least_once` opens a transaction nothing ever couples a checkpoint to.
    for (path, sink) in &routed {
        if let SinkConfig::Kafka(kafka) = sink
            && matches!(
                kafka.delivery_mode,
                super::schema::KafkaDeliveryMode::Transactional
            )
            && config.delivery_contract != DeliveryContract::EffectivelyOnce
        {
            return Err(ConfigError::Invalid(format!(
                "delivery_contract must be \"effectively_once\" when \
                 {path}.delivery_mode=\"transactional\""
            )));
        }
    }

    validate_durability_waits_fit_the_flush_timeout(config)?;

    if config.delivery_contract != DeliveryContract::EffectivelyOnce {
        return Ok(());
    }

    // Every sink the events reach has to carry the guarantee, because the contract is a
    // property of the pipeline rather than of whichever sink happens to be listed first.
    //
    // The transactional sinks are collected *with* their config rather than by name, so the
    // cluster check below reads the one it already has instead of looking it up again — a
    // second lookup that can only fail by construction is a panic waiting for a refactor to
    // make it reachable.
    let mut kafka_transaction_sinks: Vec<(&str, &super::sink::KafkaSinkConfig)> = Vec::new();
    for (path, sink) in &routed {
        match (effectively_once_mechanism(sink), sink) {
            (Some(EffectivelyOnceMechanism::KafkaTransaction), SinkConfig::Kafka(kafka)) => {
                kafka_transaction_sinks.push((path.as_str(), kafka));
            }
            (Some(EffectivelyOnceMechanism::KafkaTransaction), _) => {
                // Unreachable by construction — only a Kafka sink yields this mechanism —
                // and treated as "no mechanism" rather than asserted, so a future sink that
                // gains a transaction is rejected until this arm is taught about it.
                return Err(ConfigError::Invalid(format!(
                    "delivery_contract='effectively_once': {path} reports a Kafka transaction \
                     but is not a Kafka sink; this is a bug in rustcdc, please report it"
                )));
            }
            (Some(EffectivelyOnceMechanism::DestinationOffsetToken), _) => {}
            (None, _) => {
                return Err(ConfigError::Invalid(format!(
                    "delivery_contract='effectively_once' is incompatible with \
                     {path}.type='{}'. Reaching it needs either a Kafka sink with \
                     delivery_mode=\"transactional\", so the records and the checkpoint \
                     commit in one transaction, or a sink with a destination-side offset \
                     token (snowflake). Either change that sink, or set \
                     delivery_contract=\"at_least_once\".",
                    sink_name(sink)
                )));
            }
        }
    }

    // Two transactions are not one. `BuiltRouter::transaction_handle` yields `None` when a
    // second transactional sink exists, and a `None` handle writes the checkpoint through
    // a separate producer — outside either transaction, which is precisely the window this
    // contract is chosen to close. Refusing is the only honest answer; picking one of the
    // two silently leaves the other's records uncoupled.
    if kafka_transaction_sinks.len() > 1 {
        return Err(ConfigError::Invalid(format!(
            "delivery_contract='effectively_once' allows at most one transactional Kafka \
             sink, but {} are configured ({}). One Kafka transaction cannot span two \
             producers, so the checkpoint could only ever be written inside one of them \
             and the other's records would commit separately. Route through a single \
             transactional sink, or set delivery_contract=\"at_least_once\".",
            kafka_transaction_sinks.len(),
            kafka_transaction_sinks
                .iter()
                .map(|(path, _)| *path)
                .collect::<Vec<_>>()
                .join(", ")
        )));
    }

    // Only the Kafka mechanism constrains the state backend. A pipeline reaching the
    // contract purely through destination-side offset tokens needs no Kafka at all, which
    // is why this is not a blanket requirement of the contract.
    if let Some((path, kafka)) = kafka_transaction_sinks.first() {
        validate_effectively_once_state_backend(path, kafka, config)?;
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
///
/// `path` names the transactional sink the way the operator wrote it, because it is no
/// longer necessarily `[sink]` — a `[[sinks]]` entry can be the one carrying the
/// transaction.
fn validate_effectively_once_state_backend(
    path: &str,
    sink: &super::sink::KafkaSinkConfig,
    config: &AppConfig,
) -> Result<(), ConfigError> {
    let super::state::StateBackend::KafkaTopic(state) = &config.state.offset.backend else {
        return Err(ConfigError::Invalid(format!(
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
        return Err(ConfigError::Invalid(format!(
            "delivery_contract='effectively_once' requires the transactional sink and the \
             checkpoint topic to be on the same Kafka cluster, because one transaction cannot \
             span two. {path}.brokers is '{}' and state.offset.backend.kafka_topic.brokers is \
             '{}'.",
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

/// A sink's durability wait must finish inside the runtime's flush timeout.
///
/// `runtime.sink_flush_timeout_ms` wraps `sink.flush()` in a `tokio::time::timeout`. When it
/// fires first the flush future is **dropped mid-wait** — after the records have been sent.
/// The run loop then retries the batch, and the records go to the destination twice.
///
/// This is not hypothetical: the Snowflake sink shipped with both defaults at 60 000 ms, so
/// they raced and the runtime usually won. Both sinks that wait for a destination-side
/// acknowledgement are checked here, because the shape is the trap, not the vendor.
fn validate_durability_waits_fit_the_flush_timeout(config: &AppConfig) -> Result<(), ConfigError> {
    /// Headroom for the send itself plus one round trip.
    const SLACK_MS: u64 = 5_000;

    let flush_timeout = config.runtime.sink_flush_timeout_ms;
    let mut queue: Vec<&SinkConfig> = vec![&config.sink];
    queue.extend(config.sinks.iter().map(|named| &named.sink));

    while let Some(sink) = queue.pop() {
        let (field, wait) = match sink {
            SinkConfig::Fan(fan) => {
                queue.extend(fan.sinks.iter());
                continue;
            }
            SinkConfig::Snowflake(snowflake) => (
                "sink.snowflake.commit_timeout_ms",
                snowflake.commit_timeout_ms,
            ),
            SinkConfig::Zerobus(zerobus) => ("sink.zerobus.ack_timeout_ms", zerobus.ack_timeout_ms),
            _ => continue,
        };

        if wait + SLACK_MS > flush_timeout {
            return Err(ConfigError::Invalid(format!(
                "{field} ({wait}) must be at least {SLACK_MS} ms below \
                 runtime.sink_flush_timeout_ms ({flush_timeout}), which wraps the whole \
                 flush. Otherwise the runtime cancels the durability wait mid-flight — the \
                 records are already sent, the batch is retried, and a bound the caller \
                 abandons is not a bound. Raise runtime.sink_flush_timeout_ms to at least {}.",
                wait + SLACK_MS
            )));
        }
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
        SinkConfig::Snowflake(_) => "snowflake",
        SinkConfig::Zerobus(_) => "zerobus",
        SinkConfig::Fan(_) => "fan_out",
    }
}

/// Validate one sink and, for fan-out, every child underneath it.
///
/// `path` names the sink the way the operator wrote it — `sink`, `sinks.warehouse`,
/// `sink.sinks[1]` — so an error points at the block to edit rather than at "the Kafka
/// sink" when three of them are configured.
fn validate_sink_config(path: &str, sink: &SinkConfig) -> Result<(), ConfigError> {
    let relabel = |e: String| ConfigError::Invalid(relabel_sink_error(path, e));

    match sink {
        SinkConfig::Fan(fan) => {
            validate_fan_sink_config(path, fan)?;
            for (i, child) in fan.sinks.iter().enumerate() {
                validate_sink_config(&format!("{path}.sinks[{i}]"), child)?;
            }
        }
        SinkConfig::Kafka(kafka) => kafka.validate().map_err(relabel)?,
        SinkConfig::Iceberg(iceberg) => iceberg.validate().map_err(relabel)?,
        SinkConfig::Http(http) => http.validate().map_err(relabel)?,
        _ => {}
    }

    validate_sink_codec(path, sink)
}

/// Re-point a sink error written for the default `[sink]` block at the sink it came from.
///
/// The per-sink `validate()` methods spell their own paths (`sink.kafka.topic`), which is
/// right for the default sink and wrong for every other one. Rewriting the prefix here
/// keeps one message per rule instead of threading a path argument through every
/// validator — and leaves a message that names no path alone.
fn relabel_sink_error(path: &str, error: String) -> String {
    if path == "sink" {
        return error;
    }
    for kind in ["kafka", "iceberg", "http"] {
        if let Some(rest) = error.strip_prefix(&format!("sink.{kind}.")) {
            return format!("{path}.{kind}.{rest}");
        }
    }
    format!("{path}: {error}")
}

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
                    .map_err(|e| ConfigError::Invalid(format!("{path}.codec: {e}"))),
                None => Ok(()),
            }
        }
    }
}

fn validate_fan_sink_config(
    path: &str,
    fan: &super::schema::FanSinkConfig,
) -> Result<(), ConfigError> {
    if fan.sinks.is_empty() {
        return Err(ConfigError::Invalid(format!(
            "{path}.sinks must contain at least one child sink"
        )));
    }
    for (i, child) in fan.sinks.iter().enumerate() {
        if matches!(child, SinkConfig::Fan(_)) {
            return Err(ConfigError::Invalid(format!(
                "{path}.sinks[{i}]: nested fan-out sinks are not supported"
            )));
        }
    }
    Ok(())
}

pub(crate) fn validate_http_sink_url_policy(url: &str) -> Result<(), ConfigError> {
    let parsed = reqwest::Url::parse(url)
        .map_err(|e| ConfigError::Invalid(format!("sink.http.url must be a valid URL: {e}")))?;

    // A credential in the URL is readable by anyone holding a *read*-scoped admin
    // token: `/status` returns the config snapshot, and a URL is not a `SecretString`,
    // so no amount of redaction downstream is guaranteed to catch it. Rejecting it here
    // means the credential never enters the process in a form that can leak.
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err(ConfigError::Invalid(
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
                Err(ConfigError::Invalid(
                    "sink.http.url must use https except for localhost loopback testing"
                        .to_string(),
                ))
            }
        }
        other => Err(ConfigError::Invalid(format!(
            "sink.http.url scheme must be https (or http on localhost); found '{other}'"
        ))),
    }
}

#[cfg(test)]
#[path = "loader_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "loader_sink_tests.rs"]
mod sink_tests;
