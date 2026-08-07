use crate::error::ConfigError;

pub fn load_and_migrate_value(
    mut raw: serde_json::Value,
    target_version: &str,
) -> Result<serde_json::Value, ConfigError> {
    let current_version = config_api_version(&raw)?;

    // Accept v1alpha1 as the legacy name for v1 — bump the tag only.
    if current_version == "v1alpha1" {
        if let Some(v) = raw.get_mut("api_version") {
            *v = serde_json::Value::String("v1".to_string());
        }
    }

    let current_version = config_api_version(&raw)?;
    if current_version != target_version {
        return Err(ConfigError::InvalidApiVersion(current_version));
    }

    // Apply structural normalization (idempotent; handles legacy sub-table syntax).
    raw = normalize(raw)?;
    Ok(raw)
}

/// Normalize v1 config structure (idempotent):
///
/// 1. **Source**: flatten `source.<kind>.*` sub-tables into `source.*` with `type = "<kind>"`.
/// 2. **Sink**: reject removed sink types (`avro`, `otel`).
/// 3. **Pipeline**: hoist top-level `transforms`/`transform_runtime` into `[pipeline]`.
/// 4. **Format**: remove deprecated global `[format]` section.
/// 5. **State**: split flat `[state]` (dir/backend) into `[state.offset]` + `[state.schema_history]`.
fn normalize(mut raw: serde_json::Value) -> Result<serde_json::Value, ConfigError> {
    raw = normalize_source(raw)?;
    normalize_sink(&raw)?;
    raw = normalize_pipeline(raw);
    raw = normalize_state(raw)?;
    Ok(raw)
}

fn normalize_source(mut raw: serde_json::Value) -> Result<serde_json::Value, ConfigError> {
    let source = raw
        .get("source")
        .and_then(|v| v.as_object())
        .cloned()
        .unwrap_or_default();

    // Already in flat format — nothing to do.
    if source.contains_key("type") {
        return Ok(raw);
    }

    let source_sub_keys: Vec<&str> = ["postgres", "mysql", "mariadb", "sqlserver", "mssql"]
        .iter()
        .filter(|&&k| source.contains_key(k))
        .copied()
        .collect();

    if source_sub_keys.len() > 1 {
        return Err(ConfigError::InvalidState(
            "exactly one source block must be configured".to_string(),
        ));
    }

    if let Some(&sub_key) = source_sub_keys.first() {
        let canonical_type = if sub_key == "mssql" {
            "sqlserver"
        } else {
            sub_key
        };

        let sub_value = source
            .get(sub_key)
            .cloned()
            .unwrap_or(serde_json::Value::Object(Default::default()));

        let mut new_source = serde_json::Map::new();
        new_source.insert(
            "type".to_string(),
            serde_json::Value::String(canonical_type.to_string()),
        );

        if let Some(rp) = source.get("require_primary") {
            new_source.insert("require_primary".to_string(), rp.clone());
        }

        if let Some(sub_obj) = sub_value.as_object() {
            for (k, v) in sub_obj {
                new_source.insert(k.clone(), v.clone());
            }
        }

        raw["source"] = serde_json::Value::Object(new_source);
    }
    Ok(raw)
}

fn normalize_sink(raw: &serde_json::Value) -> Result<(), ConfigError> {
    let sink_type = raw
        .get("sink")
        .and_then(|v| v.as_object())
        .and_then(|o| o.get("type"))
        .and_then(|t| t.as_str())
        .map(str::to_string);

    if sink_type.as_deref() == Some("avro") {
        return Err(ConfigError::InvalidState(
            "sink type='avro' is no longer supported. \
             Use type='file_jsonl' or type='iceberg' for file output, or \
             type='kafka' with [sink.kafka.codec] type='avro_confluent' for Confluent Avro."
                .to_string(),
        ));
    }

    if sink_type.as_deref() == Some("otel") {
        return Err(ConfigError::InvalidState(
            "sink type='otel' is no longer supported. \
             Configure OpenTelemetry export via [observability.otlp_endpoint]."
                .to_string(),
        ));
    }

    Ok(())
}

fn normalize_pipeline(mut raw: serde_json::Value) -> serde_json::Value {
    // Total rather than `.expect("config is a JSON object")`: figment can hand us a
    // non-object document (an empty file, or a TOML array at the root), and a panic in
    // the config loader is the worst possible way to report a malformed file. A
    // non-object simply has nothing to hoist; the typed deserialise reports it properly.
    let Some(obj) = raw.as_object_mut() else {
        return raw;
    };
    let transforms = obj.remove("transforms");
    let transform_runtime = obj.remove("transform_runtime");

    if transforms.is_some() || transform_runtime.is_some() {
        let existing_pipeline = obj
            .get("pipeline")
            .and_then(|v| v.as_object())
            .cloned()
            .unwrap_or_default();
        let mut pipeline = existing_pipeline;
        if let Some(t) = transforms {
            pipeline.insert("transforms".to_string(), t);
        }
        if let Some(tr) = transform_runtime {
            pipeline.insert("transform_runtime".to_string(), tr);
        }
        obj.insert("pipeline".to_string(), serde_json::Value::Object(pipeline));
    }

    // Remove deprecated global [format] section.
    obj.remove("format");
    raw
}

/// Split the legacy flat `[state]` table into `state.offset` / `state.schema_history`.
///
/// Rejects any other key under the flat shape. This function rebuilds the table from
/// `dir` and `backend` alone, so anything else was previously **discarded in silence** —
/// a `snapshot_tables` key indented one table too far parsed cleanly and did nothing,
/// and the unknown-key guard in the loader could not see it because the migration had
/// already removed it.
fn normalize_state(mut raw: serde_json::Value) -> Result<serde_json::Value, ConfigError> {
    if let Some(obj) = raw.as_object_mut() {
        // Only split if state is a flat object (has dir/backend at the top level).
        let needs_split = obj
            .get("state")
            .and_then(|s| s.as_object())
            .map(|s| s.contains_key("dir") || s.contains_key("backend"))
            .unwrap_or(false);

        if needs_split {
            // `needs_split` proved this is an object, but returning an error costs
            // nothing and keeps the function total.
            let Some(state) = obj.remove("state") else {
                return Ok(raw);
            };

            let mut stray: Vec<String> = state
                .as_object()
                .into_iter()
                .flat_map(|map| map.keys())
                .filter(|key| key.as_str() != "dir" && key.as_str() != "backend")
                .cloned()
                .collect();
            if !stray.is_empty() {
                stray.sort();
                return Err(ConfigError::InvalidState(format!(
                    "unrecognised key(s) under [state]: {}. The flat [state] table \
                     accepts only `dir` and `backend`; anything else was previously \
                     dropped without a word. If you meant a top-level setting, move it \
                     above the [state] header.",
                    stray.join(", ")
                )));
            }

            let dir = state
                .get("dir")
                .cloned()
                .unwrap_or_else(|| serde_json::json!("./state"));
            let backend = state.get("backend").cloned();

            let mut offset_map = serde_json::Map::new();
            offset_map.insert("dir".to_string(), dir.clone());
            if let Some(b) = &backend {
                offset_map.insert("backend".to_string(), b.clone());
            }

            let mut schema_history_map = serde_json::Map::new();
            schema_history_map.insert("dir".to_string(), dir);
            if let Some(b) = backend {
                schema_history_map.insert("backend".to_string(), b);
            }

            let mut new_state = serde_json::Map::new();
            new_state.insert("offset".to_string(), serde_json::Value::Object(offset_map));
            new_state.insert(
                "schema_history".to_string(),
                serde_json::Value::Object(schema_history_map),
            );
            obj.insert("state".to_string(), serde_json::Value::Object(new_state));
        }
    }
    Ok(raw)
}

fn config_api_version(raw: &serde_json::Value) -> Result<String, ConfigError> {
    raw.get("api_version")
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| {
            ConfigError::InvalidState(
                "api_version must be present and a string in configuration root".to_string(),
            )
        })
}
