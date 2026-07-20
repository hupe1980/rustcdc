/// Redact secrets using an explicit schema contract.
///
/// This operates on parsed JSON so key ordering and whitespace do not matter,
/// and only known paths/keys are redacted.
pub(crate) fn redact_secrets(json: &str) -> String {
    match serde_json::from_str::<serde_json::Value>(json) {
        Ok(mut v) => {
            let mut path = Vec::new();
            redact_value(&mut v, &mut path);
            serde_json::to_string_pretty(&v).unwrap_or_else(|_| json.to_string())
        }
        Err(_) => json.to_string(),
    }
}

const SENSITIVE_PATHS: &[&str] = &[
    // Source credentials
    "source.postgres.password",
    "source.postgres.url",
    "source.sqlserver.password",
    "source.sqlserver.url",
    // Sink credentials
    "sink.bearer_token",
    "sink.kafka.security.sasl_password",
    "sink.kafka.security.ssl_keystore_password",
    "sink.kafka.security.ssl_key_password",
    // State backend credentials
    "state.backend.postgres.url",
    "state.backend.kafka_topic.security.sasl_password",
    // Admin notification channels
    "admin.notification_kafka.security.sasl_password",
    "admin.signal_ingress_kafka.security.sasl_password",
    // Iceberg catalog credentials
    "iceberg.catalog.rest.token",
    "iceberg.catalog.rest.credential",
];

const SENSITIVE_HEADER_MAP_PATHS: &[&str] = &["sink.headers"];

const SENSITIVE_HEADER_KEYS: &[&str] = &[
    "authorization",
    "proxy-authorization",
    "x-api-key",
    "api-key",
    "apikey",
    "x-auth-token",
    "x-access-token",
];

const SENSITIVE_KEY_NAME_TOKENS: &[&str] = &[
    "password",
    "secret",
    "token",
    "credential",
    "api_key",
    "apikey",
    "authorization",
    "private_key",
    "key_file",
];

const NON_SECRET_KEY_NAME_EXCEPTIONS: &[&str] = &[
    "public_key_hex",
    "trusted_public_keys_hex",
    "trusted_signer_public_keys_hex",
    "revoked_signer_public_keys_hex",
    "signature_hex",
];

fn is_sensitive_path(path: &[String]) -> bool {
    let joined = path.join(".");
    SENSITIVE_PATHS.iter().any(|candidate| joined == *candidate)
}

fn is_sensitive_header_entry(path: &[String], key: &str) -> bool {
    let parent = path.join(".");
    if !SENSITIVE_HEADER_MAP_PATHS
        .iter()
        .any(|candidate| parent == *candidate)
    {
        return false;
    }

    let lower = key.to_ascii_lowercase();
    SENSITIVE_HEADER_KEYS
        .iter()
        .any(|candidate| lower == *candidate)
}

fn is_sensitive_key_name(key: &str) -> bool {
    let lower = key.to_ascii_lowercase();
    if NON_SECRET_KEY_NAME_EXCEPTIONS
        .iter()
        .any(|candidate| lower == *candidate)
    {
        return false;
    }

    SENSITIVE_KEY_NAME_TOKENS
        .iter()
        .any(|token| lower.contains(token))
}

fn should_redact_key(path: &[String], key: &str) -> bool {
    let mut candidate_path = path.to_vec();
    candidate_path.push(key.to_string());

    if SENSITIVE_HEADER_MAP_PATHS
        .iter()
        .any(|candidate| path.join(".") == *candidate)
    {
        return is_sensitive_header_entry(path, key);
    }

    is_sensitive_path(&candidate_path) || is_sensitive_key_name(key)
}

fn redact_value(v: &mut serde_json::Value, path: &mut Vec<String>) {
    match v {
        serde_json::Value::Object(map) => {
            for (k, val) in map.iter_mut() {
                if should_redact_key(path, k) {
                    // Redact any non-null secret value fail-closed, including
                    // objects/arrays, to avoid schema-shape leak regressions.
                    if !val.is_null() {
                        *val = serde_json::Value::String("[REDACTED]".to_string());
                        continue;
                    }
                }

                path.push(k.clone());
                redact_value(val, path);
                path.pop();
            }
        }
        serde_json::Value::Array(arr) => {
            for (index, item) in arr.iter_mut().enumerate() {
                path.push(index.to_string());
                redact_value(item, path);
                path.pop();
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use serde_json::Value;

    use super::{redact_secrets, should_redact_key, NON_SECRET_KEY_NAME_EXCEPTIONS};

    #[test]
    fn redacts_schema_sensitive_fields() {
        let input = r#"{
    "source": {
        "postgres": {
            "host": "db.local",
            "password": "one",
            "url": "postgres://cdc:pw@db.local:5432/cdc"
        }
    },
    "sink": {
        "type": "http",
        "bearer_token": "two"
    },
    "state": {
        "backend": {
            "postgres": {
                "url": "postgres://state:pw@db.local:5432/state"
            }
        }
    }
}"#;

        let out = redact_secrets(input);
        let v: Value = serde_json::from_str(&out).expect("valid redacted json");

        assert_eq!(v["source"]["postgres"]["password"], "[REDACTED]");
        assert_eq!(v["source"]["postgres"]["url"], "[REDACTED]");
        assert_eq!(v["sink"]["bearer_token"], "[REDACTED]");
        assert_eq!(v["state"]["backend"]["postgres"]["url"], "[REDACTED]");
    }

    #[test]
    fn redacts_sensitive_http_headers_without_over_redacting_other_headers() {
        let input = r#"{
    "sink": {
        "type": "http",
        "headers": {
            "Authorization": "Bearer secret",
            "X-Api-Key": "abc123",
            "Content-Type": "application/json"
        }
    }
}"#;

        let out = redact_secrets(input);
        let v: Value = serde_json::from_str(&out).expect("valid redacted json");

        assert_eq!(v["sink"]["headers"]["Authorization"], "[REDACTED]");
        assert_eq!(v["sink"]["headers"]["X-Api-Key"], "[REDACTED]");
        assert_eq!(v["sink"]["headers"]["Content-Type"], "application/json");
    }

    #[test]
    fn schema_sensitive_like_fields_are_covered_by_redaction_policy() {
        let schema_src = [
            include_str!("config/schema.rs"),
            include_str!("config/source.rs"),
            include_str!("config/sink.rs"),
            include_str!("config/state.rs"),
            include_str!("config/pipeline.rs"),
        ]
        .join("\n");

        for line in schema_src.lines() {
            let trimmed = line.trim();
            let Some(rest) = trimmed.strip_prefix("pub ") else {
                continue;
            };
            let Some((field_decl, _)) = rest.split_once(':') else {
                continue;
            };
            let field_name = field_decl.split_whitespace().next().unwrap_or("");
            if field_name.is_empty() {
                continue;
            }

            let field_name_lower = field_name.to_ascii_lowercase();
            let is_sensitive_like = [
                "password",
                "secret",
                "token",
                "credential",
                "api_key",
                "apikey",
                "authorization",
                "private_key",
                "key_file",
            ]
            .iter()
            .any(|needle| field_name_lower.contains(needle));

            if !is_sensitive_like {
                continue;
            }

            if NON_SECRET_KEY_NAME_EXCEPTIONS
                .iter()
                .any(|candidate| field_name_lower == *candidate)
            {
                continue;
            }

            assert!(
                should_redact_key(&[], field_name),
                "schema field '{field_name}' looks secret-like but is not covered by redaction policy"
            );
        }
    }
}
