/// Redact secrets using an explicit schema contract.
///
/// This operates on parsed JSON so key ordering and whitespace do not matter,
/// and only known paths/keys are redacted.
pub fn redact_secrets(json: &str) -> String {
    match serde_json::from_str::<serde_json::Value>(json) {
        Ok(mut v) => {
            let mut path = Vec::new();
            redact_value(&mut v, &mut path);
            serde_json::to_string_pretty(&v).unwrap_or_else(|_| json.to_string())
        }
        Err(_) => json.to_string(),
    }
}

/// Paths that must be redacted **whole**, not merely stripped of userinfo.
///
/// These are written in the **post-migration** shape. `normalize_source` flattens
/// `source.<kind>.*` into `source.*`, so the entries that used to read
/// `source.postgres.password` matched nothing that `redacted_config_snapshot` could
/// ever produce — the list implied a coverage it did not have, and MySQL and MariaDB
/// were absent entirely. A connection URL is listed because the credential is not
/// always in the userinfo: `?sslpassword=` and friends live in the query string.
const SENSITIVE_PATHS: &[&str] = &[
    // Source credentials — flat shape; `type` selects the driver.
    "source.password",
    "source.url",
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

/// Does this name look like it holds a secret?
///
/// Separators are normalised to `_` before matching, so the underscore-spelled tokens
/// above also catch the hyphenated and dotted spellings. Without it `X-Api-Key` — the
/// ordinary spelling for a header, and a perfectly ordinary query parameter — matched
/// nothing, because `"x-api-key"` does not contain `"api_key"`. The header rule covered
/// that one case by exact-matching a separate list; every other surface (query strings,
/// arbitrary config keys) was uncovered. Found by the property test in
/// `tests/fuzz_properties.rs`, which generates parameter names rather than listing them.
fn is_sensitive_key_name(key: &str) -> bool {
    let normalised = key.to_ascii_lowercase().replace(['-', '.', ' '], "_");

    if NON_SECRET_KEY_NAME_EXCEPTIONS
        .iter()
        .any(|candidate| normalised == *candidate)
    {
        return false;
    }

    SENSITIVE_KEY_NAME_TOKENS
        .iter()
        .any(|token| normalised.contains(token))
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

/// Strip credentials out of a URL-shaped string, or return `None` if there are none.
///
/// This is deliberately **value**-driven rather than key-driven. Every other rule in
/// this module asks "is this field named like a secret?", which only protects fields
/// somebody remembered to name or list — and `sink.http.url` was neither, so a
/// connection string with `user:pass@` was returned verbatim by `/status` to any
/// read-scoped token. A rule that looks at the value catches the next such field
/// without an edit here.
///
/// Two carriers are handled, because a URL has two places to put a secret:
///
/// * **userinfo** — `https://user:pass@host/path`
/// * **query parameters** whose name looks like a secret — `?api_key=…`, `?token=…`.
///   This is how webhook and ingest endpoints usually carry their credential, and it
///   was the remaining hole: the loader rejects userinfo in `sink.http.url` outright,
///   so the only way a credential reaches that field *is* the query string.
///
/// The host, path and non-secret parameters are kept, because a redacted snapshot an
/// operator cannot recognise is a snapshot they stop reading.
///
/// Matching is intentionally narrow: a `scheme://` prefix is required. Free-text values
/// containing an `@` or an `=` (an email in a table comment, say) have no scheme and are
/// left alone.
fn strip_url_userinfo(text: &str) -> Option<String> {
    let scheme_end = text.find("://")?;
    if scheme_end == 0
        || !text[..scheme_end]
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"+-.".contains(&b))
    {
        return None;
    }

    let authority_start = scheme_end + 3;
    let authority_end = text[authority_start..]
        .find(['/', '?', '#'])
        .map_or(text.len(), |offset| authority_start + offset);

    let mut changed = false;
    let mut redacted = String::with_capacity(text.len());
    redacted.push_str(&text[..authority_start]);

    match text[authority_start..authority_end].rfind('@') {
        Some(at) => {
            changed = true;
            redacted.push_str("[REDACTED]");
            redacted.push_str(&text[authority_start + at..authority_end]);
        }
        None => redacted.push_str(&text[authority_start..authority_end]),
    }

    let rest = &text[authority_end..];
    match redact_query_secrets(rest) {
        Some(scrubbed) => {
            changed = true;
            redacted.push_str(&scrubbed);
        }
        None => redacted.push_str(rest),
    }

    changed.then_some(redacted)
}

/// Redact the values of secret-looking query parameters, or `None` if there are none.
///
/// `rest` is everything from the first `/`, `?` or `#` onwards, so the fragment is
/// carried through untouched — a fragment is never sent to the server and splitting on
/// it keeps `?` inside a fragment from being read as a query.
fn redact_query_secrets(rest: &str) -> Option<String> {
    let query_start = rest.find('?')?;
    let (before, query_and_fragment) = rest.split_at(query_start + 1);
    let (query, fragment) = match query_and_fragment.find('#') {
        Some(index) => query_and_fragment.split_at(index),
        None => (query_and_fragment, ""),
    };

    let mut changed = false;
    let mut out = String::with_capacity(rest.len());
    out.push_str(before);

    for (index, pair) in query.split('&').enumerate() {
        if index > 0 {
            out.push('&');
        }
        match pair.split_once('=') {
            Some((name, _)) if is_sensitive_key_name(name) => {
                changed = true;
                out.push_str(name);
                out.push_str("=[REDACTED]");
            }
            _ => out.push_str(pair),
        }
    }

    out.push_str(fragment);
    changed.then_some(out)
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
        serde_json::Value::String(text) => {
            if let Some(stripped) = strip_url_userinfo(text) {
                *text = stripped;
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

    /// The shape here is the **post-migration, flat** one — `source.password`, not
    /// `source.postgres.password`.
    ///
    /// This test used to assert against the nested shape, which `normalize_source`
    /// flattens away before anything is serialised. It exercised a document
    /// `redacted_config_snapshot` can never produce, so it passed while proving
    /// nothing about production. `redacts_a_real_app_config` below is the guard
    /// against that happening again.
    #[test]
    fn redacts_schema_sensitive_fields_in_the_flat_source_shape() {
        let input = r#"{
    "source": {
        "type": "postgres",
        "host": "db.local",
        "password": "one",
        "url": "postgres://cdc:pw@db.local:5432/cdc"
    },
    "sink": {
        "type": "http",
        "bearer_token": "two",
        "url": "https://admin:hunter2@api.example.com/events"
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

        assert_eq!(v["source"]["password"], "[REDACTED]");
        assert_eq!(
            v["source"]["url"], "[REDACTED]",
            "a source connection URL is redacted whole: a credential can sit in the \
             query string as well as the userinfo"
        );
        assert_eq!(v["sink"]["bearer_token"], "[REDACTED]");
        assert_eq!(
            v["sink"]["url"], "https://[REDACTED]@api.example.com/events",
            "a sink URL keeps its host so the snapshot stays useful for diagnostics, \
             but never its credentials"
        );
        assert_eq!(v["state"]["backend"]["postgres"]["url"], "[REDACTED]");
    }

    /// A credential embedded in a URL must not survive redaction.
    ///
    /// The loader now rejects userinfo in `sink.http.url` outright, so this is the
    /// second of two layers. It exists because the first layer is a validation rule
    /// that a future URL-bearing field could simply forget to call, and because
    /// `url` is matched by neither the sensitive-path list nor the key-name tokens —
    /// which is exactly how the value reached `/status` verbatim.
    #[test]
    fn url_userinfo_is_stripped_wherever_it_appears() {
        let input = serde_json::json!({
            "sink": { "url": "https://admin:hunter2@api.example.com/events" },
            "nested": { "list": ["mysql://root:toor@db:3306/app"] },
            "no_scheme_no_touch": "contact ops@example.com about this table",
            "plain_endpoint": "https://api.example.com/events",
        })
        .to_string();

        let out = redact_secrets(&input);
        assert!(
            !out.contains("hunter2") && !out.contains("toor"),
            "credentials survived redaction: {out}"
        );

        let v: Value = serde_json::from_str(&out).expect("valid redacted json");
        assert_eq!(
            v["sink"]["url"],
            "https://[REDACTED]@api.example.com/events"
        );
        assert_eq!(v["nested"]["list"][0], "mysql://[REDACTED]@db:3306/app");
        assert_eq!(
            v["no_scheme_no_touch"], "contact ops@example.com about this table",
            "a bare @ without a scheme is not a URL and must not be rewritten"
        );
        assert_eq!(
            v["plain_endpoint"], "https://api.example.com/events",
            "a URL without userinfo must be left exactly as it was"
        );
    }

    /// A credential in a URL **query string** must not survive either.
    ///
    /// The loader rejects userinfo in `sink.http.url` outright, so the query string is
    /// the only place a credential can actually reach that field — and it is how webhook
    /// and ingest endpoints normally carry one. Neither the sensitive-path list nor the
    /// key-name tokens see inside a URL, so `/status` returned these verbatim to any
    /// read-scoped token.
    #[test]
    fn url_query_string_credentials_are_stripped() {
        let input = serde_json::json!({
            "sink": { "url": "https://api.example.com/events?api_key=hunter2&region=eu" },
            "webhook": { "url": "https://hooks.example.com/x?token=t0ps3cret" },
            "both": { "url": "https://u:p@api.example.com/e?access_token=abc#frag" },
            "harmless": { "url": "https://api.example.com/events?region=eu&page=2" },
        })
        .to_string();

        let out = redact_secrets(&input);
        assert!(
            !out.contains("hunter2") && !out.contains("t0ps3cret") && !out.contains("abc"),
            "a query-string credential survived redaction: {out}"
        );

        let v: Value = serde_json::from_str(&out).expect("valid redacted json");
        assert_eq!(
            v["sink"]["url"], "https://api.example.com/events?api_key=[REDACTED]&region=eu",
            "the non-secret parameters must survive, or the snapshot stops being useful"
        );
        assert_eq!(
            v["webhook"]["url"],
            "https://hooks.example.com/x?token=[REDACTED]"
        );
        assert_eq!(
            v["both"]["url"], "https://[REDACTED]@api.example.com/e?access_token=[REDACTED]#frag",
            "userinfo and query credentials are independent carriers; both go"
        );
        assert_eq!(
            v["harmless"]["url"], "https://api.example.com/events?region=eu&page=2",
            "a URL with no credential must come back byte-identical"
        );
    }

    /// Redact a genuine `AppConfig`, not a hand-written JSON literal.
    ///
    /// Every other test in this module asserts against a fixture somebody typed, which
    /// is why the flat-shape drift went unnoticed for so long. This one serialises the
    /// real struct through the real snapshot function, so a schema change that moves a
    /// secret shows up here instead of in production.
    #[test]
    fn redacts_a_real_app_config() {
        const SECRET: &str = "unmistakable-secret-value-9f3a";

        let dir = tempfile::tempdir().expect("tempdir");
        let mut config = crate::commands::minimal_config_for_tests(
            dir.path().to_path_buf(),
            crate::config::schema::StateBackend::LocalFs,
        );

        // Three different carriers, three different rules: a `SecretString` (redacted by
        // its own `Serialize`), a header map entry (redacted by name within
        // `sink.headers`), and a URL userinfo (redacted by value). If any one rule
        // regresses, this fails.
        config.sink = serde_json::from_value(serde_json::json!({
            "type": "http",
            "url": format!("https://svc:{SECRET}@api.example.com/events"),
            "bearer_token": SECRET,
            "headers": { "authorization": format!("Bearer {SECRET}") },
        }))
        .expect("http sink config");

        let snapshot = redact_secrets(
            &serde_json::to_string_pretty(&config).expect("serialise the real config"),
        );

        assert!(
            !snapshot.contains(SECRET),
            "a secret from the real AppConfig survived redaction:\n{snapshot}"
        );
        assert!(
            snapshot.contains("[REDACTED]"),
            "the snapshot must show that redaction ran at all"
        );
    }

    /// Hyphenated and dotted spellings are the same secret.
    ///
    /// The token list is written with underscores, so `x-api-key` matched nothing until
    /// separators were normalised — and the exact-match header list was the only thing
    /// covering that spelling, leaving every other surface open. A property test that
    /// *generates* parameter names caught it; the enumerated tests here never would have,
    /// because whoever writes them writes the spelling they were already thinking of.
    #[test]
    fn separator_spelling_does_not_change_whether_a_name_is_secret() {
        for name in [
            "api_key",
            "api-key",
            "API-KEY",
            "x-api-key",
            "x.api.key",
            "private-key",
            "client-secret",
            "auth-token",
        ] {
            assert!(
                should_redact_key(&[], name),
                "'{name}' must be recognised as a secret whatever separator it uses"
            );
        }

        for name in ["region", "page", "table_name", "keyspace", "monkey"] {
            assert!(
                !should_redact_key(&[], name),
                "'{name}' is not a secret and must survive"
            );
        }
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
