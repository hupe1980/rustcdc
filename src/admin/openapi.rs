//! The admin API's machine-readable contract, served at `GET /openapi.json`.
//!
//! # Why this is generated rather than a file in the repo
//!
//! The control plane is a product surface — ten endpoints, two token scopes, a signal
//! vocabulary, an SSE stream — and it was documented only in prose. A consumer could not
//! generate a client, and nothing checked that the prose still matched the router.
//!
//! A hand-written `openapi.yaml` beside the source would have the same problem one commit
//! later. So the document is built here, in the same crate as the handlers, and
//! `tests/architecture.rs::every_admin_route_appears_in_the_openapi_document` fails the
//! build if a route is added to the router without a path here. That check is the reason
//! this is worth having: a spec nobody verifies is a spec that lies.
//!
//! # Why hand-built JSON rather than a derive macro
//!
//! `utoipa` and friends generate this from annotations, which is less code and one more
//! dependency tree on a binary that already links two TLS stacks under `--all-features`.
//! The whole document is ~400 lines of `serde_json::json!`, it needs no build step, and
//! the drift risk it would otherwise carry is covered by the architecture test.
//!
//! # Scope
//!
//! OpenAPI 3.1. Response bodies are described at the level a client generator needs —
//! shapes and required fields — rather than exhaustively: `/status` alone carries several
//! dozen fields whose names are already asserted by the handler tests, and duplicating them
//! here would create a second thing to keep in step for no gain.

use serde_json::{json, Value};

/// Every path the admin router serves, in the order the router declares them.
///
/// A plain list so `tests/architecture.rs` can diff it against `router()` by reading this
/// file as text, without building the JSON document or linking the crate. The unit test
/// below pins it to what [`document`] actually emits, so the two cannot drift: the
/// architecture test checks list-vs-router, and the unit test checks list-vs-document.
///
/// `#[cfg(test)]` because nothing at runtime consults it — the document's paths are
/// written in the `json!` block. The architecture test reads source text, so the gate does
/// not hide it.
#[cfg(test)]
pub(crate) const DOCUMENTED_PATHS: &[&str] = &[
    "/healthz",
    "/livez",
    "/readyz",
    "/status",
    "/config",
    "/metrics",
    "/signals",
    "/notifications",
    "/notifications/cloudevents",
    "/notifications/stream",
    "/openapi.json",
];

/// The document, serialised once for the process lifetime.
///
/// It depends only on `CARGO_PKG_VERSION`, so it is constant — there was never a reason to
/// rebuild the `json!` tree and re-serialise ~10 KB on every request, and doing so on an
/// unauthenticated route made request cost something a caller could choose.
///
/// `Bytes` rather than `String`, because a `Bytes` clone is a refcount bump: the response
/// body borrows the cached buffer instead of copying 10 KB per request. Returning an
/// `Arc<str>` and calling `.to_string()` at the call site would have reinstated exactly the
/// copy this exists to remove.
pub(crate) fn cached_document() -> bytes::Bytes {
    static DOCUMENT: std::sync::OnceLock<bytes::Bytes> = std::sync::OnceLock::new();
    DOCUMENT
        .get_or_init(|| bytes::Bytes::from(document(env!("CARGO_PKG_VERSION")).to_string()))
        .clone()
}

/// Build the OpenAPI 3.1 document.
///
/// `version` is the crate version, so a generated client can tell which build it was made
/// against.
pub(crate) fn document(version: &str) -> Value {
    json!({
        "openapi": "3.1.0",
        "info": {
            "title": "rustcdc-server admin API",
            "version": version,
            "summary": "Health, observability and control plane for a running CDC pipeline.",
            "description": concat!(
                "Two token scopes: **read** and **write**. Read covers observation ",
                "(`/status`, `/config`, `/metrics`, the notification endpoints); write covers ",
                "the one endpoint that changes what the pipeline is doing (`/signals`).\n\n",
                "`/healthz` and `/livez` are unauthenticated by design — a liveness probe that ",
                "needs a credential fails closed for the wrong reason. `/readyz` is ",
                "configurable via `admin.probe_auth_mode`.\n\n",
                "`/config` returns the parsed configuration with every secret redacted. ",
                "Redaction is enforced by property tests rather than by a field allowlist, ",
                "because an allowlist only covers the fields somebody remembered."
            ),
            "license": { "name": "MIT OR Apache-2.0" }
        },
        "servers": [
            { "url": "http://localhost:8080", "description": "Default bind address" }
        ],
        "tags": [
            { "name": "health", "description": "Kubernetes-style probes" },
            { "name": "observability", "description": "State, configuration and metrics" },
            { "name": "control", "description": "Operations that change pipeline behaviour" },
            { "name": "notifications", "description": "Signal lifecycle events" }
        ],
        "components": {
            "securitySchemes": {
                "readToken": {
                    "type": "http",
                    "scheme": "bearer",
                    "description": "Read-scope bearer token (`admin.read_token_env`)."
                },
                "writeToken": {
                    "type": "http",
                    "scheme": "bearer",
                    "description": concat!(
                        "Write-scope bearer token (`admin.write_token_env`). This token can ",
                        "start and stop backfills; treat it as an operator credential."
                    )
                }
            },
            "responses": {
                "Unauthorized": {
                    "description": "Missing, malformed, expired, revoked or wrong-scope token.",
                    "content": { "text/plain": { "schema": { "type": "string" } } }
                },
                "RateLimited": {
                    "description": concat!(
                        "Per-client rate limit exceeded. Applies to `/readyz`, `/status` and ",
                        "`/metrics`, which are the endpoints an unauthenticated or ",
                        "read-scoped client can reach most cheaply."
                    ),
                    "content": { "text/plain": { "schema": { "type": "string" } } }
                }
            },
            "schemas": {
                "SignalRequest": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": ["action_type"],
                    "properties": {
                        "action_type": { "$ref": "#/components/schemas/SignalActionType" },
                        "signal_id": {
                            "type": "string",
                            "description": concat!(
                                "Idempotency key. Omitted, one is generated — which means a ",
                                "redelivered payload without an id is a *new* signal, so ",
                                "supply one if the sender may retry."
                            )
                        },
                        "correlation_id": { "type": "string" },
                        "message": {
                            "type": "string",
                            "description": "Required for `log_marker`."
                        },
                        "tables": {
                            "type": "array",
                            "items": { "type": "string" },
                            "description": concat!(
                                "Fully-qualified `schema.table` names. Required and non-empty ",
                                "for `execute_snapshot`; rejected for every other action."
                            )
                        },
                        "conditions": {
                            "type": "object",
                            "additionalProperties": { "type": "string" },
                            "description": concat!(
                                "Per-table SQL row filter for this request — Debezium's ",
                                "`additional-conditions`. Overrides ",
                                "`incremental_snapshot.table_conditions` for the tables it ",
                                "names. Every key must appear in `tables`, or the request is ",
                                "rejected: an inert filter reads the whole table, and the only ",
                                "symptom is volume.\n\n",
                                "**Raw SQL, and trusted input.** Interpolated into the chunk ",
                                "SELECT; carries the same trust level as the connection ",
                                "string. It is not a tenancy boundary."
                            )
                        },
                        "additional_data": {
                            "description": "Opaque JSON echoed into the audit trail and notifications."
                        }
                    }
                },
                "SignalActionType": {
                    "type": "string",
                    "enum": [
                        "log_marker",
                        "execute_snapshot",
                        "pause_snapshot",
                        "resume_snapshot",
                        "stop_snapshot"
                    ],
                    "description": concat!(
                        "`execute_snapshot` backfills tables on the running pipeline. ",
                        "`pause_snapshot` / `resume_snapshot` suspend and resume chunk ",
                        "reading — the live stream is unaffected. `stop_snapshot` abandons ",
                        "the remaining tables and survives a restart."
                    )
                },
                "SignalAccepted": {
                    "type": "object",
                    "required": ["signal_id", "action_type", "state"],
                    "properties": {
                        "signal_id": { "type": "string" },
                        "correlation_id": { "type": "string" },
                        "action_type": { "$ref": "#/components/schemas/SignalActionType" },
                        "state": {
                            "type": "string",
                            "description": concat!(
                                "`STARTED` for an asynchronous action. The outcome arrives on ",
                                "the audit trail and the notification stream, not in this ",
                                "response — poll `/notifications` or subscribe to ",
                                "`/notifications/stream`."
                            )
                        },
                        "expected_terminal_state": { "type": "string" }
                    }
                },
                "Notification": {
                    "type": "object",
                    "required": ["action", "result", "timestamp"],
                    "properties": {
                        "action": { "type": "string" },
                        "result": { "type": "string" },
                        "state": { "type": "string" },
                        "signal_id": { "type": "string" },
                        "correlation_id": { "type": "string" },
                        "timestamp": { "type": "string", "format": "date-time" }
                    }
                },
                "IncrementalSnapshotProgress": {
                    "type": "object",
                    "description": "Absent entirely when no snapshot is in flight.",
                    "properties": {
                        "snapshot_id": { "type": "string" },
                        "paused": { "type": "boolean" },
                        "stopped": {
                            "type": "boolean",
                            "description": concat!(
                                "Distinct from an empty `tables`: a stopped snapshot must stay ",
                                "stopped across a restart."
                            )
                        },
                        "generation": {
                            "type": "integer",
                            "description": concat!(
                                "Times snapshot work has been requested. What makes a ",
                                "deliberate re-snapshot distinguishable from a replay."
                            )
                        },
                        "tables": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "table": { "type": "string" },
                                    "rows_emitted": { "type": "integer" },
                                    "chunks_emitted": { "type": "integer" },
                                    "is_complete": { "type": "boolean" },
                                    "condition": {
                                        "type": ["string", "null"],
                                        "description": concat!(
                                            "The row filter actually in effect, after merging ",
                                            "a request override over the configuration. Present ",
                                            "so \"did my filter apply?\" is observable rather ",
                                            "than inferred from row counts."
                                        )
                                    }
                                }
                            }
                        }
                    }
                }
            }
        },
        "paths": {
            "/healthz": {
                "get": {
                    "tags": ["health"],
                    "summary": "Liveness — the process is running.",
                    "description": "Unauthenticated by design. Never consults pipeline state.",
                    "security": [],
                    "responses": { "200": { "description": "Alive." } }
                }
            },
            "/livez": {
                "get": {
                    "tags": ["health"],
                    "summary": "Liveness — the pipeline task has not wedged.",
                    "security": [],
                    "responses": {
                        "200": { "description": "Alive." },
                        "503": { "description": "The instance has entered a terminal state." }
                    }
                }
            },
            "/readyz": {
                "get": {
                    "tags": ["health"],
                    "summary": "Readiness — connected to the source and able to deliver.",
                    "description": concat!(
                        "Authentication depends on `admin.probe_auth_mode`: `open`, ",
                        "`loopback` (unauthenticated from loopback only) or `token`."
                    ),
                    "responses": {
                        "200": { "description": "Ready." },
                        "401": { "$ref": "#/components/responses/Unauthorized" },
                        "429": { "$ref": "#/components/responses/RateLimited" },
                        "503": { "description": "Not ready." }
                    }
                }
            },
            "/status": {
                "get": {
                    "tags": ["observability"],
                    "summary": "Pipeline state, SLO indicators and snapshot progress.",
                    "security": [{ "readToken": [] }],
                    "responses": {
                        "200": {
                            "description": "Current state.",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "object",
                                        "properties": {
                                            "state": { "type": "string" },
                                            "incremental_snapshot": {
                                                "$ref": "#/components/schemas/IncrementalSnapshotProgress"
                                            }
                                        }
                                    }
                                }
                            }
                        },
                        "401": { "$ref": "#/components/responses/Unauthorized" },
                        "429": { "$ref": "#/components/responses/RateLimited" }
                    }
                }
            },
            "/config": {
                "get": {
                    "tags": ["observability"],
                    "summary": "The parsed configuration, with secrets redacted.",
                    "description": concat!(
                        "What the process actually loaded, after environment overrides, ",
                        "defaults and migrations — not the file on disk. Redaction covers ",
                        "secret-named keys, URL userinfo and secret-named query parameters, ",
                        "and is checked by property tests rather than a field allowlist."
                    ),
                    "security": [{ "readToken": [] }],
                    "responses": {
                        "200": {
                            "description": "Redacted configuration.",
                            "content": { "application/json": { "schema": { "type": "object" } } }
                        },
                        "401": { "$ref": "#/components/responses/Unauthorized" }
                    }
                }
            },
            "/metrics": {
                "get": {
                    "tags": ["observability"],
                    "summary": "Prometheus exposition.",
                    "security": [{ "readToken": [] }],
                    "responses": {
                        "200": {
                            "description": "Metrics in Prometheus text format.",
                            "content": { "text/plain": { "schema": { "type": "string" } } }
                        },
                        "401": { "$ref": "#/components/responses/Unauthorized" },
                        "429": { "$ref": "#/components/responses/RateLimited" }
                    }
                }
            },
            "/signals": {
                "post": {
                    "tags": ["control"],
                    "summary": "Send a control signal to the running pipeline.",
                    "description": concat!(
                        "Asynchronous: the response says the signal was accepted, not that ",
                        "the action completed. Watch `/notifications/stream` for the terminal ",
                        "state.\n\n",
                        "Unknown fields are rejected with 400 over HTTP and through the file ",
                        "and Kafka ingress channels alike, so a mistyped key fails rather than ",
                        "being silently dropped."
                    ),
                    "security": [{ "writeToken": [] }],
                    "requestBody": {
                        "required": true,
                        "content": {
                            "application/json": {
                                "schema": { "$ref": "#/components/schemas/SignalRequest" }
                            }
                        }
                    },
                    "responses": {
                        "200": {
                            "description": "Accepted.",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/SignalAccepted" }
                                }
                            }
                        },
                        "400": { "description": "Malformed payload, unknown field, or a filter that could never apply." },
                        "401": { "$ref": "#/components/responses/Unauthorized" },
                        "409": { "description": "This signal id and action are already in flight or terminal." },
                        "503": { "description": "The pipeline cannot service this action — e.g. no incremental snapshot configured." }
                    }
                }
            },
            "/notifications": {
                "get": {
                    "tags": ["notifications"],
                    "summary": "Recent signal lifecycle notifications.",
                    "security": [{ "readToken": [] }],
                    "responses": {
                        "200": {
                            "description": "A bounded, in-memory ring of recent notifications.",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "array",
                                        "items": { "$ref": "#/components/schemas/Notification" }
                                    }
                                }
                            }
                        },
                        "401": { "$ref": "#/components/responses/Unauthorized" }
                    }
                }
            },
            "/notifications/cloudevents": {
                "get": {
                    "tags": ["notifications"],
                    "summary": "The same notifications, as CloudEvents.",
                    "security": [{ "readToken": [] }],
                    "responses": {
                        "200": {
                            "description": "CloudEvents 1.0 structured-mode envelopes.",
                            "content": { "application/json": { "schema": { "type": "array", "items": { "type": "object" } } } }
                        },
                        "401": { "$ref": "#/components/responses/Unauthorized" }
                    }
                }
            },
            "/notifications/stream": {
                "get": {
                    "tags": ["notifications"],
                    "summary": "Server-sent events for signal lifecycle notifications.",
                    "description": concat!(
                        "The ring buffer behind `/notifications` holds a bounded history, so a ",
                        "consumer that must not miss a terminal state should subscribe here ",
                        "rather than poll."
                    ),
                    "security": [{ "readToken": [] }],
                    "responses": {
                        "200": {
                            "description": "An SSE stream.",
                            "content": { "text/event-stream": { "schema": { "type": "string" } } }
                        },
                        "401": { "$ref": "#/components/responses/Unauthorized" }
                    }
                }
            },
            "/openapi.json": {
                "get": {
                    "tags": ["observability"],
                    "summary": "This document.",
                    "description": concat!(
                        "Unauthenticated: it describes the shape of the API, not its state. ",
                        "Requiring a credential to discover how to authenticate is a loop."
                    ),
                    "security": [],
                    "responses": {
                        "200": {
                            "description": "OpenAPI 3.1 document.",
                            "content": { "application/json": { "schema": { "type": "object" } } }
                        }
                    }
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Deliberately not the crate version.
    ///
    /// These tests check that the argument is threaded into `info.version` and that the
    /// document's shape is right — not what the version happens to be. A literal that
    /// looked like the real version invited exactly the reflex bump this avoids, and would
    /// have read as authoritative to anyone grepping for the project's version.
    const TEST_VERSION: &str = "0.0.0-test";

    /// The document must be valid enough for a generator to consume.
    #[test]
    fn the_document_has_the_structure_a_client_generator_needs() {
        let doc = document(TEST_VERSION);
        assert_eq!(doc["openapi"], "3.1.0");
        assert_eq!(doc["info"]["version"], TEST_VERSION);

        let paths = doc["paths"].as_object().expect("paths is an object");
        for path in DOCUMENTED_PATHS {
            assert!(
                paths.contains_key(*path),
                "{path} missing from the document"
            );
        }
        assert_eq!(
            paths.len(),
            DOCUMENTED_PATHS.len(),
            "DOCUMENTED_PATHS and the document's paths must agree"
        );
    }

    /// Every `$ref` must resolve. A dangling one breaks generators at the worst moment.
    #[test]
    fn every_ref_resolves() {
        let doc = document(TEST_VERSION);
        let mut refs = Vec::new();
        collect_refs(&doc, &mut refs);
        assert!(!refs.is_empty(), "the document should use refs at all");

        for reference in refs {
            let path = reference
                .strip_prefix("#/")
                .unwrap_or_else(|| panic!("only local refs are supported: {reference}"));
            let mut node = &doc;
            for segment in path.split('/') {
                node = node
                    .get(segment)
                    .unwrap_or_else(|| panic!("dangling $ref: {reference}"));
            }
        }
    }

    fn collect_refs(value: &Value, out: &mut Vec<String>) {
        match value {
            Value::Object(map) => {
                for (key, child) in map {
                    if key == "$ref" {
                        if let Some(text) = child.as_str() {
                            out.push(text.to_string());
                        }
                    } else {
                        collect_refs(child, out);
                    }
                }
            }
            Value::Array(items) => items.iter().for_each(|item| collect_refs(item, out)),
            _ => {}
        }
    }

    /// The write scope must be required exactly where the pipeline's behaviour changes.
    ///
    /// A spec that understates an endpoint's scope is worse than no spec: it invites a
    /// consumer to hand a read token to something that needs a write one, and discover it
    /// in production.
    #[test]
    fn only_signals_requires_the_write_token() {
        let doc = document(TEST_VERSION);
        let paths = doc["paths"].as_object().expect("paths");

        for (path, item) in paths {
            for (method, operation) in item.as_object().expect("path item") {
                let security = operation["security"].to_string();
                let expects_write = path == "/signals";
                assert_eq!(
                    security.contains("writeToken"),
                    expects_write,
                    "{method} {path} declares security {security}"
                );
            }
        }
    }
}
