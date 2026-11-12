//! The console at `/ui`: what it is served with, what the API keeps, and the off switch.
#![cfg(feature = "ui")]

use std::net::SocketAddr;

use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use rustcdc_server::admin::AdminState;
use tower::ServiceExt;

async fn admin_state(dir: &std::path::Path) -> AdminState {
    let config_toml = format!(
        r#"api_version = "v1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc"
password = {{ env = "CDC_TEST_SOURCE_PASSWORD" }}
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
table_include_list = ["public.orders"]

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "{state}"
"#,
        state = dir.join("state").display(),
    );
    let path = dir.join("cdc.toml");
    std::fs::write(&path, config_toml).expect("write config");
    let config = rustcdc_server::config::load(&path).expect("config must load");
    AdminState::new(&config).await.expect("admin state")
}

struct Answer {
    status: StatusCode,
    csp: String,
    cache: String,
    body: String,
}

async fn get(console: bool, path: &str) -> Answer {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut request = Request::builder().uri(path).body(Body::empty()).unwrap();
    request.extensions_mut().insert(axum::extract::ConnectInfo(
        "203.0.113.7:54321".parse::<SocketAddr>().unwrap(),
    ));
    let response = rustcdc_server::admin::router(admin_state(dir.path()).await, console)
        .oneshot(request)
        .await
        .expect("router responds");
    let header = |name| {
        response
            .headers()
            .get(name)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_string()
    };
    let (status, csp, cache) = (
        response.status(),
        header(header::CONTENT_SECURITY_POLICY),
        header(header::CACHE_CONTROL),
    );
    let bytes = axum::body::to_bytes(response.into_body(), 1 << 22)
        .await
        .unwrap_or_default();
    Answer {
        status,
        csp,
        cache,
        body: String::from_utf8_lossy(&bytes).into_owned(),
    }
}

#[tokio::test]
async fn the_console_is_served_from_this_origin_only_and_never_framed() {
    let answer = get(true, "/ui").await;
    assert_eq!(answer.status, StatusCode::OK);
    assert!(answer.csp.contains("default-src 'self'"), "{}", answer.csp);
    assert!(answer.csp.contains("connect-src 'self'"), "{}", answer.csp);
    assert!(
        answer.csp.contains("frame-ancestors 'none'"),
        "{}",
        answer.csp
    );
    assert_eq!(
        answer.cache, "no-cache",
        "index.html must name the current build's files"
    );
}

/// A release build compiles the bundle in; every other build explains how to.
#[tokio::test]
async fn a_release_build_serves_the_console_itself() {
    let answer = get(true, "/ui/").await;
    if option_env!("RUSTCDC_REQUIRE_CONSOLE") == Some("1") {
        assert!(
            !answer.body.contains("not built"),
            "the bundle must be embedded"
        );
        assert!(answer.body.contains("<script"), "{}", answer.body);
    } else {
        assert!(answer.body.contains("cargo xtask console") || answer.body.contains("<script"));
    }
}

#[tokio::test]
async fn api_responses_are_data_never_a_page() {
    let answer = get(true, "/healthz").await;
    assert!(answer.csp.contains("default-src 'none'"), "{}", answer.csp);
    let answer = get(true, "/openapi.json").await;
    assert!(answer.csp.contains("default-src 'none'"), "{}", answer.csp);
}

#[tokio::test]
async fn only_embedded_files_are_served() {
    for path in [
        "/ui/../Cargo.toml",
        "/ui/missing.js",
        "/ui/%2e%2e%2fCargo.toml",
    ] {
        assert_eq!(
            get(true, path).await.status,
            StatusCode::NOT_FOUND,
            "{path}"
        );
    }
}

#[tokio::test]
async fn switching_it_off_removes_the_routes() {
    assert_eq!(get(false, "/ui").await.status, StatusCode::NOT_FOUND);
    assert_eq!(get(false, "/healthz").await.status, StatusCode::OK);
}

/// The console words every cause and owner the library routes. A cause added there without a
/// sentence here would still render (the console has a fallback), but as the bare label.
#[test]
fn the_console_has_words_for_every_cause_and_owner() {
    let health =
        std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/ui/src/lib/health.ts"))
            .expect("ui/src/lib/health.ts");
    for cause in rustcdc::core::StallCause::ALL {
        assert!(
            health.contains(&format!("  {}: {{", cause.as_str())),
            "no sentence for {}",
            cause.as_str()
        );
        let owner = cause.owner().as_str();
        assert!(
            health.contains(&format!("  {owner}: {{ hop:")),
            "no wording for owner {owner}"
        );
    }
}

/// Each cause links a runbook section by its anchor: a heading renamed there must fail here, not
/// leave the remedy's second half pointing nowhere.
#[test]
fn every_runbook_link_in_the_console_lands_on_a_section() {
    let root = env!("CARGO_MANIFEST_DIR");
    let health =
        std::fs::read_to_string(format!("{root}/ui/src/lib/health.ts")).expect("health.ts");
    let runbook = std::fs::read_to_string(format!("{root}/../../site/content/docs/runbook.md"))
        .expect("runbook.md");
    // Zola's anchors: lower case, punctuation dropped, runs of anything else become one '-'.
    let slug = |heading: &str| {
        let kept: String = heading
            .to_lowercase()
            .chars()
            .filter(|c| !"`'\"().,:".contains(*c))
            .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
            .collect();
        kept.split('-')
            .filter(|part| !part.is_empty())
            .collect::<Vec<_>>()
            .join("-")
    };
    let anchors: Vec<String> = runbook
        .lines()
        .filter_map(|line| {
            line.strip_prefix("## ")
                .or_else(|| line.strip_prefix("### "))
        })
        .map(slug)
        .collect();
    let sections: Vec<&str> = health
        .lines()
        .filter_map(|line| line.trim().strip_prefix("section: \""))
        .filter_map(|rest| rest.split('"').next())
        .collect();
    assert_eq!(
        sections.len(),
        rustcdc::core::StallCause::ALL.len(),
        "one section per cause"
    );
    for section in sections {
        assert!(
            anchors.iter().any(|a| a == section),
            "no runbook section #{section}: {anchors:?}"
        );
    }
}
