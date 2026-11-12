//! The operator console at `/ui`: `ui/dist`, embedded at compile time.
//!
//! It holds no authority. Every request it makes carries the operator's admin token and is
//! authorised like any client's; with no token configured it can do exactly what `curl` can.
//! Its files get their own CSP (this origin only, never framed). A file is served by its exact
//! name or not at all.

use axum::Router;
use axum::extract::Path;
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;

mod bundle {
    include!(concat!(env!("OUT_DIR"), "/console_bundle.rs"));
}

/// What the console may load and where it may send: this origin, and nothing else.
pub(crate) const CONTENT_SECURITY_POLICY: &str = "default-src 'self'; script-src 'self'; \
     style-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src 'self'; font-src 'self'; \
     object-src 'none'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'";

/// Shown by a build compiled without the bundle. A build that ships refuses to compile instead
/// (`RUSTCDC_REQUIRE_CONSOLE=1`, see `build.rs`).
const NOT_BUILT: &str = "<!doctype html><meta charset=utf-8><title>rustcdc</title>\
<body style=\"font:15px/1.5 -apple-system,system-ui,sans-serif;max-width:36rem;margin:15vh auto;padding:0 1rem\">\
<h1 style=\"font-weight:600\">The console is not built</h1>\
<p>This binary was compiled without it. Build it, then rebuild the server:</p>\
<pre style=\"background:#f2f2f7;padding:12px;border-radius:8px\">cargo xtask console\ncargo build -p rustcdc-server</pre></body>";

/// The `/ui` routes.
pub(crate) fn routes() -> Router {
    Router::new()
        .route("/ui", get(index))
        .route("/ui/", get(index))
        .route("/ui/{file}", get(file))
}

async fn index() -> Response {
    match find("index.html") {
        Some(bytes) => serve("index.html", bytes),
        None => serve("index.html", NOT_BUILT.as_bytes()),
    }
}

async fn file(Path(name): Path<String>) -> Response {
    match find(&name) {
        Some(bytes) => serve(&name, bytes),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

fn find(name: &str) -> Option<&'static [u8]> {
    bundle::BUNDLE
        .iter()
        .find(|(file, _)| *file == name)
        .map(|(_, bytes)| *bytes)
}

fn serve(name: &str, bytes: &'static [u8]) -> Response {
    (
        [
            (
                header::CONTENT_SECURITY_POLICY,
                HeaderValue::from_static(CONTENT_SECURITY_POLICY),
            ),
            (
                header::CONTENT_TYPE,
                HeaderValue::from_static(content_type(name)),
            ),
            (
                header::CACHE_CONTROL,
                HeaderValue::from_static(cache_control(name)),
            ),
        ],
        bytes,
    )
        .into_response()
}

/// `index.html` is revalidated every time, so it names the current build's files. Every other
/// file carries a content hash in its name, so it can be cached for good and an old one can never
/// mix into a new console after an upgrade.
fn cache_control(name: &str) -> &'static str {
    if name == "index.html" {
        "no-cache"
    } else {
        "public, max-age=31536000, immutable"
    }
}

fn content_type(name: &str) -> &'static str {
    match name.rsplit('.').next() {
        Some("html") => "text/html; charset=utf-8",
        Some("js") => "text/javascript; charset=utf-8",
        Some("css") => "text/css; charset=utf-8",
        Some("svg") => "image/svg+xml",
        Some("png") => "image/png",
        Some("webp") => "image/webp",
        Some("json") => "application/json",
        Some("woff2") => "font/woff2",
        _ => "application/octet-stream",
    }
}
