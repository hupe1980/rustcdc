# syntax=docker/dockerfile:1.10
# ─────────────────────────────────────────────────────────────────────────────
# Demo-only Dockerfile — fast local build (debug binary, no cargo-chef).
# NOT suitable for production. Use the workspace-root Dockerfile for production images.
# ─────────────────────────────────────────────────────────────────────────────

ARG RUST_VERSION=1.94.1
ARG DEBIAN_CODENAME=bookworm

FROM rust:${RUST_VERSION}-slim-${DEBIAN_CODENAME} AS builder

# aws-lc-sys requires cmake, clang, and perl at compile time.
RUN apt-get update \
    && apt-get install -y --no-install-recommends cmake clang perl pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# The whole workspace: the server binary links the library at the workspace root, and
# the library embeds `README.md` and `site/content/docs/*.md` with `include_str!`.
#
# No dependency-only pre-build stage here. The stub-`main.rs` trick cannot work across a
# workspace — cargo would need a skeleton for every member and every declared target —
# and this image exists to be rebuilt quickly on a developer's machine, where the cargo
# layer cache already does the job. The production image in the repository root uses
# cargo-chef, which does handle workspaces.
COPY . .
RUN cargo build -p rustcdc-server --bin rustcdc --all-features

# Self-signed TLS cert for the admin API (demo only — the server refuses a
# non-loopback admin bind without TLS, and the demo port-maps admin to the host).
RUN apt-get update \
    && apt-get install -y --no-install-recommends openssl \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /build/tls \
    && openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 \
         -keyout /build/tls/admin-key.pem -out /build/tls/admin-cert.pem \
         -days 3650 -nodes -subj "/CN=localhost" \
         -addext "subjectAltName=DNS:localhost,DNS:rustcdc,IP:127.0.0.1"

# ── Runtime ───────────────────────────────────────────────────────────────────
# Use debian-slim (has glibc + shell) so we can run shell-based healthchecks.
FROM debian:${DEBIAN_CODENAME}-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/debug/rustcdc /usr/local/bin/rustcdc
COPY --from=builder /build/tls /etc/rustcdc/tls

EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/rustcdc"]
