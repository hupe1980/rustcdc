# syntax=docker/dockerfile:1.10
# ─────────────────────────────────────────────────────────────────────────────
# Demo-only Dockerfile — fast local build (debug binary, no cargo-chef).
# NOT suitable for production. Use the root Dockerfile for production images.
# ─────────────────────────────────────────────────────────────────────────────

ARG RUST_VERSION=1.93
ARG DEBIAN_CODENAME=bookworm

FROM rust:${RUST_VERSION}-slim-${DEBIAN_CODENAME} AS builder

# aws-lc-sys requires cmake, clang, and perl at compile time.
RUN apt-get update \
    && apt-get install -y --no-install-recommends cmake clang perl pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Cache dependencies first — only rebuilt when Cargo.toml / Cargo.lock change.
COPY Cargo.toml Cargo.lock ./
RUN mkdir -p src && echo 'fn main(){}' > src/main.rs \
    && cargo build --all-features \
    && rm -rf src

# Build the real binary (debug — fast, incremental).
COPY src src
COPY build.rs ./
RUN cargo build --all-features

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
