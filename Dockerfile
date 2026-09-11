# syntax=docker/dockerfile:1.10
# ─────────────────────────────────────────────────────────────────────────────
# rustcdc-server — multi-stage, multi-arch container image
#
# Built from the workspace root: the binary lives in `crates/rustcdc-server/` and depends on the
# library at `.`, so the build context has to span both.
#
# Stages
#   chef      – Rust toolchain + cargo-chef (shared base)
#   planner   – generate dependency recipe (cache key)
#   builder   – cook deps, compile release binary
#   runtime   – distroless/cc nonroot (glibc only, no shell, no package mgr)
#
# Targets
#   linux/amd64   (x86_64-unknown-linux-gnu)
#   linux/arm64   (aarch64-unknown-linux-gnu)
#
# Build-time requirements compiled inside the builder stage:
#   cmake, clang, perl   — needed by aws-lc-sys (C crypto library)
#   ca-certificates      — fetched via cargo during build if any deps do so
#
# Runtime requirements (satisfied by distroless/cc-debian12):
#   glibc, libgcc        — dynamic runtime for the binary
#   /etc/ssl/certs       — CA bundle (included in the distroless image)
# ─────────────────────────────────────────────────────────────────────────────

ARG RUST_VERSION=1.94.1
ARG DEBIAN_CODENAME=bookworm

# ── Stage 1: install cargo-chef onto the Rust toolchain image ────────────────
FROM rust:${RUST_VERSION}-slim-${DEBIAN_CODENAME} AS chef

# Install cargo-chef for reproducible, cache-friendly dependency cooking.
# --locked pins to the version in cargo-chef's own Cargo.lock.
RUN cargo install cargo-chef --locked

WORKDIR /build

# ── Stage 2: generate the dependency recipe ──────────────────────────────────
# Produces recipe.json — the dependency graph with none of this repository's own
# source in it. The expensive "cook" layer below is keyed on that file's *content*,
# so it stays cached across every source change that does not touch a dependency.
#
# `COPY . .` rather than an enumerated list. This is a workspace now, and
# `cargo chef prepare` has to resolve every member under `crates/` plus
# every target each manifest declares: `cargo metadata` silently drops a target whose
# source file is absent, so a missed directory produces a recipe that omits a
# `[[bench]]` the cook stage then cannot reconstruct ("can't find `pipeline` bench").
# Enumerating the tree was already fragile with one member; with three it is a
# standing trap, and `.dockerignore` keeps the context small.
FROM chef AS planner

COPY . .

RUN cargo chef prepare --recipe-path recipe.json

# ── Stage 3: compile release binary ──────────────────────────────────────────
FROM chef AS builder

# ── Build-time C toolchain (required by aws-lc-sys) ──────────────────────────
# aws-lc-sys vendors the AWS-LC C library and compiles it from source.
# cmake + clang are needed for the C build; perl for generated assembly.
# We pin apt to a single RUN to minimise layer count and image bloat.
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
        cmake \
        clang \
        perl \
        pkg-config \
        ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# ── Pre-build all dependencies (the "cook" layer) ────────────────────────────
# This layer is reused on every subsequent build as long as the recipe and
# toolchain have not changed, even when application source files change.
COPY --from=planner /build/recipe.json recipe.json

RUN cargo chef cook \
        --release \
        --all-features \
        --recipe-path recipe.json

# ── Build the application binary ──────────────────────────────────────────────
# Only invalidated when source code outside of Cargo.toml / Cargo.lock changes.
COPY . .

# Release profile tuning (additive to Cargo.toml defaults).
#   strip=debuginfo  — remove DWARF from the binary (-20 % typical)
#   lto=thin         — cross-crate inlining without full LTO compile cost
#   codegen-units=1  — single codegen unit for maximum inlining
ENV CARGO_PROFILE_RELEASE_STRIP=debuginfo \
    CARGO_PROFILE_RELEASE_LTO=thin \
    CARGO_PROFILE_RELEASE_CODEGEN_UNITS=1

# `-p rustcdc-server`: the workspace root package is the library, and `--bin rustcdc`
# alone would leave cargo resolving the binary across every default member.
RUN cargo build \
        -p rustcdc-server \
        --release \
        --bin rustcdc \
        --all-features \
        --locked

# Verify the binary is dynamically linked only against glibc / libgcc
# (no libssl, no libcrypto — we use rustls + aws-lc-rs which are statically
# linked into the binary by Cargo).
RUN ldd target/release/rustcdc \
 && ! ldd target/release/rustcdc | grep -q 'libssl\|libcrypto' \
 || (echo "ERROR: binary links against libssl/libcrypto — check feature flags" && exit 1)

# ── Stage 4: minimal runtime image ───────────────────────────────────────────
# gcr.io/distroless/cc-debian12:nonroot provides:
#   • glibc + libgcc  (C runtime for the Rust binary)
#   • /etc/ssl/certs  (CA bundle — required for outbound TLS to sinks)
#   • No shell, no package manager, no utilities → minimal attack surface
#   • Runs as uid=65532 (nonroot) by default
FROM gcr.io/distroless/cc-debian12:nonroot AS runtime

# ── OCI image annotations (populated by docker/metadata-action in CI) ────────
LABEL org.opencontainers.image.title="rustcdc-server" \
      org.opencontainers.image.description="Change Data Capture server" \
      org.opencontainers.image.url="https://github.com/hupe1980/rustcdc" \
      org.opencontainers.image.source="https://github.com/hupe1980/rustcdc" \
      org.opencontainers.image.licenses="MIT OR Apache-2.0" \
      org.opencontainers.image.vendor="hupe1980"

# Licence texts. The image is redistributed, and the `licenses` label above is an SPDX
# expression, not a substitute for the grants themselves.
COPY LICENSE-MIT LICENSE-APACHE /usr/share/licenses/rustcdc-server/

# Copy the statically-complete release binary from the builder stage.
COPY --from=builder /build/target/release/rustcdc /usr/local/bin/rustcdc

# Admin API default port. Override via config or --admin-bind flag.
EXPOSE 8080

# The binary is the sole entrypoint — no shell wrapper, no tini required
# (Rust signal handling is correct; tokio propagates SIGTERM).
ENTRYPOINT ["/usr/local/bin/rustcdc"]

# Default subcommand. Operators mount their config at /etc/rustcdc/config.toml
# or override CMD / pass flags via the Kubernetes args[] field.
CMD ["run", "--config-file", "/etc/rustcdc/config.toml"]
