FROM rust:1.88-bookworm AS builder

WORKDIR /app

COPY . .

RUN cargo build --release --example pg_to_stdout --features postgres

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /var/lib/cdc-rs

COPY --from=builder /app/target/release/examples/pg_to_stdout /usr/local/bin/pg_to_stdout

ENV RUST_LOG=info
ENTRYPOINT ["/usr/local/bin/pg_to_stdout"]
