FROM rust:1-bookworm AS builder
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY crates crates
RUN cargo build --release

FROM debian:bookworm-slim
# hadolint ignore=DL3008
RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system halephant \
    && useradd --system --gid halephant --no-create-home halephant
COPY --from=builder --chmod=0555 /build/target/release/halephant /usr/local/bin/halephant
COPY --chmod=0444 examples/docker.toml /etc/halephant/halephant.toml
USER halephant
EXPOSE 6432 6433
HEALTHCHECK --interval=10s --timeout=2s --retries=3 --start-period=5s --start-interval=1s \
    CMD curl -sf http://127.0.0.1:6433/ready || exit 1

ENTRYPOINT ["halephant", "-c", "/etc/halephant/halephant.toml"]
