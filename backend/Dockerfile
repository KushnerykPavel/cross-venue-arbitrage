FROM rust:1.98-slim-bookworm AS builder

ARG GIT_COMMIT=unknown
ARG GIT_DIRTY=true

WORKDIR /app

COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY apps ./apps
COPY crates ./crates

RUN GIT_COMMIT="$GIT_COMMIT" GIT_DIRTY="$GIT_DIRTY" \
    cargo build --locked --release -p trader -p replay -p export-parquet

FROM debian:bookworm-slim AS runtime

RUN useradd --system --uid 10001 --no-create-home app

COPY --from=builder /app/target/release/trader /usr/local/bin/trader
COPY --from=builder /app/target/release/replay /usr/local/bin/replay
COPY --from=builder /app/target/release/export-parquet /usr/local/bin/export-parquet

USER app

CMD ["trader"]
