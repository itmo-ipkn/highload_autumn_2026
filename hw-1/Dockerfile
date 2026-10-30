FROM rust:1-slim-trixie AS builder
WORKDIR /app

ENV SQLX_OFFLINE=true

COPY Cargo.toml Cargo.lock ./
COPY .sqlx ./.sqlx
COPY migrations ./migrations
COPY src ./src

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    cargo build --release --locked \
    && cp target/release/simple-crud /usr/local/bin/simple-crud

FROM debian:trixie-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

RUN useradd --system --no-create-home app
USER app

COPY --from=builder /usr/local/bin/simple-crud /usr/local/bin/simple-crud

EXPOSE 3000
CMD ["simple-crud"]
