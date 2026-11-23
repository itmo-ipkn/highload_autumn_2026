FROM rust:1-slim-trixie AS builder
WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY src ./src

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    cargo build --release --locked \
    && cp target/release/dashboard-generator /usr/local/bin/dashboard-generator

FROM debian:trixie-slim
RUN useradd --system --no-create-home app
USER app

COPY --from=builder /usr/local/bin/dashboard-generator /usr/local/bin/dashboard-generator

CMD ["dashboard-generator"]
