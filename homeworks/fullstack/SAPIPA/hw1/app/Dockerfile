FROM ghcr.io/userver-framework/ubuntu-24.04-userver:v3.2 AS builder

WORKDIR /workspace

COPY . .

RUN cmake --preset release \
    && cmake --build build-release \
       --parallel 4 --target app


FROM ghcr.io/userver-framework/ubuntu-24.04-userver:v3.2

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/app

COPY --from=builder /workspace/build-release/app ./app
COPY --from=builder /workspace/configs/ ./configs/

USER 1000:1000

EXPOSE 8080 8081

ENTRYPOINT ["./app", "--config", "configs/static_config.yaml", "--config_vars", "configs/config_vars.yaml"]
