FROM ghcr.io/userver-framework/ubuntu-24.04-userver:v3.2

WORKDIR /app
COPY CMakeLists.txt main.cpp ./
RUN cmake -S . -B build -DCMAKE_BUILD_TYPE=Release \
    && cmake --build build --parallel 2
COPY static_config.yaml ./

USER 1000:1000
ENTRYPOINT ["./build/producer", "--config", "static_config.yaml"]
