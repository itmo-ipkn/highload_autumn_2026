FROM ghcr.io/userver-framework/ubuntu-24.04-userver:v3.2

WORKDIR /app
COPY CMakeLists.txt main.cpp kafka_consumer.hpp kafka_consumer.cpp sink.hpp sink.cpp ./
RUN cmake -S . -B build -DCMAKE_BUILD_TYPE=Release \
    && cmake --build build --parallel 2 \
    && install -d -o 1000 -g 1000 /app/state
COPY static_config.yaml ./

USER 1000:1000
ENTRYPOINT ["./build/consumer", "--config", "static_config.yaml"]
