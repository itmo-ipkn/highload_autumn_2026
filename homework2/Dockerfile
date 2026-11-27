FROM ghcr.io/userver-framework/ubuntu-22.04-userver:v3.2

WORKDIR /app/homework
COPY CMakeLists.txt ./
COPY src/ ./src/
RUN cmake -S . -B /tmp/homework-build -G Ninja -DCMAKE_BUILD_TYPE=Release -DUSERVER_FEATURE_TESTSUITE=OFF \
    && cmake --build /tmp/homework-build --parallel 2 \
    && install /tmp/homework-build/homework-producer /tmp/homework-build/homework-consumer /usr/local/bin/ \
    && rm -rf /tmp/homework-build

COPY config/ ./config/
