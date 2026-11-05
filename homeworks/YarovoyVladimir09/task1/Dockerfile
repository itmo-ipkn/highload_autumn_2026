# The same immutable userver image as the development container.
# This image is amd64; Compose selects the platform explicitly on ARM hosts.
FROM ghcr.io/userver-framework/ubuntu-22.04-userver@sha256:c7e971fee07af30fa479de4634974e763c76e27e0a2db41770331003dcad777d AS build
WORKDIR /src
COPY CMakeLists.txt ./
COPY src ./src
RUN cmake -S . -B /build -DCMAKE_BUILD_TYPE=Release \
    && cmake --build /build --target my_service --parallel 1

# Copy the executable, curl for the healthcheck, and their transitive libraries.
# All libraries come from the pinned builder; no unpinned apt installation.
RUN set -eu; \
    mkdir -p /runtime/app /runtime/usr/bin /runtime/etc/ssl/certs /runtime/tmp; \
    chmod 1777 /runtime/tmp; \
    strip /build/my_service; \
    cp /build/my_service /runtime/app/my_service; \
    cp /usr/bin/curl /runtime/usr/bin/curl; \
    ldd /build/my_service > /tmp/runtime-dependencies; \
    ldd /usr/bin/curl >> /tmp/runtime-dependencies; \
    awk '$2 == "=>" {print $3} $1 ~ "^/" {print $1}' /tmp/runtime-dependencies > /tmp/runtime-libraries; \
    sort -u /tmp/runtime-libraries -o /tmp/runtime-libraries; \
    while read -r library; do cp --parents -L "$library" /runtime; done < /tmp/runtime-libraries; \
    test -f /runtime/lib64/ld-linux-x86-64.so.2; \
    cp /etc/ssl/certs/ca-certificates.crt /runtime/etc/ssl/certs/; \
    cp /etc/nsswitch.conf /runtime/etc/; \
    printf 'app:x:10001:10001:Application:/app:/sbin/nologin\n' > /runtime/etc/passwd; \
    printf 'app:x:10001:\n' > /runtime/etc/group

FROM scratch
COPY --from=build /runtime/ /
WORKDIR /app
COPY config/static_config.yaml /app/config.yaml
ENV PATH=/usr/bin QEMU_GUEST_BASE=0x800000000000
USER 10001:10001
EXPOSE 8080
ENTRYPOINT ["/app/my_service", "--config", "/app/config.yaml"]
