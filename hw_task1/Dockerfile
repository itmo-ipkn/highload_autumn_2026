# syntax=docker/dockerfile:1.7

# ---------- Стадия сборки ----------
FROM ubuntu:24.04 AS build

RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    apt-get update && apt-get install -y --no-install-recommends \
        build-essential cmake ninja-build git curl zip unzip tar \
        pkg-config ca-certificates

ARG VCPKG_COMMIT=67972e92edd181d433927b42d8ab4bfa52ac164a

RUN git clone https://github.com/microsoft/vcpkg /opt/vcpkg \
    && cd /opt/vcpkg && git checkout ${VCPKG_COMMIT} \
    && /opt/vcpkg/bootstrap-vcpkg.sh -disableMetrics

ENV VCPKG_ROOT=/opt/vcpkg

ARG TARGETARCH

WORKDIR /src

COPY vcpkg.json CMakeLists.txt ./

RUN --mount=type=cache,target=/root/.cache/vcpkg \
    case "${TARGETARCH}" in \
        amd64) TRIPLET=x64-linux ;; \
        arm64) TRIPLET=arm64-linux ;; \
        *) echo "unsupported arch: ${TARGETARCH}" >&2; exit 1 ;; \
    esac \
    && echo "${TRIPLET}" > /src/.vcpkg-triplet \
    && /opt/vcpkg/vcpkg install --triplet "${TRIPLET}"

COPY src ./src

RUN TRIPLET="$(cat /src/.vcpkg-triplet)" \
    && cmake -S . -B build -G Ninja \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_TOOLCHAIN_FILE=/opt/vcpkg/scripts/buildsystems/vcpkg.cmake \
        -DVCPKG_TARGET_TRIPLET="${TRIPLET}" \
    && cmake --build build

# ---------- Стадия рантайма ----------
FROM ubuntu:24.04

RUN apt-get update && apt-get install -y --no-install-recommends \
        libssl3 zlib1g curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN useradd --uid 10001 --create-home --shell /usr/sbin/nologin app

COPY --from=build /src/build/app /usr/local/bin/app

USER 10001:10001

EXPOSE 8080

HEALTHCHECK --interval=5s --timeout=3s --start-period=5s --retries=5 \
    CMD curl -fsS http://localhost:8080/health || exit 1

ENTRYPOINT ["/usr/local/bin/app"]