FROM eclipse-temurin:21.0.10_7-jdk-jammy AS build
WORKDIR /workspace
COPY . .
RUN --mount=type=cache,id=highload-lab-gradle,target=/root/.gradle,sharing=locked \
    ./gradlew --no-daemon :load-generator:installDist

FROM eclipse-temurin:21.0.10_7-jre-jammy
WORKDIR /app
COPY --from=build /workspace/load-generator/build/install/load-generator/ ./
ENTRYPOINT ["/app/bin/load-generator"]
