FROM eclipse-temurin:21.0.10_7-jdk-jammy AS build
WORKDIR /workspace
COPY . .
RUN --mount=type=cache,id=highload-lab-gradle,target=/root/.gradle,sharing=locked \
    ./gradlew --no-daemon :dashboard-generator:installDist

FROM eclipse-temurin:21.0.10_7-jre-jammy
WORKDIR /app
COPY --from=build /workspace/dashboard-generator/build/install/dashboard-generator/ ./
ENTRYPOINT ["/app/bin/dashboard-generator"]
