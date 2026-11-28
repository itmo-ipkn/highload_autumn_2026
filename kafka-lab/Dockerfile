FROM eclipse-temurin:21.0.10_7-jdk-jammy AS build
WORKDIR /workspace
COPY gradlew build.gradle.kts settings.gradle.kts ./
COPY gradle/ gradle/
COPY src/ src/
RUN ./gradlew --no-daemon installDist

FROM eclipse-temurin:21.0.10_7-jre-jammy
WORKDIR /app
COPY --from=build /workspace/build/install/kafka-lab/ ./
ENTRYPOINT ["/app/bin/kafka-lab"]
