FROM gradle:9.8.2-jdk21-corretto AS build
USER root
RUN mkdir -p /workspace && chown gradle:gradle /workspace
USER gradle
WORKDIR /workspace
COPY --chown=gradle:gradle . .
RUN gradle --no-daemon clean bootJar

FROM eclipse-temurin:21-jre-jammy
WORKDIR /app
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /workspace/build/libs/app.jar /app/app.jar
EXPOSE 8080
HEALTHCHECK --interval=5s --timeout=3s --start-period=20s --retries=12 \
  CMD curl -fsS http://127.0.0.1:8080/health || exit 1
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75.0", "-jar", "/app/app.jar"]
