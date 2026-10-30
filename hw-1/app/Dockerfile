FROM eclipse-temurin:21-jdk AS build

WORKDIR /workspace

COPY gradlew settings.gradle build.gradle ./

COPY gradle gradle

COPY app/build.gradle app/

COPY dashgen/build.gradle dashgen/

RUN chmod +x gradlew

COPY app/src/main app/src/main

RUN --mount=type=cache,target=/root/.gradle \
    ./gradlew :app:bootJar --no-daemon

FROM eclipse-temurin:21-jre-alpine

WORKDIR /app

COPY --from=build /workspace/app/build/libs/app-*.jar app.jar

EXPOSE 8080

HEALTHCHECK --interval=5s --timeout=3s --start-period=30s --retries=3 \
    CMD wget -q -O /dev/null http://localhost:8080/health || exit 1

ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-jar", "app.jar"]
