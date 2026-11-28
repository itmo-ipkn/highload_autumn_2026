FROM eclipse-temurin:27-jdk AS builder

RUN apt-get update \
    && apt-get install -y --no-install-recommends maven \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY pom.xml .
COPY producer-api/ producer-api/
COPY producer-service/ producer-service/

RUN --mount=type=cache,target=/root/.m2 \
    mvn clean package -pl producer-service -am -DskipTests

FROM eclipse-temurin:27-jre

WORKDIR /app

COPY --from=builder /app/producer-service/target/*.jar app.jar

STOPSIGNAL SIGTERM

ENTRYPOINT ["java", "-jar", "app.jar"]