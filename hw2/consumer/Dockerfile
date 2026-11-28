FROM eclipse-temurin:27-jdk AS builder

RUN apt-get update \
    && apt-get install -y --no-install-recommends maven \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY pom.xml .
COPY consumer-api/ consumer-api/
COPY consumer-service/ consumer-service/

RUN --mount=type=cache,target=/root/.m2 \
    mvn clean package -pl consumer-service -am -DskipTests

FROM eclipse-temurin:27-jre

WORKDIR /app

COPY --from=builder /app/consumer-service/target/*.jar app.jar

STOPSIGNAL SIGTERM

ENTRYPOINT ["java", "-jar", "app.jar"]
