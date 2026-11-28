FROM gradle:8.14-jdk21-alpine AS build
WORKDIR /workspace
COPY --chown=gradle:gradle . .
RUN gradle --no-daemon bootJar

FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
COPY --from=build /workspace/build/libs/hw2-0.0.1-SNAPSHOT.jar /app/app.jar
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
