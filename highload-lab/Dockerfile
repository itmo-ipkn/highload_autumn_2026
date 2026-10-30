FROM eclipse-temurin:21.0.10_7-jdk-jammy AS build
WORKDIR /workspace
COPY . .
RUN ./gradlew --no-daemon bootJar

FROM eclipse-temurin:21.0.10_7-jre-jammy
WORKDIR /app
COPY --from=build /workspace/build/libs/*.jar app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]
