FROM eclipse-temurin:21-jdk AS build
WORKDIR /src
COPY gradle gradle
COPY gradlew settings.gradle build.gradle ./
COPY src src
RUN --mount=type=cache,target=/root/.gradle ./gradlew --no-daemon installDist

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /src/build/install/hl-hw2/ ./
USER 10001:10001
ENTRYPOINT ["java","-cp","lib/*","tech.soba4ka.hw2.Main"]
