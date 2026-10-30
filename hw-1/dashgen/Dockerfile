FROM eclipse-temurin:21-jdk AS build

WORKDIR /workspace

COPY gradlew settings.gradle build.gradle ./

COPY gradle gradle

COPY app/build.gradle app/

COPY dashgen/build.gradle dashgen/

RUN chmod +x gradlew

COPY dashgen/src/main dashgen/src/main

RUN --mount=type=cache,target=/root/.gradle \
    ./gradlew :dashgen:bootJar --no-daemon

FROM eclipse-temurin:21-jre-alpine

WORKDIR /dashgen

COPY --from=build /workspace/dashgen/build/libs/dashgen-*.jar dashgen.jar

ENTRYPOINT ["java", "-jar", "dashgen.jar"]
