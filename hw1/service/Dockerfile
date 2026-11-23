FROM maven:3.9-eclipse-temurin-17 AS build

WORKDIR /app

COPY pom.xml .
COPY metrics-starter/pom.xml metrics-starter/pom.xml
COPY highload-ms/pom.xml highload-ms/pom.xml

COPY metrics-starter/src metrics-starter/src
COPY highload-ms/src highload-ms/src

RUN mvn clean package -pl highload-ms -am -DskipTests

FROM eclipse-temurin:17-jre

WORKDIR /app

COPY --from=build /app/highload-ms/target/*.jar app.jar

EXPOSE 8080

ENTRYPOINT ["java", "-jar", "app.jar"]