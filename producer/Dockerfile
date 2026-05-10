FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /build
COPY pom.xml .
COPY producer/pom.xml producer/
COPY producer/src producer/src
COPY streams/pom.xml streams/
COPY streams/src streams/src
RUN mvn -q -pl producer -am -DskipTests package

FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
COPY --from=build /build/producer/target/kafka-lab-producer-1.0.0.jar /app/app.jar
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
