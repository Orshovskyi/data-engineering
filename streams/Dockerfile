FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /build
COPY pom.xml .
COPY producer/pom.xml producer/
COPY producer/src producer/src
COPY streams/pom.xml streams/
COPY streams/src streams/src
RUN mvn -q -pl streams -am -DskipTests package

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /build/streams/target/kafka-lab-streams-1.0.0.jar /app/app.jar
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
