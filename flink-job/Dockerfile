# Stage 1: build the jar with Maven
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /build
COPY pom.xml .
RUN mvn -q dependency:go-offline
COPY src ./src
RUN mvn -q package -DskipTests

# Stage 2: run it on the official Flink image
FROM flink:1.20.1-java17
RUN mkdir -p /opt/flink/usrlib
COPY --from=build /build/target/fraud-flink-job-1.0.0.jar /opt/flink/usrlib/fraud-flink-job.jar