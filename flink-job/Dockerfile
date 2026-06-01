# ---- Stage 1: build the fat JAR ------------------------------------------------
FROM maven:3.9-eclipse-temurin-11 AS build

WORKDIR /build

# Copy only the POM files first so Maven's layer cache is reused when only source changes.
COPY pom.xml ./pom.xml

# The flink-job module depends on the parent POM for property/dependency management.
# We copy it into a predictable location that matches the relative path Maven expects.
COPY ../pom.xml /parent-pom.xml

# Pre-fetch all declared dependencies — this layer is cached as long as pom.xml is unchanged.
RUN mvn dependency:go-offline -q

# Copy sources and build the shaded JAR.
COPY src ./src
RUN mvn package -DskipTests -q

# ---- Stage 2: Flink base image -------------------------------------------------
FROM flink:1.20-java11

WORKDIR /app

# Place the fat JAR in the standard Flink user-lib directory.
# VVP3 mounts this directory into the JobManager and TaskManager containers automatically.
COPY --from=build /build/target/flink-job-1.0.0-SNAPSHOT.jar /opt/flink/usrlib/flink-job.jar
