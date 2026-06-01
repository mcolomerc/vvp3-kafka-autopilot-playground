FROM eclipse-temurin:11-jre-jammy
WORKDIR /app
COPY target/kafka-consumer-1.0.0-SNAPSHOT.jar app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]
