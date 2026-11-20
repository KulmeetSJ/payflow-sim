# One Dockerfile for both services:  docker build --build-arg MODULE=payment-gateway .
# Base images pinned by digest (same as gitops-demo-app).
FROM maven:3.10.0-eclipse-temurin-21-noble@sha256:f6d6c7eb62ca6a7034c17bb04a6177a8269ca38e3a1febf95094dfcd97d1305b AS build
ARG MODULE
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/root/.m2 mvn -B -q -pl ${MODULE} -am package -DskipTests \
 && cp ${MODULE}/target/${MODULE}-*.jar /app.jar

# OpenTelemetry Java agent: zero-code traces for Spring MVC, Kafka producer/consumer, JDBC.
# Checksum-verified download (sha256 published on Maven Central).
FROM scratch AS otel
ADD --checksum=sha256:e43a270df53c6f423e0ab157be9c27ccc01d5dc3232e162fe54777573db2f149 \
    https://repo1.maven.org/maven2/io/opentelemetry/javaagent/opentelemetry-javaagent/2.33.1/opentelemetry-javaagent-2.33.1.jar \
    /opentelemetry-javaagent.jar

FROM gcr.io/distroless/java21-debian13:nonroot@sha256:6b60e44d652d66a9e2e584364c96adac4df7133674b2834b6f71741b7cdb1ff3
COPY --from=build /app.jar /app/app.jar
COPY --from=otel --chmod=444 /opentelemetry-javaagent.jar /app/opentelemetry-javaagent.jar
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=75 -XX:+ExitOnOutOfMemoryError"
USER nonroot
# The agent is always loaded; OTEL_JAVAAGENT_ENABLED=false turns it off.
ENTRYPOINT ["java", "-javaagent:/app/opentelemetry-javaagent.jar", "-jar", "/app/app.jar"]
