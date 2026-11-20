# One Dockerfile for both services:  docker build --build-arg MODULE=payment-gateway .
# Base images pinned by digest (same as gitops-demo-app).
FROM maven:3.10.0-eclipse-temurin-21-noble@sha256:f6d6c7eb62ca6a7034c17bb04a6177a8269ca38e3a1febf95094dfcd97d1305b AS build
ARG MODULE
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/root/.m2 mvn -B -q -pl ${MODULE} -am package -DskipTests \
 && cp ${MODULE}/target/${MODULE}-*.jar /app.jar

FROM gcr.io/distroless/java21-debian13:nonroot@sha256:6b60e44d652d66a9e2e584364c96adac4df7133674b2834b6f71741b7cdb1ff3
COPY --from=build /app.jar /app/app.jar
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=75 -XX:+ExitOnOutOfMemoryError"
USER nonroot
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
