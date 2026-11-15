# Spark jobs + OpenTelemetry Java agent in one image.
# Build stage compiles the Scala code on JDK 8 (Scala 2.12 / Spark 3.4 target).
FROM maven:3.9.9-eclipse-temurin-8 AS build
WORKDIR /build
COPY pom.xml ./
RUN mvn -q -B dependency:go-offline || true
COPY src ./src
RUN mvn -q -B package -DskipTests

FROM eclipse-temurin:8-jre
ARG OTEL_AGENT_VERSION=2.16.0
ADD https://repo1.maven.org/maven2/io/opentelemetry/javaagent/opentelemetry-javaagent/${OTEL_AGENT_VERSION}/opentelemetry-javaagent-${OTEL_AGENT_VERSION}.jar /opt/otel/opentelemetry-javaagent.jar

WORKDIR /app
COPY --from=build /build/target/OtelSpark-1.0-SNAPSHOT.jar /app/app.jar
COPY --from=build /build/target/lib /app/lib
COPY docker/run-job.sh docker/traffic-loop.sh /app/
RUN chmod 755 /opt/otel/opentelemetry-javaagent.jar /app/*.sh \
    && useradd --create-home --uid 10001 spark && chown spark /app

# OpenTelemetry agent settings (override per service in docker-compose.yml)
ENV OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4317 \
    OTEL_EXPORTER_OTLP_PROTOCOL=grpc \
    OTEL_TRACES_EXPORTER=otlp \
    OTEL_METRICS_EXPORTER=otlp \
    OTEL_LOGS_EXPORTER=otlp \
    OTEL_METRIC_EXPORT_INTERVAL=5000 \
    OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE=delta \
    OTEL_INSTRUMENTATION_LOG4J_APPENDER_ENABLED=false

USER spark
ENTRYPOINT ["/app/traffic-loop.sh"]
