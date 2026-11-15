#!/bin/sh
# Run one Spark job with the OpenTelemetry Java agent attached.
# Usage: run-job.sh <main-class> [args...]
#   OTEL_SERVICE_NAME sets the service name shown in Kibana (default: the first job argument).
set -e
MAIN_CLASS="$1"; shift
export OTEL_SERVICE_NAME="${OTEL_SERVICE_NAME:-${1:-spark-job}}"
exec java \
  -javaagent:/opt/otel/opentelemetry-javaagent.jar \
  -Dlog4j.configurationFile=classpath:log4j2.xml \
  -Dotel.listener.useJavaAgent=true \
  -Dspark.driver.host=127.0.0.1 \
  -Xmx2g \
  -cp "/app/app.jar:/app/lib/*" \
  "$MAIN_CLASS" "$@"
