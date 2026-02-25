# OpenTelemetry Spark Application with Datadog Integration

## Overview
This project demonstrates how to integrate OpenTelemetry with a Spark application to export traces and metrics. It also includes Datadog integration for monitoring.

## Prerequisites
1. **OpenTelemetry Java Agent**: Download the agent.
   ```bash
    -javaagent:/Users/ereshtayanna/Downloads/opentelemetry-javaagent.jar
    -Dlog4j.configurationFile=classpath:log4j2.xml
    -Dotel.datadog.enabled=true

### Install the Spark integration for Datadog Agent
    vi /opt/datadog-agent/etc/conf.d/spark.d/conf.yaml

    datadog-agent status
    sudo launchctl stop com.datadoghq.agent
    sudo launchctl start com.datadoghq.agent

### Test the integration configuration:
    sudo datadog-agent check spark

### restart the agent:
    launchctl kickstart -k gui/$(id -u)/com.datadoghq.agent

### Added below VM options
    -Dotel.datadog.enabled=true
    -Dlog4j.configurationFile=classpath:log4j2.xml
    -Dspark.app.name=fraud-detection-app
    -Dotel.service.name=OTEL-SPARK-APP
    -Dcom.sun.management.jmxremote
    -Dcom.sun.management.jmxremote.port=9010
    -Dcom.sun.management.jmxremote.rmi.port=9010
    -Dcom.sun.management.jmxremote.authenticate=false
    -Dcom.sun.management.jmxremote.ssl=false
    -Djava.rmi.server.hostname=127.0.0.1
    -javaagent:/Users/ereshtayanna/Downloads/opentelemetry-javaagent.jar
    -Dotel.traces.exporter=otlp
    -Dotel.exporter.otlp.endpoint=http://localhost:14317
    -Dotel.exporter.otlp.protocol=grpc

### Exposes JVM MBeans over JMX on 127.0.0.1:9010 so an external collector (Datadog Agent JMXFetch) can scrape:
    heap / non-heap
    GC metrics
    thread counts
    class loading, etc.


### Datadog Spark integration + JMX (port 9010).
    /opt/datadog-agent/etc/conf.d/spark.d/conf.yaml

### Send Spark metrics (driver/executor/JVM + partial Spark internals) from a local Spark standalone app (IntelliJ) to Datadog using JMX.
    ``

    Spark JVM (Driver / Executors)
                ↓  (JMX on port 9010)
    Datadog Agent (JMXFetch + Spark integration)
                ↓
    Datadog Dashboards

    ``
### Datadog Agent status , Ensure Agent & JMXFetch are running:
    sudo datadog-agent status | grep -i jmx
    sudo datadog-agent status | grep -i -E "jmx|jmxfetch|9010|collector"

## How to verify it's actually working (critical)
### JVM must expose JMX, Run below command while Spark job is running:
    $ lsof -nP -iTCP:9010 -sTCP:LISTEN

### High-signal ones:

    FileScan / BatchScan → reading source (HDFS/S3/Hive table)
    Project → select/withColumn/rename (column projection)
    Filter → where/filter
    HashAggregate / ObjectHashAggregate → groupBy/agg
    Join (BroadcastHashJoin / SortMergeJoin / ShuffledHashJoin) → join type
    Exchange → shuffle boundary (usually the big cost driver)
    Sort → sort/orderBy
    GlobalLimit/LocalLimit → head/show/take-like actions
    WriteFiles / InsertIntoHadoopFsRelationCommand / SaveIntoDataSourceCommand → writes

    Read → Filter → Select → Join/Aggregate → Shuffle → Write


datadog queries:

    service:otel-spark-app AND status:error
    service:otel-spark-app AND resource_name:"Spark Driver Failure"
    service:otel-spark-app AND resource_name:"Exchange"



---

# Phase 2 — Switched from Datadog to OTLP Collector + Local JSON Files

## What Changed

Removed the Datadog dependency. Instead of sending to Datadog's OTLP intake (port 14317), we now send to a local OpenTelemetry Collector (port 4317) which writes all signals to JSON files.

## Architecture

```
Spark Application
        │
        ├── OtelSparkListener       → spans: App → SQL → Job → Stage → Task
        ├── OpenTelemetryAppender   → logs with trace context injected
        └── OTel Java Agent
              │
              ▼  OTLP gRPC (localhost:4317)
    OpenTelemetry Collector
              │
    ┌─────────┼──────────┐
    ▼         ▼          ▼
spans.json  logs.json  metrics.json
```

## Collector Config (`otel/otel-config.yaml`)

```yaml
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317
processors:
  batch: {}
exporters:
  file/traces:
    path: data/spans.json
  file/logs:
    path: data/logs.json
  file/metrics:
    path: data/metrics.json
service:
  pipelines:
    traces:
      receivers: [otlp]
      processors: [batch]
      exporters: [file/traces]
    logs:
      receivers: [otlp]
      processors: [batch]
      exporters: [file/logs]
    metrics:
      receivers: [otlp]
      processors: [batch]
      exporters: [file/metrics]
```

## JVM Flags for Phase 2

```
-javaagent:/Users/ereshtayanna/Downloads/opentelemetry-javaagent.jar
-Dotel.service.name=fraud-detection-app
-Dotel.exporter.otlp.endpoint=http://localhost:4317
-Dotel.exporter.otlp.protocol=grpc
-Dotel.traces.exporter=otlp
-Dotel.metrics.exporter=otlp
-Dotel.logs.exporter=otlp
-Dotel.listener.useJavaAgent=true
-Dotel.instrumentation.log4j-appender.enabled=false
-Dlog4j.configurationFile=classpath:log4j2.xml
-Dspark.app.name=fraud-detection-app
```

Note: `-Dotel.instrumentation.log4j-appender.enabled=false` is required. Without it the agent's built-in Log4j2 capture runs alongside our custom appender and produces log records with no trace context.

## Start the Collector

```bash
otelcol --config otel/otel-config.yaml
```

If port 8888 conflicts on startup: `lsof -i :8888` then `kill <PID>`.

## Trace–Log Correlation

`OpenTelemetryAppender` injects `dd.trace_id` and `dd.span_id` as attributes into every log record. In the JSON files, join logs to spans using:

```scala
spans.join(logs_df, spans("traceId") === logs_df("trace_id"), "left")
```

Note: `traceId` in `logs.json` is inside the `attributes` array, not a top-level OTLP field. Extract it with:

```scala
expr("filter(l.attributes, x -> x.key = 'dd.trace_id')[0].value.stringValue")
```

About 94% of log records carry trace context. The remaining 6% are from Spark startup before the application span is created.

## Metrics Correlation

Metrics do not carry `traceId` — by OTel spec, metrics are aggregated time-series not per-request events.

- In Datadog: correlation is done via shared tags and Datadog auto-generates APM metrics from traces.
- In JSON files: correlate at the application level using `spark.app.id` as a shared attribute, or by time window.

## Reading the JSON Files (SparkApp)

No JVM flags or agent needed.

```bash
mvn package
java -cp target/OtelSpark-1.0-SNAPSHOT.jar com.example.apps.SparkApp
```

Shows: span tree, logs with trace IDs, logs joined to their parent span, metric series names.