# Datadog (optional backend)

The framework was first built against a Datadog trial. Datadog is now **optional**: the default
stack is open source (OpenTelemetry Collector → Elasticsearch → Kibana). Nothing in the Spark
code is Datadog-specific except the `dd.trace_id` / `dd.span_id` log attributes, which Datadog
uses for log ↔ trace linking and which are harmless elsewhere.

There are two ways to send to Datadog.

## Option 1 (recommended): through the OpenTelemetry Collector

No code or JVM-flag changes. The collector fans out to Datadog **and** Elasticsearch.

```bash
export DD_API_KEY=<your key>          # keep it in your shell or a secrets manager, never in git
export DD_SITE=datadoghq.com
OTEL_CONFIG=./otel/otel-config.datadog.yaml docker compose up -d otel-collector
make load
```

`otel/otel-config.datadog.yaml` uses the Datadog exporter plus the Datadog connector, which
computes APM trace metrics (hits, errors, latency per resource) from the spans.

Useful Datadog queries:

```
service:fraud-detection AND status:error
service:risk-aggregation AND resource_name:"Spark Driver Failure"
service:clickstream-funnel AND @spark.sql.label:SHUFFLE
```

## Option 2: listener exports straight to a Datadog Agent (no Java agent)

`OtelSparkListener.setUpDatadog` builds its own OpenTelemetry SDK and exports OTLP to a
Datadog Agent with OTLP ingest enabled on `localhost:14317`.

```
-Dotel.listener.useJavaAgent=false
-Dotel.datadog.enabled=true
-Dotel.service.name=fraud-detection-app
-Dlog4j.configurationFile=classpath:log4j2.xml
```

Enable OTLP ingest in `datadog.yaml`:

```yaml
otlp_config:
  receiver:
    protocols:
      grpc:
        endpoint: localhost:14317
```

## Option 3: Spark + JVM metrics through JMX (Datadog Agent JMXFetch)

Exposes JVM MBeans so the Datadog Agent's Spark integration can scrape heap, GC, threads and
class loading:

```
-Dcom.sun.management.jmxremote
-Dcom.sun.management.jmxremote.port=9010
-Dcom.sun.management.jmxremote.rmi.port=9010
-Dcom.sun.management.jmxremote.authenticate=false
-Dcom.sun.management.jmxremote.ssl=false
-Djava.rmi.server.hostname=127.0.0.1
```

```bash
vi /opt/datadog-agent/etc/conf.d/spark.d/conf.yaml      # Spark integration + JMX port 9010
sudo datadog-agent check spark                         # test the integration
launchctl kickstart -k gui/$(id -u)/com.datadoghq.agent # restart the agent (macOS)
sudo datadog-agent status | grep -i -E "jmx|jmxfetch|9010"
lsof -nP -iTCP:9010 -sTCP:LISTEN                       # JVM must be listening while Spark runs
```
