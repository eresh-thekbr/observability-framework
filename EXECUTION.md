# Execution guide

## Prerequisites

- Docker Desktop, running, with at least 6 GB of memory (Settings → Resources)
- `make`, `curl`, Python 3 (only for the helper scripts)
- Optional, for building outside Docker: JDK 8 and Maven 3.9

Ports used: `9200` Elasticsearch, `5601` Kibana, `4317` / `4318` OTLP.

## 1. Start the stack

```bash
git clone https://github.com/eresh-thekbr/observability-framework.git
cd observability-framework
make up
```

This starts three containers and then imports the Kibana dashboard. It takes about a minute
the first time, while the images download.

| Service | Role |
|---|---|
| `elasticsearch` | single-node Elasticsearch 8.17, `localhost:9200` |
| `kibana` | Kibana 8.17, `localhost:5601` |
| `otel-collector` | OpenTelemetry Collector contrib 0.145: OTLP in on `4317` (gRPC) / `4318` (HTTP), out to Elasticsearch and `./data/*.json` |

Expected ending:

```
waiting for Kibana at http://localhost:5601 ...... up
"successCount":17
Dashboard: http://localhost:5601/app/dashboards#/view/spark-observability
```

## 2. Start live traffic

```bash
make load    # builds the Spark image and starts the simulator
make logs    # follow it
```

Each run is a separate Spark application, so a separate trace. Runs take about 10 seconds,
followed by a 3–15 second pause:

```
[traffic] run #1 pipeline=fraud-detection scenario=normal
[traffic] run #1 fraud-detection/normal SUCCEEDED in 12s
[traffic] run #4 pipeline=fraud-detection scenario=skew
[traffic] run #4 fraud-detection/skew SUCCEEDED in 10s
[traffic] run #11 pipeline=risk-aggregation scenario=failure
[traffic] run #11 risk-aggregation/failure FAILED in 10s
```

| Scenario | Share | What happens |
|---|---|---|
| `normal` | 70% | the pipeline completes |
| `skew` | 15% | ~70% of rows land on one key, which produces one straggler task per stage |
| `failure` | 15% | one corrupt record makes a task throw. The task, stage, job, SQL execution and application all end in `ERROR`, plus an ERROR log |

Check data is arriving (example after ~10 minutes):

```bash
make status
```

```
index                                              docs.count store.size
.ds-logs-generic.otel-default-2026.11.15-000001          4012      2.1mb
.ds-metrics-generic.otel-default-2026.11.15-000001        812    310.4kb
.ds-traces-generic.otel-default-2026.11.15-000001        1490    1.2mb
```

`make stop-load` pauses the simulator. Leave it running for about 10 minutes before a demo,
so the dashboard has a few failures and a visible timeline.

## 3. Explore in Kibana

Open the dashboard: http://localhost:5601/app/dashboards#/view/spark-observability
(it refreshes every 10 seconds).

Useful queries in **Discover** (choose the data view first):

| Data view | KQL | Shows |
|---|---|---|
| Spark traces | `spark_level : application and run_status : FAILED` | failed runs |
| Spark traces | `trace_id : "<id>"` | every span of one run |
| Spark traces | `spark_level : sql and attributes.spark.sql.label : SHUFFLE` | shuffle-heavy queries |
| Spark traces | `spark_level : task and duration_sec > 1` | straggler tasks |
| Spark logs | `severity_text : ERROR` | errors, each with its `trace_id` |
| Spark logs | `trace_id : "<id>"` | every log line of one run |
| Spark metrics | `metrics.spark.tasks.running : *` | parallelism over time |

`spark_level`, `run_status` and `duration_sec` are runtime fields defined on the
**Spark traces** data view (see `kibana/build_dashboard.py`). Nothing extra is stored.

## 4. Demo script (about 5 minutes)

1. **Architecture.** Show `docs/telemetry-flow.svg` in the README: listener, appender, agent,
   collector, Elasticsearch, Kibana.
2. **Live dashboard.** Runs over time, the FAILED bars, and the duration-by-scenario chart
   (skewed runs are slower for the same pipeline).
3. **Drill into a failure.** In the *ERROR logs with trace context* panel, copy a `trace_id`.
4. **Follow the trace.** In Discover (Spark traces), run `trace_id : "<id>"` and sort by
   `@timestamp`. The failed task, then its stage, job, SQL execution and application are all
   `status.code: Error`.
5. **Same id, logs.** In Discover (Spark logs), run `trace_id : "<id>"` to see the full story of
   that run, ending with `Pipeline … FAILED … Corrupt record …`.
6. **Talk about limits.** README section 5: task-span volume, executor agents, MDC for per-job
   log linking.

## 5. Raw OTLP files and the Spark reader

The collector also appends every batch to `./data/spans.json`, `logs.json` and `metrics.json`.
To read them back with Spark (no Elasticsearch needed):

```bash
make analyze
```

Prints the span table (`traceId`, `spanId`, `parentSpanId`, `name`, timings), the log records
with their `trace_id`, logs joined to spans, and the metric names.

## 6. The original FraudDetectionApp

The first version of the framework ran over CSV inputs. To run it once:

```bash
make fraud   # generates data/*.csv (scripts/generate_sample_data.py), then runs the app
```

It shows up in Kibana as service `fraud-detection-app`.

## 7. Building without Docker

Scala 2.12 does not compile on JDK 21+ (`scala.reflect.internal.FatalError ... errorBadIndex`).
Build on JDK 8:

```bash
make build                # uses /usr/libexec/java_home -v 1.8
# or: JAVA_HOME=/path/to/jdk8 mvn package -DskipTests
```

To run a job from the host against the Dockerised collector, download the agent from
`https://repo1.maven.org/maven2/io/opentelemetry/javaagent/opentelemetry-javaagent/2.16.0/` and run:

```bash
java -javaagent:tools/opentelemetry-javaagent.jar \
  -Dotel.service.name=fraud-detection \
  -Dotel.exporter.otlp.endpoint=http://localhost:4317 \
  -Dotel.exporter.otlp.metrics.temporality.preference=delta \
  -Dotel.instrumentation.log4j-appender.enabled=false \
  -Dlog4j.configurationFile=classpath:log4j2.xml \
  -cp "target/OtelSpark-1.0-SNAPSHOT.jar:target/lib/*" \
  com.example.apps.PipelineSimulator fraud-detection normal
```

## 8. Regenerating the docs images

```bash
python3 scripts/render_outcome_svg.py     # docs/outcome.svg from the live Elasticsearch data
python3 kibana/build_dashboard.py         # after editing the dashboard definition
./scripts/setup-kibana.sh                 # re-import it
```

`docs/telemetry-flow.svg` is hand-written. It animates on GitHub (SMIL), but GitHub caches
images, so a new version can take a few minutes to appear.

## 9. Stop and clean up

```bash
make stop-load   # stop the simulator, keep the data
make down        # stop everything, delete Elasticsearch data
make clean       # also delete ./data/*.json
```

## Configuration

| Setting | Where | Default | Meaning |
|---|---|---|---|
| `RUNS` | `traffic` service | `0` | number of simulator runs; `0` runs until stopped |
| `PAUSE_MIN` / `PAUSE_MAX` | `traffic` service | `3` / `15` | seconds between runs |
| `OTEL_METRIC_EXPORT_INTERVAL` | Dockerfile | `5000` | metric export interval (ms) |
| `OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE` | Dockerfile | `delta` | the Elasticsearch exporter drops cumulative histograms |
| `OTEL_INSTRUMENTATION_LOG4J_APPENDER_ENABLED` | Dockerfile | `false` | the agent's built-in log4j capture would duplicate every log without trace context |
| `-Dotel.listener.useJavaAgent` | `docker/run-job.sh` | `true` | `false` makes the listener build its own SDK |
| `-Dotel.datadog.enabled` | JVM flag | unset | with `useJavaAgent=false`: export straight to a Datadog Agent on `:14317` |
| `OTEL_CONFIG` | shell, for `docker compose` | `./otel/otel-config.yaml` | collector config; `./otel/otel-config.datadog.yaml` adds Datadog |
| `DD_API_KEY`, `DD_SITE` | shell | unset | only for the Datadog config |

## Troubleshooting

| Symptom | Cause and fix |
|---|---|
| `bind: address already in use` on 4317 | another collector is running on the host (`lsof -i :4317`); stop it |
| Elasticsearch exits with code 137 | Docker is out of memory; give it ≥ 6 GB |
| Collector warns `dropping cumulative temporality histogram` | a JVM is exporting cumulative metrics; set the temporality preference to `delta` |
| Kibana panels empty | check the time picker (last 30 minutes) and that `make status` shows documents |
| `make up` import shows nothing | Kibana not ready yet; rerun `./scripts/setup-kibana.sh` |
| Build fails with `FatalError ... errorBadIndex` | compiling on JDK 21+; use JDK 8 (`make build`) |
| Logs have no `trace_id` | they were written before the application span started (JVM / Spark start-up), which is expected |
