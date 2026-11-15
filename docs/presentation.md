# OtelSpark: presentation notes

## The problem

Spark jobs are black boxes. When a job fails, runs slowly, or produces wrong results, engineers
have no structured way to answer:

- Which stage took the longest?
- Which task failed, and why?
- What was Spark doing when this error log was written?
- How much did this job shuffle compared to last week?

The Spark UI disappears when the application ends, and log scraping gives no structure and no
correlation. Neither approach scales as the number and complexity of jobs grows.

## What we built

An observability framework that instruments any Spark application and produces three correlated
signals (**traces, logs and metrics**) in the OpenTelemetry standard format.

The core is `OtelSparkListener`, a custom Spark listener. It hooks into Spark's lifecycle events
and builds a span hierarchy that mirrors how Spark actually executes a job:

```
Application run → SQL operation → Job → Stage → Task
```

Every span carries context: stage IDs, task counts, executor host, the SQL operator chain and a
plan excerpt, error details. When a task fails, the failure propagates upward, so the stage, job,
SQL operation and application are all marked failed and the problem is visible at every level.

A companion `OpenTelemetryAppender` (a log4j2 plugin) stamps every Spark log line with the run's
`trace_id` and `span_id`, so a log line leads straight to the trace that produced it.

## Three phases

### Phase 1: Datadog

All signals went to Datadog through its OTLP intake:

- traces in Datadog APM with the full span hierarchy
- logs linked to traces through `dd.trace_id`: click a span, see its logs
- JVM and Spark metrics in Datadog dashboards through JMX
- error queries like `service:otel-spark-app AND status:error`

This proved the instrumentation was correct and the signal quality was high.

### Phase 2: vendor-neutral OTLP pipeline

We removed the Datadog dependency by putting an OpenTelemetry Collector between Spark and the
backend. The same signals flow over standard OTLP to the collector, which first wrote them to
JSON files. From then on, the instrumentation was no longer tied to any vendor.

### Phase 3: open-source stack with live traffic (current)

- The collector writes to **Elasticsearch**, and a **Kibana** dashboard (defined as code) shows
  run health, failures, durations, SQL operations, slow stages, logs and JVM metrics.
- A **simulator** runs three realistic pipelines (fraud detection, clickstream funnel, risk
  aggregation) back to back, with normal, skewed and failing runs, so the dashboard always has
  live data.
- The whole stack is in **Docker**: `make up && make load`.
- Datadog is still one collector config away (`otel/otel-config.datadog.yaml`).

## Signal correlation

### Traces and logs

Logs are joined to traces on the `trace_id` that `OpenTelemetryAppender` adds to every record.
In practice 85–95% of log records carry a trace ID. The rest are written during JVM and Spark
start-up, before the application span exists, which is expected.

For any error log, you can therefore open the exact run, and the span tree, that produced it.

### Traces and metrics

Metrics are aggregated time series and do not carry `trace_id`; the OpenTelemetry
specification is designed that way. They are linked to runs through shared attributes:
`service.name`, `spark.app.id`, and for shuffle volume `spark.job.id` and `spark.stage.id`.

## What this enables

| Capability | Before | After |
|---|---|---|
| Root cause of a slow job | manual log grep, Spark UI | navigate the span tree, "slowest stages" panel |
| Link an error log to its Spark operation | not possible | query by `trace_id` |
| Shuffle bytes over time, per job and stage | not available | `spark.shuffle.read.bytes` / `write.bytes` metrics |
| Detect cascading failures | read full logs | ERROR status propagates up the span tree |
| Spot data skew | inspect tasks one by one | duration by scenario, straggler task spans |
| Switch observability backend | rebuild instrumentation | change the collector config |

## Technical decisions worth noting

**Java agent plus a Spark listener.** The OTel Java agent handles JVM-level telemetry and
exporting. The listener adds Spark-specific context on top through `GlobalOpenTelemetry`. Adding
the framework to an application takes JVM flags and one Spark config
(`spark.extraListeners`), with no changes to business code.

**One trace per application run.** All spans of a run share one `trace_id`, which gives a
complete picture of the job from start to finish.

**Custom log4j2 appender instead of the agent's log capture.** The agent's own log4j2
instrumentation does not know Spark's span context, because Spark's listener threads are not
in any agent-managed scope. The custom appender pulls the application span context explicitly
and stamps it on every record. The agent's capture is turned off so that logs are not
duplicated.

**The collector is the integration point.** Batching, routing and the choice of backend live in
collector config, outside the Spark jobs.

## What is next

- Run the collector as a shared service and attach the agent to executors
  (`spark.executor.extraJavaOptions`) for cluster jobs.
- Emit task spans only for failed, retried and straggler tasks, and roll the rest up into stage
  attributes, so very large jobs stay affordable.
- Put the job ID in the logging MDC so logs link to the job or stage span, not only the
  application.
- Add alerting (Kibana rules) on failure rate and on duration regressions per pipeline.
