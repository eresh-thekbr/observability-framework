package com.example.apps

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode, expr}

object SparkApp {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("OtelSpark JSON Reader")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // ── Spans ──────────────────────────────────────────────────────────────
    println("\n=== SPANS ===")
    val spans = spark.read.json("data/spans.json")
      .select(explode(col("resourceSpans")).as("rs"))
      .select(explode(col("rs.scopeSpans")).as("ss"))
      .select(explode(col("ss.spans")).as("s"))
      .select(
        col("s.traceId"),
        col("s.spanId"),
        col("s.parentSpanId"),
        col("s.name"),
        col("s.startTimeUnixNano"),
        col("s.endTimeUnixNano")
      )
    spans.show(20, truncate = false)
    println(s"Total spans: ${spans.count()}")

    // ── Logs ───────────────────────────────────────────────────────────────
    // traceId/spanId live inside the attributes array (dd.trace_id / dd.span_id),
    // not in the top-level OTLP fields.
    println("\n=== LOGS ===")
    val logs = spark.read.json("data/logs.json")
    println(s"Total logs: ${logs.count()}")

    val result = logs
      .withColumn("resourceLog", explode(col("resourceLogs")))
      .withColumn("scopeLog", explode(col("resourceLog.scopeLogs")))
      .withColumn("logRecord", explode(col("scopeLog.logRecords")))
      .select(
        col("logRecord.traceId").alias("trace_id"),
        col("logRecord.spanId").alias("span_id"),
        col("logRecord.body.stringValue").alias("body"),
        col("logRecord.severityNumber").alias("severity"),
        col("logRecord.timeUnixNano").alias("event_time"),
        col("logRecord.observedTimeUnixNano").alias("observed_time")
      )
    val logs_df = result.filter("trace_id is not null")
    logs_df.show(20, truncate = false)

    // ── Correlated: logs joined to their span ──────────────────────────────
    println("\n=== LOGS WITH SPAN NAME (trace correlation) ===")
    // join logs to spans using trace_id (span_id is optional since not all logs have it, and we can still correlate on trace_id alone)
    val spans_logs = spans.join(logs_df, spans("traceId") === logs_df("trace_id"), "left")
      .select(
        col("traceId"),
        col("spanId"),
        col("name").as("spanName"),
        col("severity"),
        col("body"),
        col("event_time"),
        col("observed_time"),
        col("startTimeUnixNano"),
        col("endTimeUnixNano")
      )
    spans_logs.show(20, truncate = false)

    // ── Metrics ────────────────────────────────────────────────────────────
    println("\n=== METRICS ===")
    val metrics = spark.read.json("data/metrics.json")
      .select(explode(col("resourceMetrics")).as("rm"))
      .select(explode(col("rm.scopeMetrics")).as("sm"))
      .select(explode(col("sm.metrics")).as("m"))
      .select(
        col("m.name"),
        col("m.description")
      )
    metrics.show(20, truncate = false)
    println(s"Total metric series: ${metrics.count()}")

    spark.stop()
  }
}