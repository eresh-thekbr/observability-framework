package com.example.listeners

import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.api.trace.{Span, Tracer, StatusCode}
import io.opentelemetry.context.Context
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener

class OtelQueryExecutionListener extends QueryExecutionListener {

  private val tracer: Tracer = GlobalOpenTelemetry.getTracer("spark-sql-tracer")

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
    val parentContext = OtelSparkListener.getAppContext
    val span = tracer.spanBuilder(s"Spark SQL: $funcName")
      .setParent(parentContext)
      .startSpan()

    try {
      span.setAttribute("spark.sql.query", qe.logical.toString()) // Logical Plan
      span.setAttribute("spark.sql.duration_ms", durationNs / 1000000)

      // Extract metrics from the executed plan (e.g., numOutputRows)
      // This varies by Spark version, but generally available in executedPlan.metrics
      val metrics = qe.executedPlan.metrics
      metrics.foreach { case (name, metric) =>
        span.setAttribute(s"spark.sql.metric.$name", metric.value)
      }

    } finally {
      span.end()
    }
  }

/*  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
    val parentContext = OtelSparkListener.getAppContext
    val span = tracer.spanBuilder(s"Spark SQL: $funcName")
      .setParent(parentContext)
      .startSpan()

    try {
      span.setAttribute("error", true)
      span.setAttribute("spark.sql.error.message", exception.getMessage)
      span.setAttribute("spark.sql.query", qe.logical.toString())
    } finally {
      span.end()
    }
  }*/

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
    val parentContext = OtelSparkListener.getAppContext
    val span = tracer.spanBuilder(s"Spark SQL FAILED: $funcName")
      .setParent(parentContext)
      .startSpan()

    try {
      span.setAttribute("spark.sql.query", qe.logical.toString())
      span.setAttribute("error", true)
      span.setAttribute("error.type", exception.getClass.getName)
      span.setAttribute("error.message", exception.getMessage)

      // 🚨 Record the exception with full stack trace
      span.recordException(exception)
      span.setStatus(StatusCode.ERROR, exception.getMessage)

      // 🚨 Also log it via logger (goes to Datadog through OpenTelemetryAppender)
      val logger = org.apache.logging.log4j.LogManager.getLogger(this.getClass)
      logger.error(s"[QueryExecutionListener] SQL query failed: $funcName", exception)

    } finally {
      span.end()
    }
  }
}
