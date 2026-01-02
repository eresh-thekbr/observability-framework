package com.example.exporters

import com.example.listeners.OtelSparkListener
import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.logs.{Logger, Severity}
import io.opentelemetry.context.Context
import org.apache.logging.log4j.core.{Filter, LogEvent}
import org.apache.logging.log4j.core.appender.AbstractAppender
import org.apache.logging.log4j.core.config.plugins.{Plugin, PluginAttribute, PluginElement, PluginFactory}
import org.apache.logging.log4j.core.layout.PatternLayout
import org.apache.logging.log4j.core.Layout
import com.example.listeners.OtelSparkListener
import io.opentelemetry.api.trace.Span
import java.io.Serializable
import java.util.concurrent.TimeUnit

@Plugin(
  name = "OpenTelemetryAppender",
  category = "Core",
  elementType = "appender",
  printObject = true
)
class OpenTelemetryAppender(
                             name: String,
                             filter: Filter,
                             layout: Layout[_ <: Serializable]
                           ) extends AbstractAppender(name, filter, layout, true, Array.empty) {

  // Debug so we know plugin is actually constructed
  println(s"[OpenTelemetryAppender] constructed appender: $name")

override def append(event: LogEvent): Unit = {
  try {
    val currentContext: Context = OtelSparkListener.getAppContext

    val logger: Logger = GlobalOpenTelemetry.get().getLogsBridge.get("spark-application")

    // 2. Extract Trace/Span ID from current Span
    val currentSpan = Span.fromContext(currentContext)
    val traceId = currentSpan.getSpanContext.getTraceId
    val spanId = currentSpan.getSpanContext.getSpanId

    val logRecordBuilder =
      logger
        .logRecordBuilder()
        .setContext(currentContext)
        .setTimestamp(event.getTimeMillis, TimeUnit.MILLISECONDS)
        .setSeverity(mapLogLevelToSeverity(event.getLevel.toString))
        .setBody(event.getMessage.getFormattedMessage)

    // Set attributes individually; do not use setAllAttributes
    logRecordBuilder
      .setAttribute("level", event.getLevel.toString)
      .setAttribute("logger.name", event.getLoggerName)
      .setAttribute("thread.name", event.getThreadName)

    if (currentSpan.getSpanContext.isValid) {
      logRecordBuilder.setAttribute("dd.trace_id", traceId) // Datadog expects this
      logRecordBuilder.setAttribute("dd.span_id", spanId) // Datadog expects this
      logRecordBuilder.setAttribute("trace_id", traceId) // Standard OTel
      logRecordBuilder.setAttribute("span_id", spanId) // Standard OTel
    }

    logRecordBuilder.emit()
  } catch {
    case ex: Exception =>
      System.err.println(s"OpenTelemetryAppender failed: ${ex.getMessage}")
  }
}


  private def mapLogLevelToSeverity(level: String): Severity =
    level.toUpperCase match {
      case "TRACE" => Severity.TRACE
      case "DEBUG" => Severity.DEBUG
      case "INFO"  => Severity.INFO
      case "WARN"  => Severity.WARN
      case "ERROR" => Severity.ERROR
      case "FATAL" => Severity.FATAL
      case _       => Severity.UNDEFINED_SEVERITY_NUMBER
    }
}

object OpenTelemetryAppender {

  @PluginFactory
  def createAppender(
                      @PluginAttribute("name") name: String,
                      @PluginElement("Filter") filter: Filter,
                      @PluginElement("Layout") layout: Layout[_ <: Serializable]
                    ): OpenTelemetryAppender = {
    val finalLayout =
      if (layout == null) PatternLayout.createDefaultLayout()
      else layout

    new OpenTelemetryAppender(name, filter, finalLayout)
  }
}
