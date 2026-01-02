package com.example.exporters

import io.opentelemetry.sdk.common.CompletableResultCode
import io.opentelemetry.sdk.trace.export.SpanExporter
import io.opentelemetry.sdk.trace.data.SpanData

import java.io.{FileWriter, PrintWriter}
import java.util
import scala.collection.JavaConverters._

class FileExporter(path: String) extends SpanExporter {

  override def export(spans: util.Collection[SpanData]): CompletableResultCode = {
    val fw  = new FileWriter(path, true)
    val out = new PrintWriter(fw)

    try {
      spans.asScala.foreach { span =>
        val attrs = span.getAttributes.asMap().asScala

        // Very simple JSON for attributes: "key":"value" pairs, values as strings
        val attrsJson = attrs.map { case (k, v) =>
          val key   = k.getKey.replace("\"", "\\\"")
          val value = String.valueOf(v).replace("\"", "\\\"")
          s"""\"$key\":\"$value\""""
        }.mkString(",")

        val jsonString =
          s"""{
             | "trace_id": "${span.getTraceId}",
             | "span_id": "${span.getSpanId}",
             | "parent_span_id": "${span.getParentSpanId}",
             | "name": "${span.getName}",
             | "start_time_ns": ${span.getStartEpochNanos},
             | "end_time_ns": ${span.getEndEpochNanos},
             | "status": "${span.getStatus.getStatusCode}",
             | "description": "${span.getStatus.getDescription}",
             | "attributes": { $attrsJson }
             |}""".stripMargin.replaceAll("\n", " ")
        out.println(jsonString)
      }
    } finally {
      out.flush()
      out.close()
      fw.close()
    }

    CompletableResultCode.ofSuccess()
  }

  override def flush(): CompletableResultCode =
    CompletableResultCode.ofSuccess()

  override def shutdown(): CompletableResultCode =
    CompletableResultCode.ofSuccess()
}
