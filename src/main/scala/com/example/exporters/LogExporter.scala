package com.example.exporters

import io.opentelemetry.sdk.common.CompletableResultCode
import io.opentelemetry.sdk.logs.`export`.LogRecordExporter
import io.opentelemetry.sdk.logs.data.LogRecordData

import java.io.{FileWriter, PrintWriter}
import java.util
import scala.collection.JavaConverters._

class LogExporter(path: String) extends LogRecordExporter {

  override def `export`(logs: util.Collection[LogRecordData]): CompletableResultCode = {
    val fw  = new FileWriter(path, true)
    val out = new PrintWriter(fw)

    try {
      logs.asScala.foreach { log =>
        val attrs = log.getAttributes.asMap().asScala

        val attrsJson = attrs.map { case (k, v) =>
          val key   = k.getKey.replace("\"", "\\\"")
          val value = String.valueOf(v).replace("\"", "\\\"")
          s"""\"$key\":\"$value\""""
        }.mkString(",")

        val jsonString =
          s"""{
             | "timestamp_ns": ${log.getTimestampEpochNanos},
             | "severity": "${log.getSeverity}",
             | "trace_id": "${log.getSpanContext.getTraceId}",
             | "span_id": "${log.getSpanContext.getSpanId}",
             | "body": "${log.getBody.asString().replace("\"", "\\\"")}",
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
