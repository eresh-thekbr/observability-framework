package com.example.exporters

import io.opentelemetry.sdk.common.CompletableResultCode
import io.opentelemetry.sdk.metrics.InstrumentType
import io.opentelemetry.sdk.metrics.data.{AggregationTemporality, MetricData, MetricDataType}
import io.opentelemetry.sdk.metrics.export.MetricExporter
import java.io.{FileWriter, PrintWriter}
import java.util
import scala.collection.JavaConverters._

class MetricFileExporter(path: String) extends MetricExporter {

  new java.io.File(path).getParentFile.mkdirs()
  new FileWriter(path, false).close()

  override def export(metrics: util.Collection[MetricData]): CompletableResultCode = {
    val fw = new FileWriter(path, true)
    val out = new PrintWriter(fw)
    try {
      metrics.asScala.foreach { metric =>
        val points = metric.getType match {
          case MetricDataType.LONG_GAUGE =>
            metric.getLongGaugeData.getPoints.asScala.map { p =>
              s"""{"value":${p.getValue},"epoch_ns":${p.getEpochNanos},"start_ns":${p.getStartEpochNanos}}"""
            }.mkString(",")
          case MetricDataType.DOUBLE_GAUGE =>
            metric.getDoubleGaugeData.getPoints.asScala.map { p =>
              s"""{"value":${p.getValue},"epoch_ns":${p.getEpochNanos},"start_ns":${p.getStartEpochNanos}}"""
            }.mkString(",")
          case MetricDataType.LONG_SUM =>
            metric.getLongSumData.getPoints.asScala.map { p =>
              s"""{"value":${p.getValue},"epoch_ns":${p.getEpochNanos},"start_ns":${p.getStartEpochNanos}}"""
            }.mkString(",")
          case MetricDataType.DOUBLE_SUM =>
            metric.getDoubleSumData.getPoints.asScala.map { p =>
              s"""{"value":${p.getValue},"epoch_ns":${p.getEpochNanos},"start_ns":${p.getStartEpochNanos}}"""
            }.mkString(",")
          case MetricDataType.HISTOGRAM =>
            metric.getHistogramData.getPoints.asScala.map { p =>
              s"""{"sum":${p.getSum},"count":${p.getCount},"min":${p.getMin},"max":${p.getMax},"epoch_ns":${p.getEpochNanos}}"""
            }.mkString(",")
          case _ => ""
        }
        val json =
          s"""{"name":"${metric.getName}","unit":"${metric.getUnit}","type":"${metric.getType}","points":[$points]}"""
        out.println(json)
      }
    } finally {
      out.flush(); out.close(); fw.close()
    }
    CompletableResultCode.ofSuccess()
  }

  override def flush(): CompletableResultCode = CompletableResultCode.ofSuccess()
  override def shutdown(): CompletableResultCode = CompletableResultCode.ofSuccess()

  override def getAggregationTemporality(instrumentType: InstrumentType): AggregationTemporality =
    AggregationTemporality.CUMULATIVE
}