package com.example.listeners

import java.util.concurrent.atomic.AtomicInteger
import com.example.exporters.FileExporter
import io.opentelemetry.api.{GlobalOpenTelemetry, OpenTelemetry}
import io.opentelemetry.api.trace.{Span, SpanKind, StatusCode, Tracer}
import io.opentelemetry.context.{Context, Scope}
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.trace.SdkTracerProvider
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor
import org.apache.spark.SparkConf

import java.time.Duration
import org.apache.spark.scheduler._
import com.example.exporters.{FileExporter, LogExporter}
import io.opentelemetry.api.common.{AttributeKey, Attributes}
import io.opentelemetry.sdk.logs.SdkLoggerProvider
import io.opentelemetry.sdk.logs.export.SimpleLogRecordProcessor
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter
import io.opentelemetry.exporter.otlp.logs.OtlpGrpcLogRecordExporter
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter
import io.opentelemetry.sdk.metrics.SdkMeterProvider
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader
import io.opentelemetry.api.metrics.Meter
import io.opentelemetry.sdk.resources.Resource

import java.util.concurrent.ConcurrentHashMap
import org.apache.spark.sql.execution.ui.{SparkListenerSQLExecutionEnd, SparkListenerSQLExecutionStart}

import scala.collection.mutable

object OtelSparkListener {
  @volatile private var init = false
  @volatile var appSpan: Span = _
  @volatile private var tracerProvider: SdkTracerProvider = _
  @volatile private var loggerProvider: SdkLoggerProvider = _
  @volatile var appHasFailed: Boolean = false

  // Shared across all listener instances
  val activeSqlSpans = new ConcurrentHashMap[Long, Span]()

  private val logger = org.apache.logging.log4j.LogManager.getLogger(classOf[OtelSparkListener])

  def setUpDatadog(serviceName: String): Unit = {
    println("OpenTelemetry: Datadog OTLP export ENABLED")
    val resource = Resource.getDefault.merge(
      Resource.create(Attributes.of(AttributeKey.stringKey("service.name"), serviceName))
    )

    val spanExporter = OtlpGrpcSpanExporter.builder()
      .setEndpoint("http://localhost:14317")
      .build()

    val logExporter = OtlpGrpcLogRecordExporter.builder()
      .setEndpoint("http://localhost:14317")
      .build()

    val metricExporter = OtlpGrpcMetricExporter.builder()
      .setEndpoint("http://localhost:14317")
      .build()

    val tracerProvider = SdkTracerProvider.builder()
      .addSpanProcessor(SimpleSpanProcessor.create(spanExporter))
      .setResource(resource)
      .build()

    val loggerProvider = SdkLoggerProvider.builder()
      .addLogRecordProcessor(SimpleLogRecordProcessor.create(logExporter))
      .setResource(resource)
      .build()

    val meterProvider = SdkMeterProvider.builder()
      .registerMetricReader(
        PeriodicMetricReader.builder(metricExporter)
          .setInterval(Duration.ofSeconds(10))
          .build()
      )
      .setResource(resource)
      .build()

    OtelSparkListener.tracerProvider = tracerProvider
    OtelSparkListener.loggerProvider = loggerProvider

    val otel: OpenTelemetry = OpenTelemetrySdk.builder()
      .setTracerProvider(tracerProvider)
      .setLoggerProvider(loggerProvider)
      .setMeterProvider(meterProvider)
      .buildAndRegisterGlobal()

    println("OpenTelemetry: Datadog OTLP exporter configured")
  }

  def setUpFileExps(): Unit = {
    val spanExporter = new FileExporter("data/spans.json")
    val tracerProvider = SdkTracerProvider.builder()
      .addSpanProcessor(SimpleSpanProcessor.create(spanExporter))
      .build()

    val logExporter = new LogExporter("data/logs.json")
    val loggerProvider = SdkLoggerProvider.builder()
      .addLogRecordProcessor(SimpleLogRecordProcessor.create(logExporter))
      .build()

    val otel: OpenTelemetry = OpenTelemetrySdk.builder()
      .setTracerProvider(tracerProvider)
      .setLoggerProvider(loggerProvider)
      .buildAndRegisterGlobal()
  }

  def init_tracer(serviceName: String = "default-spark-app"): Unit = this.synchronized {
    if (init) return
    println("Registering Global OpenTelemetry")

    Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler {
      override def uncaughtException(t: Thread, e: Throwable): Unit = {
        OtelSparkListener.appHasFailed = true



        val msg = s"Uncaught Exception: ${Option(e.getMessage).getOrElse(e.getClass.getName)}"
        logger.error(s"[OtelSparkListener] $msg", e)

        if (OtelSparkListener.appSpan != null) {
          OtelSparkListener.appSpan.setAttribute("error", true) // <-- add this
          OtelSparkListener.appSpan.setStatus(StatusCode.ERROR, msg)
          OtelSparkListener.appSpan.recordException(e)
        }


        val tracer: Tracer = GlobalOpenTelemetry.getTracer("spark-otel-listener")
        val parentCtx: Context =
          if (OtelSparkListener.appSpan != null) Context.root().`with`(OtelSparkListener.appSpan)
          else Context.current()

        val errSpan: Span =
          tracer.spanBuilder("Spark Driver Failure")
            .setParent(parentCtx)
            .startSpan()


        try {
          errSpan.setAttribute("spark.error.thread", t.getName)
          errSpan.setAttribute("error", true)
          errSpan.recordException(e)
          errSpan.setStatus(StatusCode.ERROR, Option(e.getMessage).getOrElse(e.getClass.getName))
        } finally {
          errSpan.end()
        }
        if (OtelSparkListener.appSpan != null) {
          OtelSparkListener.appSpan.setStatus(StatusCode.ERROR, msg)
          OtelSparkListener.appSpan.recordException(e)
        }
      }
    })

    val useJavaAgent = Option(System.getProperty("otel.listener.useJavaAgent")).forall(_.equalsIgnoreCase("true"))
    if (useJavaAgent) {
      println("OpenTelemetry: using Java agent (skipping SDK/exporter setup in listener)")
      init = true
      return
    }

    val datadogEnabled = Option(System.getProperty("otel.datadog.enabled")).exists(_.equalsIgnoreCase("true"))
    if (datadogEnabled) {
      setUpDatadog(serviceName)
      init = true
    } else {
      println("OpenTelemetry: Datadog OTLP export DISABLED")
      setUpFileExps()
      init = true
    }
  }

  def getAppContext: Context = {
    if (appSpan != null) Context.root().`with`(appSpan) else Context.root()
  }

  def shutdown(): Unit = {
    if (tracerProvider != null) {
      tracerProvider.forceFlush()
      tracerProvider.shutdown()
    }
    if (loggerProvider != null) {
      loggerProvider.forceFlush()
      loggerProvider.shutdown()
    }
  }
}

class OtelSparkListener(conf: SparkConf) extends SparkListener {
  private val stagesRunning = new AtomicInteger(0)
  private val tasksRunning = new AtomicInteger(0)
  private val executorsLive = new AtomicInteger(0)

  @volatile private var appName: String = "unknown"
  @volatile private var appId: String = "unknown"

  val serviceName = Option(System.getProperty("otel.service.name"))
    .orElse(Option(System.getProperty("spark.app.name")))
    .getOrElse(conf.get("spark.app.name", "default-spark-app"))

  OtelSparkListener.init_tracer(serviceName)

  val logger = OtelSparkListener.logger
  private lazy val tracer: Tracer = GlobalOpenTelemetry.getTracer("spark-otel-listener")
  val meter: Meter = GlobalOpenTelemetry.getMeter("spark-listener")

  private val activeJobSpans = mutable.Map.empty[Int, (Span, Scope)]
  private val activeStageSpans = mutable.Map.empty[Int, Span]
  private val activeTaskSpans = mutable.Map.empty[Long, Span]
  private val stageToJobSpan = new ConcurrentHashMap[Int, Span]()
  private val jobToStageIds = new ConcurrentHashMap[Int, Array[Int]]()

  // Track failures for propagation
  private val failedStageIds = mutable.Set.empty[Int]
  private val failedJobIds = mutable.Set.empty[Int]
  private val failedSqlExecutionIds = mutable.Set.empty[Long]
  private val stageToJobId = new ConcurrentHashMap[Int, Int]()
  private val jobToSqlExecutionId = new ConcurrentHashMap[Int, Long]()

  private def commonAttrs(): Attributes =
    Attributes.builder()
      .put(AttributeKey.stringKey("spark.app.name"), appName)
      .put(AttributeKey.stringKey("spark.app.id"), appId)
      .build()

  meter.gaugeBuilder("spark.stages.running")
    .setUnit("1")
    .buildWithCallback(m => m.record(stagesRunning.get(), commonAttrs()))

  meter.gaugeBuilder("spark.tasks.running")
    .setUnit("1")
    .buildWithCallback(m => m.record(tasksRunning.get(), commonAttrs()))

  meter.gaugeBuilder("spark.executor.count")
    .setUnit("1")
    .buildWithCallback(m => m.record(executorsLive.get(), commonAttrs()))

  private val shuffleReadBytes = meter.counterBuilder("spark.shuffle.read.bytes").setUnit("bytes").build()
  private val shuffleWriteBytes = meter.counterBuilder("spark.shuffle.write.bytes").setUnit("bytes").build()

  private var jobCount: Long = 0L
  private var stageCount: Long = 0L
  private var taskCount: Long = 0L
  private var failedJobCount: Long = 0L
  private var failedStageCount: Long = 0L
  private var failedTaskCount: Long = 0L
  private var executorAddedCount: Long = 0L
  private var executorRemovedCount: Long = 0L

  //Helper methods for error propagation
  private def markJobFailed(jobId: Int, reason: String): Unit = {
    if (!failedJobIds.contains(jobId)) {
      failedJobIds.add(jobId)
      logger.error(s"[OtelSparkListener] Job $jobId marked as FAILED: $reason")

      // Mark the SQL execution that spawned this job
      Option(jobToSqlExecutionId.get(jobId)).foreach { sqlId =>
        markSqlExecutionFailed(sqlId, s"Job $jobId failed: $reason")
      }
    }
  }

  private def markStageFailed(stageId: Int, reason: String): Unit = {
    if (!failedStageIds.contains(stageId)) {
      failedStageIds.add(stageId)
      logger.error(s"[OtelSparkListener] Stage $stageId marked as FAILED: $reason")

      // Mark the parent job
      Option(stageToJobSpan.get(stageId)).foreach { jobSpan =>
        jobSpan.setAttribute("error", true)
        jobSpan.setStatus(StatusCode.ERROR, s"Stage failed: $reason")
      }

      // Mark the job ID
      Option(stageToJobId.get(stageId)).foreach { jobId =>
        markJobFailed(jobId, s"Stage $stageId failed: $reason")
      }
    }
  }

  private def markSqlExecutionFailed(executionId: Long, reason: String): Unit = {
    if (!failedSqlExecutionIds.contains(executionId)) {
      failedSqlExecutionIds.add(executionId)
      logger.error(s"[OtelSparkListener] SQL Execution $executionId marked as FAILED: $reason")

      Option(OtelSparkListener.activeSqlSpans.get(executionId)).foreach { sqlSpan =>
        sqlSpan.setAttribute("error", true)
        sqlSpan.setStatus(StatusCode.ERROR, reason)
      }
    }
  }

  private def createFallbackTaskSpan(taskInfo: TaskInfo, taskEnd: SparkListenerTaskEnd, taskStatus: String): Unit = {
    val stageId = taskEnd.stageId
    val m = taskEnd.taskMetrics

    logger.warn(s"[OtelSparkListener] Creating FALLBACK span for failed Task ${taskInfo.taskId} in Stage $stageId")

    val fallbackSpan = tracer.spanBuilder(s"Spark Task (recovered): ${taskInfo.taskId}")
      .setParent(OtelSparkListener.getAppContext)
      .setAttribute("spark.task.id", taskInfo.taskId)
      .setAttribute("spark.task.status", taskStatus)
      .setAttribute("spark.task.failed", true)
      .setAttribute("spark.task.endReason", taskEnd.reason.toString)
      .setAttribute("spark.stage.id", stageId)
      .setAttribute("spark.task.executorId", taskInfo.executorId)
      .setAttribute("spark.task.host", taskInfo.host)
      .setAttribute("spark.task.index", taskInfo.index)
      .setAttribute("spark.task.attemptNumber", taskInfo.attemptNumber)
      .setAttribute("error", true)
      .startSpan()

    // Add metrics if available
    if (m != null) {
      fallbackSpan.setAttribute("spark.task.executorRunTime_ms", m.executorRunTime)
      fallbackSpan.setAttribute("spark.task.jvmGcTime_ms", m.jvmGCTime)
      fallbackSpan.setAttribute("spark.task.memBytesSpilled", m.memoryBytesSpilled)
      fallbackSpan.setAttribute("spark.task.diskBytesSpilled", m.diskBytesSpilled)
    }

    fallbackSpan.setStatus(StatusCode.ERROR, s"Task failed: ${taskInfo.status}")
    fallbackSpan.end()
  }

  override def onApplicationStart(appStart: SparkListenerApplicationStart): Unit = {
    appName = appStart.appName
    appId = appStart.appId.getOrElse("unknown")
    println(s"[OtelListener] App started: ${appName}, id=${appId}")

    val span = tracer.spanBuilder(s"Spark Application: $appName")
      .setNoParent()
      .setSpanKind(SpanKind.SERVER) // <-- helps Datadog treat it like an entry span
      .setAttribute("spark.app.id", appId)
      .setAttribute("spark.app.name", appName)
      .setAttribute("spark.app.startTime", appStart.time)
      .startSpan()

    OtelSparkListener.appSpan = span
  }

  override def onApplicationEnd(appEnd: SparkListenerApplicationEnd): Unit = {
    Option(OtelSparkListener.appSpan).foreach { span =>
      val failed = OtelSparkListener.appHasFailed || failedJobCount > 0 || failedStageCount > 0 || failedTaskCount > 0

      span.setAttribute("spark.app.status", if (failed) "FAILED" else "SUCCEEDED")
      span.setAttribute("error", failed)

      if (failed) {
        span.setStatus(StatusCode.ERROR, "Application failed")
        span.recordException(new Exception("Application failed"))
      }

      span.end()
      OtelSparkListener.appSpan = null
    }
  }


  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    jobCount += 1
    println(s"[OtelListener] Job started: jobId=${jobStart.jobId}, stages=${jobStart.stageInfos.map(_.stageId).mkString(",")}")

    val sqlExecIdOpt: Option[Long] =
      Option(jobStart.properties)
        .flatMap(p => Option(p.getProperty("spark.sql.execution.id")))
        .flatMap(s => scala.util.Try(s.toLong).toOption)

    val parentContext: Context =
      sqlExecIdOpt.flatMap(id => Option(OtelSparkListener.activeSqlSpans.get(id))) match {
        case Some(sqlSpan) => Context.root().`with`(sqlSpan)
        case None => OtelSparkListener.getAppContext
      }

    val span = tracer.spanBuilder(s"Spark Job: ${jobStart.jobId}")
      .setParent(parentContext)
      .setAttribute("spark.job.id", jobStart.jobId)
      .setAttribute("spark.job.stageIds", jobStart.stageIds.mkString(","))
      .setAttribute("spark.job.time", jobStart.time)
      .startSpan()

    val stageIdsArr = jobStart.stageIds.toArray
    jobToStageIds.put(jobStart.jobId, stageIdsArr)
    stageIdsArr.foreach { stageId =>
      stageToJobSpan.put(stageId, span)
      stageToJobId.put(stageId, jobStart.jobId)
    }

    // Track SQL execution ID for this job
    sqlExecIdOpt.foreach { sqlId =>
      jobToSqlExecutionId.put(jobStart.jobId, sqlId)
    }

    val scope = span.makeCurrent()
    activeJobSpans.put(jobStart.jobId, (span, scope))
  }
  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    activeJobSpans.remove(jobEnd.jobId) match {
      case Some((span, scope)) =>
        try {
          scope.close()

          jobEnd.jobResult match {
            case JobSucceeded =>
              span.setAttribute("spark.job.status", "succeeded")
              span.setAttribute("error", false)

            case other =>
              failedJobCount += 1
              val reason = other.toString
              span.setAttribute("spark.job.status", "failed")
              span.setAttribute("error", true)
              span.setStatus(StatusCode.ERROR, reason)
          }
        } finally {
          span.end()
        }

      case None =>
      // no-op
    }
  }


  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    stagesRunning.incrementAndGet()
    stageCount += 1
    val stageInfo = stageSubmitted.stageInfo
    println(s"[OtelListener] Stage submitted: stageId=${stageInfo.stageId}, name=${stageInfo.name}")

    val jobSpan = stageToJobSpan.get(stageInfo.stageId)
    val parentContext: Context =
      if (jobSpan != null) Context.root().`with`(jobSpan)
      else OtelSparkListener.getAppContext

    val span = tracer.spanBuilder(s"Spark Stage: ${stageInfo.stageId}")
      .setParent(parentContext)
      .setAttribute("spark.stage.id", stageInfo.stageId)
      .setAttribute("spark.stage.name", stageInfo.name)
      .setAttribute("spark.stage.numTasks", stageInfo.numTasks)
      .startSpan()

    activeStageSpans.put(stageInfo.stageId, span)
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    stagesRunning.updateAndGet(v => Math.max(0, v - 1))
    val si = stageCompleted.stageInfo
    val stageId = si.stageId
    val reasonOpt = si.failureReason

    activeStageSpans.remove(stageId).foreach { span =>
      reasonOpt match {
        case Some(reason) =>
          failedStageCount += 1
          span.setAttribute("spark.stage.status", "FAILED")
          span.setAttribute("spark.stage.failureReason", reason)
          span.recordException(new Exception(reason))
          span.setStatus(StatusCode.ERROR, reason)

        case None =>
          span.setAttribute("spark.stage.status", "SUCCESS")
      }
      span.end()
    }
  }


  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
    tasksRunning.incrementAndGet()
    taskCount += 1
    val taskInfo = taskStart.taskInfo
    val stageId = taskStart.stageId
    println(
      s"[OtelListener] Task start: taskId=${taskInfo.taskId}, " +
        s"stageId=$stageId, host=${taskInfo.host}, executor=${taskInfo.executorId}"
    )

    val parentSpanOpt = activeStageSpans.get(stageId)
    val parentContext: Context =
      parentSpanOpt match {
        case Some(stageSpan) => Context.root().`with`(stageSpan)
        case None => OtelSparkListener.getAppContext
      }

    val sb = tracer.spanBuilder(s"Spark Task: ${taskInfo.taskId}")
      .setParent(parentContext)

    val sb2 =
      if (taskInfo.launchTime > 0)
        sb.setStartTimestamp(java.time.Instant.ofEpochMilli(taskInfo.launchTime))
      else
        sb

    val span = sb2
      .setAttribute("spark.task.id", taskInfo.taskId)
      .setAttribute("spark.task.stageId", stageId)
      .setAttribute("spark.task.host", taskInfo.host)
      .setAttribute("spark.task.executorId", taskInfo.executorId)
      .setAttribute("spark.task.index", taskInfo.index)
      .setAttribute("spark.task.attemptNumber", taskInfo.attemptNumber)
      .setAttribute("spark.task.speculative", taskInfo.speculative)
      .setAttribute("spark.task.locality", taskInfo.taskLocality.toString)
      .startSpan()

    activeTaskSpans.put(taskInfo.taskId, span)
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    val ti = taskEnd.taskInfo
    val stageId = taskEnd.stageId
    tasksRunning.updateAndGet(v => Math.max(0, v - 1))

    val spanOpt = activeTaskSpans.remove(ti.taskId)

    spanOpt match {
      case Some(span) =>
        val failed = ti.failed
        val status = if (failed) "FAILED" else "SUCCESS"

        span.setAttribute("spark.task.status", status)
        span.setAttribute("spark.task.failed", failed)
        span.setAttribute("spark.task.endReason", taskEnd.reason.toString)
        span.setAttribute("spark.task.stageId", stageId)

        if (failed) {
          failedTaskCount += 1
          span.setAttribute("error", true)
          span.recordException(new Exception(taskEnd.reason.toString))
          span.setStatus(StatusCode.ERROR, taskEnd.reason.toString)
          markStageFailed(stageId, s"Task ${ti.taskId} failed")
        } else {
          span.setAttribute("error", false)
        }

        span.end()

      case None =>
        // only if we truly missed creating the span
        if (ti.failed) {
          failedTaskCount += 1
          createFallbackTaskSpan(ti, taskEnd, "FAILED")
          markStageFailed(stageId, s"Task ${ti.taskId} failed (missing span)")
        }
    }
  }


  override def onTaskGettingResult(taskGettingResult: SparkListenerTaskGettingResult): Unit = {
    val taskInfo = taskGettingResult.taskInfo
    println(
      s"[OtelListener] Task getting result: taskId=${taskInfo.taskId}, " +
        s"host=${taskInfo.host}, index=${taskInfo.index}, executor=${taskInfo.executorId}"
    )

    val parentContext: Context = OtelSparkListener.getAppContext
    val span = tracer.spanBuilder(s"Spark Task Getting Result: ${taskInfo.taskId}")
      .setParent(parentContext)
      .setAttribute("spark.task.id", taskInfo.taskId)
      .setAttribute("spark.task.host", taskInfo.host)
      .setAttribute("spark.task.executorId", taskInfo.executorId)
      .setAttribute("spark.task.index", taskInfo.index)
      .startSpan()
    span.end()
  }

  override def onEnvironmentUpdate(environmentUpdate: SparkListenerEnvironmentUpdate): Unit = {
    println("[OtelListener] Environment update received")
    val parentContext: Context = OtelSparkListener.getAppContext
    val span = tracer.spanBuilder("Spark Environment Update")
      .setParent(parentContext)
      .startSpan()
    try {
      val envDetails = environmentUpdate.environmentDetails
      envDetails.get("Spark Properties").foreach { entries =>
        entries.foreach { case (k, v) =>
          if (!k.toLowerCase.contains("password") && !k.toLowerCase.contains("secret")) {
            span.setAttribute(s"spark.env.spark.$k", v)
          }
        }
      }
      envDetails.get("System Properties").foreach { entries =>
        entries.foreach { case (k, v) =>
          if (!k.toLowerCase.contains("password") && !k.toLowerCase.contains("secret")) {
            span.setAttribute(s"spark.env.system.$k", v)
          }
        }
      }
    } finally {
      span.end()
    }
  }

  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
    executorsLive.incrementAndGet()
    executorAddedCount += 1
    val info = executorAdded.executorInfo
    println(
      s"[OtelListener] Executor added: id=${executorAdded.executorId}, " +
        s"host=${info.executorHost}, cores=${info.totalCores}"
    )

    val parentContext: Context = OtelSparkListener.getAppContext
    val span = tracer.spanBuilder(s"Spark Executor Added: ${executorAdded.executorId}")
      .setParent(parentContext)
      .setAttribute("spark.executor.id", executorAdded.executorId)
      .setAttribute("spark.executor.host", info.executorHost)
      .setAttribute("spark.executor.cores", info.totalCores)
      .setAttribute("spark.executor.startTime", executorAdded.time)
      .startSpan()
    span.end()
  }

  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {
    executorsLive.updateAndGet(v => Math.max(0, v - 1))
    executorRemovedCount += 1
    println(
      s"[OtelListener] Executor removed: id=${executorRemoved.executorId}, reason=${executorRemoved.reason}"
    )

    val parentContext: Context = OtelSparkListener.getAppContext
    val span = tracer.spanBuilder(s"Spark Executor Removed: ${executorRemoved.executorId}")
      .setParent(parentContext)
      .setAttribute("spark.executor.id", executorRemoved.executorId)
      .setAttribute("spark.executor.removedTime", executorRemoved.time)
      .setAttribute("spark.executor.removedReason", executorRemoved.reason)
      .startSpan()
    span.end()
  }

  private def extractOps(plan: String, maxOps: Int = 25): String = {
    if (plan == null) return ""
    val ops = plan
      .split("\n")
      .iterator
      .map(_.trim)
      .filter(l => l.startsWith("+-") || l.startsWith(":-") || l.startsWith("*("))
      .map(l => l.replaceFirst("""^[\|\s]*[+:\-]*\s*""", ""))
      .map(l => l.replaceFirst("""^\*\(\d+\)\s*""", ""))
      .map { l =>
        val first = l.takeWhile(c => c != ' ' && c != '(')
        first
      }
      .filter(_.nonEmpty)
      .toList
      .distinct
      .take(maxOps)
    ops.mkString(" -> ")
  }

  private def parseDesc(desc: String): (String, String) = {
    val parts = Option(desc).getOrElse("").split(" at ", 2)
    val action = parts.headOption.getOrElse(desc)
    val callsite = if (parts.length == 2) parts(1) else ""
    (action, callsite)
  }

  private def extractSources(plan: String, max: Int = 10): Seq[String] = {
    val p = Option(plan).getOrElse("")
    p.split("\n")
      .iterator
      .map(_.trim)
      .flatMap { line =>
        val toks = line.split("\\s+")
        if (toks.length >= 2 && toks(0).equalsIgnoreCase("Scan")) Some(toks(1))
        else if (toks.length >= 2 && toks(0).equalsIgnoreCase("FileScan")) Some(toks(1))
        else None
      }
      .map(_.replaceAll("[^A-Za-z0-9_]", ""))
      .filter(_.nonEmpty)
      .toSeq
      .distinct
      .take(max)
  }

  private def isWritePlan(plan: String): Boolean = {
    val p = Option(plan).getOrElse("").toLowerCase
    p.contains("insertintohadoopfsrelationcommand") ||
      p.contains("saveintodatasourcecommand") ||
      p.contains("executewrite") ||
      p.contains("writefiles") ||
      p.contains("execute insertinto")
  }

  private def pickPrimaryLabel(ops: String, plan: String): String = {
    val o = Option(ops).getOrElse("")
    val p = Option(plan).getOrElse("").toLowerCase

    if (isWritePlan(plan)) "WRITE"
    else if (o.contains("Join")) "JOIN"
    else if (o.contains("Aggregate")) "AGG"
    else if (o.contains("Exchange")) "SHUFFLE"
    else if (o.contains("Window")) "WINDOW"
    else if (o.contains("Sort")) "SORT"
    else {
      val readish =
        p.contains(" scan ") &&
          !p.contains(" join") &&
          !p.contains("aggregate") &&
          !p.contains("exchange") &&
          !p.contains("window") &&
          !p.contains("sort") &&
          !p.contains("insertinto") &&
          !p.contains("saveinto")
      if (readish) "READ" else "TRANSFORM"
    }
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
    case e: SparkListenerSQLExecutionStart =>
      val plan = Option(e.physicalPlanDescription).getOrElse("")
      val ops = extractOps(e.physicalPlanDescription)
      val sources = extractSources(plan)
      val (action, callsite) = parseDesc(e.description)
      val label = pickPrimaryLabel(ops, plan)

      val spanName = label match {
        case "WRITE" =>
          s"Spark WRITE: $action"
        case "READ" =>
          val srcPart = if (sources.nonEmpty) s" (${sources.size} src)" else ""
          s"Spark READ: $action$srcPart"
        case other =>
          val srcPart = if (sources.nonEmpty) s" (${sources.size} src)" else ""
          s"Spark $other: $action$srcPart"
      }

      val sqlSpan = tracer.spanBuilder(spanName)
        .setParent(OtelSparkListener.getAppContext)
        .setAttribute("spark.sql.execution.id", e.executionId)
        .setAttribute("spark.sql.action", action)
        .setAttribute("spark.sql.callsite", callsite)
        .setAttribute("spark.sql.label", label)
        .setAttribute("spark.sql.ops", ops)
        .setAttribute("spark.sql.source.count", sources.size.toLong)
        .setAttribute("spark.sql.sources", sources.mkString(","))
        .startSpan()

      val planShort = plan.take(3000)
      if (planShort.nonEmpty) sqlSpan.setAttribute("spark.sql.plan", planShort)

      OtelSparkListener.activeSqlSpans.put(e.executionId, sqlSpan)

    case e: SparkListenerSQLExecutionEnd =>
      val s = OtelSparkListener.activeSqlSpans.remove(e.executionId)
      if (s != null) {
        if (failedJobCount > 0 || failedStageCount > 0 || failedTaskCount > 0) {
          s.setAttribute("error", true)
          s.setStatus(StatusCode.ERROR, "SQL execution ended with task/stage/job failures")
          logger.error(s"[OtelSparkListener] SQL Execution ${e.executionId} FAILED due to child component failures")
        } else {
          s.setAttribute("error", false)
        }
        s.end()
      }

    case _ =>
  }
}
