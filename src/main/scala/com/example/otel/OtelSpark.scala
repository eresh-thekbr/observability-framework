package com.example.otel

import com.example.listeners.OtelSparkListener
import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.api.common.{AttributeKey, Attributes, AttributesBuilder}
import io.opentelemetry.api.metrics.LongCounter
import io.opentelemetry.api.trace.{Span, StatusCode}
import io.opentelemetry.context.Context

import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters._

/**
 * Public API for application code: add business context to the run's trace.
 *
 * The Spark listener already records App → SQL → Job → Stage → Task automatically.
 * Use this when the business meaning matters too:
 *
 * {{{
 *   import com.example.otel.OtelSpark
 *
 *   OtelSpark.attribute("business.date", "2024-06-01")          // tag the whole run
 *   val flagged = OtelSpark.step("sanctions-screening") {         // custom span around a step
 *     val n = enriched.join(broadcast(blacklist), "account_id").count()
 *     OtelSpark.event("sanctions.hits", "count" -> n)             // milestone on the current span
 *     n
 *   }
 *   OtelSpark.count("fraud.flagged.transactions", flagged)        // business metric
 * }}}
 *
 * From Java: `OtelSpark.event("sanctions.hits", Map.of("count", n))`,
 * `OtelSpark.step("load", () -> df.count())`.
 *
 * Everything is a safe no-op when OpenTelemetry is not configured, so the calls can stay in
 * production code.
 */
object OtelSpark {

  private lazy val tracer = GlobalOpenTelemetry.getTracer("otelspark-user-api")
  private lazy val meter = GlobalOpenTelemetry.getMeter("otelspark-user-api")
  private val counters = new ConcurrentHashMap[String, LongCounter]()

  /** Adds a timestamped event to the current step span, or to the application span. */
  def event(name: String, attributes: (String, Any)*): Unit =
    currentSpan.addEvent(name, toAttributes(attributes))

  /** Java-friendly overload of [[event]]. */
  def event(name: String, attributes: java.util.Map[String, Object]): Unit =
    event(name, attributes.asScala.toSeq: _*)

  /** Sets an attribute on the application span, i.e. on the run as a whole. */
  def attribute(key: String, value: Any): Unit =
    put(appSpan, key, value)

  /**
   * Runs `body` inside a custom child span named `name`. Events added inside go on this span;
   * an exception marks it ERROR, records the stack trace, and is rethrown.
   */
  def step[T](name: String, attributes: (String, Any)*)(body: => T): T = {
    val parent = if (Span.current().getSpanContext.isValid) Context.current() else OtelSparkListener.getAppContext
    val span = tracer.spanBuilder(name).setParent(parent).setAllAttributes(toAttributes(attributes)).startSpan()
    span.setAttribute("otelspark.step", true)
    val scope = span.makeCurrent()
    try body
    catch {
      case e: Throwable =>
        span.recordException(e) // full stack trace goes into the exception event
        span.setStatus(StatusCode.ERROR, rootCause(e))
        throw e
    } finally {
      scope.close()
      span.end()
    }
  }

  /** Java-friendly overload of [[step]]. */
  def step[T](name: String, body: java.util.function.Supplier[T]): T = step(name)(body.get())

  /** Adds `value` to a monotonic business counter, e.g. records processed or rejected. */
  def count(name: String, value: Long, attributes: (String, Any)*): Unit =
    counters.computeIfAbsent(name, n => meter.counterBuilder(n).build())
      .add(value, toAttributes(attributes :+ ("spark.app.name" -> appName)))

  // ── internals ────────────────────────────────────────────────────────────────────────────────

  /** First line of the innermost cause: readable in a status field, unlike Spark's full stack text. */
  private def rootCause(e: Throwable): String = {
    var t = e
    while (t.getCause != null && (t.getCause ne t)) t = t.getCause
    val msg = Option(t.getMessage).getOrElse(t.getClass.getName).linesIterator.next()
    s"${t.getClass.getSimpleName}: $msg".take(300)
  }

  private def appSpan: Span = Option(OtelSparkListener.appSpan).getOrElse(Span.getInvalid)

  private def currentSpan: Span = {
    val s = Span.current()
    if (s.getSpanContext.isValid) s else appSpan
  }

  private def appName: String =
    Option(OtelSparkListener.appSpan).map(_ => sys.props.getOrElse("otel.service.name",
      sys.env.getOrElse("OTEL_SERVICE_NAME", "unknown"))).getOrElse("unknown")

  private def put(span: Span, key: String, value: Any): Unit = value match {
    case v: String            => span.setAttribute(key, v)
    case v: Int               => span.setAttribute(key, v.toLong)
    case v: Long              => span.setAttribute(key, v)
    case v: Double            => span.setAttribute(key, v)
    case v: Float             => span.setAttribute(key, v.toDouble)
    case v: Boolean           => span.setAttribute(key, v)
    case v: java.lang.Number  => span.setAttribute(key, v.doubleValue())
    case null                 => ()
    case v                    => span.setAttribute(key, v.toString)
  }

  private def toAttributes(kvs: Seq[(String, Any)]): Attributes = {
    val b: AttributesBuilder = Attributes.builder()
    kvs.foreach {
      case (k, v: String)           => b.put(AttributeKey.stringKey(k), v)
      case (k, v: Int)              => b.put(AttributeKey.longKey(k), Long.box(v.toLong))
      case (k, v: Long)             => b.put(AttributeKey.longKey(k), Long.box(v))
      case (k, v: Double)           => b.put(AttributeKey.doubleKey(k), Double.box(v))
      case (k, v: Boolean)          => b.put(AttributeKey.booleanKey(k), Boolean.box(v))
      case (k, v: java.lang.Number) => b.put(AttributeKey.doubleKey(k), Double.box(v.doubleValue()))
      case (_, null)                => ()
      case (k, v)                   => b.put(AttributeKey.stringKey(k), v.toString)
    }
    b.build()
  }
}
