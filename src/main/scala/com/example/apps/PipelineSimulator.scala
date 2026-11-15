package com.example.apps

import com.example.listeners.OtelSparkListener
import com.example.otel.OtelSpark
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.util.Random

/**
 * Realistic, self-contained Spark workloads used to generate live telemetry for the demo.
 * Data is synthesised in memory, so no input files are needed.
 *
 * Usage: PipelineSimulator <pipeline> <scenario> [rows]
 *   pipeline: fraud-detection | clickstream-funnel | risk-aggregation
 *   scenario: normal | skew | failure
 *
 * Every run is one Spark application, so one trace: App -> SQL -> Job -> Stage -> Task.
 * "skew" piles most rows onto one key (one straggler task per stage);
 * "failure" hits a corrupt record mid-pipeline, so a task, stage, job and the app all end in ERROR.
 */
object PipelineSimulator {

  private val logger = LogManager.getLogger("PipelineSimulator")

  def main(args: Array[String]): Unit = {
    val pipeline = args.lift(0).getOrElse("fraud-detection")
    val scenario = args.lift(1).getOrElse("normal")
    val rows = args.lift(2).map(_.toLong).getOrElse(300000L + Random.nextInt(1200000))

    val spark = SparkSession.builder()
      .appName(pipeline)
      .master("local[4]")
      .config("spark.extraListeners", "com.example.listeners.OtelSparkListener")
      .config("spark.sql.shuffle.partitions", if (scenario == "skew") "32" else "8")
      .config("spark.sql.adaptive.enabled", "false") // keep skew visible as a straggler task
      .config("spark.ui.enabled", "false")
      .getOrCreate()

    logger.info(s"Starting pipeline=$pipeline scenario=$scenario rows=$rows")
    OtelSpark.attribute("pipeline.scenario", scenario)
    OtelSpark.attribute("pipeline.input.rows", rows)
    val t0 = System.currentTimeMillis()
    var exitCode = 0
    try {
      pipeline match {
        case "fraud-detection"    => fraudDetection(spark, rows, scenario)
        case "clickstream-funnel" => clickstreamFunnel(spark, rows, scenario)
        case "risk-aggregation"   => riskAggregation(spark, rows, scenario)
        case other                => throw new IllegalArgumentException(s"Unknown pipeline: $other")
      }
      logger.info(s"Pipeline $pipeline finished in ${System.currentTimeMillis() - t0} ms")
    } catch {
      case e: Exception =>
        exitCode = 1
        OtelSparkListener.appHasFailed = true
        logger.error(s"Pipeline $pipeline FAILED after ${System.currentTimeMillis() - t0} ms: ${rootMessage(e)}", e)
    } finally {
      spark.stop()
    }
    sys.exit(exitCode)
  }

  // ── 1. Card-transaction fraud checks: broadcast join, window, aggregation, write ──────────────
  private def fraudDetection(spark: SparkSession, rows: Long, scenario: String): Unit = {
    import spark.implicits._
    val accounts = spark.range(5000).select(
      $"id".as("account_id"),
      concat(lit("customer-"), $"id").as("customer_name"),
      element_at(array(lit("US"), lit("IN"), lit("GB"), lit("DE"), lit("SG")), ($"id" % 5 + 1).cast("int")).as("country"))
    val blacklist = accounts.sample(0.02, 7).select($"account_id", lit("sanctions match").as("reason"))

    val txns = transactions(spark, rows, scenario)
    logger.info(s"Generated $rows card transactions")

    val enriched = txns.join(accounts, "account_id")
    val flaggedCount = OtelSpark.step("sanctions-screening", "list" -> "internal-blacklist") {
      val n = enriched.join(broadcast(blacklist), "account_id").count()
      OtelSpark.event("sanctions.hits", "count" -> n, "list" -> "internal-blacklist")
      n
    }
    OtelSpark.count("fraud.flagged.transactions", flaggedCount, "pipeline" -> "fraud-detection")
    logger.info(s"Sanctions screening flagged $flaggedCount transactions")
    if (flaggedCount > 0) logger.warn(s"$flaggedCount transactions matched the sanctions list and were held")

    val w = Window.partitionBy("account_id").orderBy("ts")
    val rapid = txns
      .withColumn("prev_ts", lag("ts", 1).over(w))
      .filter($"ts" - $"prev_ts" < 60)
      .count()
    logger.info(s"Rapid-fire detector found $rapid suspicious transactions")

    OtelSpark.step("exposure-report") {
    val exposure = guard(enriched, scenario)
      .filter($"amount" > 4000)
      .groupBy("country")
      .agg(count("*").as("txn_count"), sum("amount").as("total_volume"))
      .orderBy(desc("total_volume"))
    exposure.write.mode(SaveMode.Overwrite).parquet("/tmp/output/fraud_exposure")
    OtelSpark.event("report.published", "path" -> "/tmp/output/fraud_exposure")
    }
    logger.info("High-value exposure by country written")
  }

  // ── 2. E-commerce clickstream: lead/lag funnel, pivot, conversion KPIs ─────────────────────────
  private def clickstreamFunnel(spark: SparkSession, rows: Long, scenario: String): Unit = {
    import spark.implicits._
    val events = spark.range(rows).select(
      ($"id" % (rows / 8 + 1)).as("session_id"),
      (unix_timestamp(lit("2019-11-01 00:00:00")) + (rand(11) * 86400 * 30).cast("long")).as("ts"),
      when(rand(12) < 0.85, "view").when(rand(13) < 0.66, "cart").otherwise("purchase").as("event_type"),
      element_at(array(lit("samsung"), lit("apple"), lit("xiaomi"), lit("sony"), lit("lenovo")),
        (rand(14) * 5 + 1).cast("int")).as("brand"),
      round(rand(15) * 1500 + 5, 2).as("amount"))
    logger.info(s"Generated $rows clickstream events")

    val w = Window.partitionBy("session_id").orderBy("ts")
    val abandoned = events
      .withColumn("next_event", lead("event_type", 1).over(w))
      .filter($"event_type" === "cart" && ($"next_event".isNull || $"next_event" =!= "purchase"))
      .groupBy("brand").agg(count("*").as("abandoned_carts"), avg("amount").as("avg_cart_value"))
    val abandonedBrands = abandoned.count()
    OtelSpark.event("funnel.abandoned_carts.computed", "brands" -> abandonedBrands)
    logger.info(s"Abandoned-cart brands: $abandonedBrands")

    val kpi = guard(events, scenario)
      .groupBy("brand").pivot("event_type", Seq("view", "cart", "purchase")).agg(count("amount")).na.fill(0)
      .withColumn("conversion_rate", $"purchase" / $"view")
    kpi.write.mode(SaveMode.Overwrite).parquet("/tmp/output/funnel_kpi")
    logger.info("Daily funnel KPIs written")
  }

  // ── 3. Portfolio risk roll-up: wide shuffle + sort, the usual skew victim ─────────────────────
  private def riskAggregation(spark: SparkSession, rows: Long, scenario: String): Unit = {
    import spark.implicits._
    val positions = guard(transactions(spark, rows, scenario), scenario)
      .withColumn("exposure", $"amount" * (rand(21) * 3 + 0.5))
    val byAccount = positions
      .repartition($"account_id")
      .groupBy("account_id")
      .agg(sum("exposure").as("total_exposure"), max("exposure").as("max_single"), count("*").as("positions"))
      .orderBy(desc("total_exposure"))
    val top = OtelSpark.step("risk-rollup") { byAccount.limit(20).collect() }
    OtelSpark.count("risk.accounts.ranked", top.length.toLong, "pipeline" -> "risk-aggregation")
    logger.info(s"Top risk account ${top.headOption.map(_.get(0)).getOrElse("n/a")} across ${byAccount.count()} accounts")
    if (scenario == "skew") logger.warn("Partition imbalance detected: one account holds most positions")
  }

  /** Synthetic transactions; under "skew" ~70% of rows land on account 42. */
  private def transactions(spark: SparkSession, rows: Long, scenario: String): DataFrame = {
    import spark.implicits._
    val account =
      if (scenario == "skew") when(rand(3) < 0.7, lit(42L)).otherwise((rand(4) * 5000).cast("long"))
      else (rand(4) * 5000).cast("long")
    spark.range(rows).select(
      $"id".as("txn_id"),
      account.as("account_id"),
      round(exp(rand(5) * 4.5 + 4), 2).as("amount"),
      (lit(1717200000L) + $"id" * 3 + (rand(6) * 60).cast("long")).as("ts"))
  }

  /**
   * In the "failure" scenario exactly one record (the 5,000th row of partition 1) is corrupt:
   * its task throws, Spark gives up (local mode has no retries), and the failure propagates
   * Task -> Stage -> Job -> SQL -> Application as ERROR spans plus an ERROR log.
   * The UDF reads a row id, so the optimizer cannot prune it away.
   */
  private def guard(df: DataFrame, scenario: String): DataFrame = {
    if (scenario != "failure") return df
    val corruptRow = (1L << 33) + 5000L // monotonically_increasing_id = partition << 33 | row
    val validate = udf { (amount: Double, rowId: Long) =>
      if (rowId == corruptRow) throw new IllegalStateException(s"Corrupt record at row $rowId: amount=$amount failed schema validation")
      amount
    }
    df.withColumn("amount", validate(col("amount"), monotonically_increasing_id()))
  }

  private def rootMessage(e: Throwable): String = {
    var t = e
    while (t.getCause != null && t.getCause != t) t = t.getCause
    Option(t.getMessage).getOrElse(t.getClass.getName)
  }
}
