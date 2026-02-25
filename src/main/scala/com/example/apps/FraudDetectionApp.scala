package com.example.apps

import com.example.listeners.{OtelQueryExecutionListener, OtelSparkListener}
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.storage.StorageLevel

object FraudDetectionApp {

  val serviceName = Option(System.getProperty("otel.service.name"))
    .getOrElse("fraud-detection-app")
  OtelSparkListener.init_tracer(serviceName)
  private val logger = LogManager.getLogger("FraudDetectionApp")

  def main(args: Array[String]): Unit = {
      val spark = SparkSession.builder()
        .appName("Fraud Detection Pipeline")
        .master("local[*]")
        .config("spark.extraListeners", "com.example.listeners.OtelSparkListener")
//        .config("spark.sql.queryExecutionListeners", "com.example.listeners.OtelQueryExecutionListener")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.autoBroadcastJoinThreshold", "10MB")
        .getOrCreate()

      import spark.implicits._

      // 1. Ingest Data
      val accounts = spark.read.option("header", "true").csv("data/accounts.csv")
      val blacklist = spark.read.option("header", "true").csv("data/blacklist.csv")
      val transactions = spark.read.option("header", "true").option("inferSchema", "true").csv("data/transactions.csv")

      logger.info(s"accounts sample data: ${accounts.show(5, truncate = false)}")
      logger.info(s"blacklist sample data: ${blacklist.show(5, truncate = false)}")
      logger.info(s"transactions sample data: ${transactions.show(5, truncate = false)}")


      // 2. Data Enrichment (Standard Join)
      logger.info("Enriching transactions with Account details...")
      val enriched = transactions
        .join(accounts, "account_id")
        .select($"txn_id", $"account_id", $"customer_name", $"amount", $"timestamp", $"txn_type")
        .withColumn("country", lit("US"))

      // 3. Fraud Check: Broadcast Join against Blacklist
      // Finding transactions from flagged accounts
      logger.info("Checking against Sanctions List...")
      val flaggedTxns = enriched
        .join(broadcast(blacklist), "account_id")
        .select($"txn_id", $"customer_name", $"amount", $"reason".as("flag_reason"))

      flaggedTxns.write.mode("overwrite").csv("data/output/flagged_transactions")

      // 4. Complex Pattern: "Rapid Fire" Transactions
      // Finding accounts with > 3 transactions in short window
      logger.info("Detecting rapid-fire anomalies...")
      val windowSpec = Window.partitionBy("account_id").orderBy("timestamp")

      val patternDf = transactions
        .withColumn("prev_txn_time", lag("timestamp", 1).over(windowSpec))
        .withColumn("time_diff", ($"timestamp" - $"prev_txn_time") / 1000) // seconds
        .filter($"time_diff" < 60) // Transactions within 60 seconds of each other

      // Force evaluation
      val anomalyCount = patternDf.count()
      logger.info(s"Found $anomalyCount suspicious rapid transactions")

      // 5. Aggregation: High Value Transfers by Country
      logger.info("Calculating regional exposure...")
      enriched
        .filter($"amount" > 4000)
        .groupBy("country")
        .agg(
          count("txn_id").as("txn_count"),
          sum("amount").as("total_volume")
        )
        .orderBy(desc("total_volume"))
        .show()


    val rawSchema = StructType(Array(
      StructField("event_time", TimestampType, true),
      StructField("event_type", StringType, true),
      StructField("product_id", IntegerType, true),
      StructField("category_id", LongType, true),
      StructField("category_code", StringType, true),
      StructField("brand", StringType, true),
      StructField("price", DoubleType, true),
      StructField("user_id", IntegerType, true),
      StructField("user_session", StringType, true)
    ))

    val ecom_df = spark.read
      .option("header", "true")
      .option("timestampFormat", "yyyy-MM-dd HH:mm:ss z") // Handle "UTC" suffix if present
      .schema(rawSchema)
      .csv("data/2019-Nov.csv")

    ecom_df.printSchema()
    ecom_df.show(5, false)
    val ecount = ecom_df.count()
    logger.info(s"E-commerce raw data count: $ecount")


    val dfSilver = ecom_df
      .filter($"price" > 0)
      .withColumn("event_date", to_date($"event_time"))
      .withColumn("cat_split", split($"category_code", "\\."))
      .withColumn("main_category", $"cat_split".getItem(0))
      .withColumn("sub_category", $"cat_split".getItem(1))
      .withColumn("brand", coalesce($"brand", lit("unknown")))
      .drop("cat_split", "category_code")

    dfSilver.show(5, false)

    dfSilver.write
      .mode(SaveMode.Overwrite)
      .partitionBy("event_date")
      .parquet("data/output/ecom_silver/")

    val dfClean = spark.read.parquet("data/output/ecom_silver/")
    val cleanCount = dfClean.count()
    logger.info(s"E-commerce cleaned data count: $cleanCount")

    val sessionWindow = Window.partitionBy("user_session").orderBy("event_time")

    val dfFunnelRaw = dfClean
      // Lag(1): What did the user do immediately before this action?
      .withColumn("prev_event", lag($"event_type", 1).over(sessionWindow))
      // Lead(1): What did the user do immediately after?
      .withColumn("next_event", lead($"event_type", 1).over(sessionWindow))
      // Time Gap: Time spent between actions
      .withColumn("time_diff_sec",
        (unix_timestamp($"event_time") - unix_timestamp(lag($"event_time", 1).over(sessionWindow))))

    val abandonedCarts = dfFunnelRaw
      .filter($"event_type" === "cart" &&
        ($"next_event".isNull || $"next_event" =!= "purchase"))
      .groupBy("event_date", "brand")
      .agg(
        count("user_session").alias("abandoned_count"),
        avg("price").alias("avg_abandoned_amount")
      )

    val dfDailyKPI = dfClean
      .groupBy("event_date", "brand")
      .pivot("event_type", Seq("view", "cart", "purchase")) // Pivoting distinct values
      .agg(count("product_id"))
      .na.fill(0)

    val dfFinalReport = dfDailyKPI
      .withColumn("conversion_rate", when($"view" > 0, $"purchase" / $"view").otherwise(0.0))
      .orderBy($"conversion_rate".desc)

    dfFinalReport.write
      .mode(SaveMode.Overwrite)
      .parquet("data/output/ecom_gold/")


    println("--- Top Converting Brands ---")
    dfFinalReport.filter($"view" > 1000).show(10, false)

    Thread.sleep(10000)
    spark.stop()


  }
}
