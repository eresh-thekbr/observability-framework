package com.example.apps

import com.example.listeners.OtelSparkListener
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, length, upper}

object SparkApp {
//  OtelSparkListener.init_tracer()
  private val logger = LogManager.getLogger(SparkApp.getClass)

  def main(args: Array[String]): Unit = {
    println("Spark Application Started")

    val spark = SparkSession.builder()
      .appName("Simple Spark App")
      .master("local[*]")
      .config("spark.extraListeners", "com.example.listeners.OtelSparkListener")
      .config("spark.sql.queryExecutionListeners", "com.example.listeners.OtelQueryExecutionListener")
      .getOrCreate()

    logger.info("Application initialized, ready to process data")

    val data = Seq((1, "John"), (2, "Jane"), (3, "Joe"))
    val df = spark.createDataFrame(data).toDF("id", "name")
    df.show()

    val cnt = df.count()
    logger.info(s"DataFrame contains $cnt records")

    val df1 = df.withColumn("name_upper", upper(col("name")))
    df1.show()

    val df2 = df1.withColumn("name_length", length(col("name")))
    val df2_cnt = df2.count()
    logger.info(s"Transformed DataFrame contains $df2_cnt records")


    df.write.mode("overwrite").parquet("data/test_output")

    logger.info("Stopping Spark Application")
    spark.stop()
  }
}
