package com.example.apps

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, count, desc, expr, lit, rand, sum, when}

object ShuffleIssue {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Shuffle Issue")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "200")
      .config("spark.executor.memory", "4g")
      .config("spark.driver.memory", "4g")
      .config("spark.sql.autoBroadcastJoinThreshold", "-1")
      .config("spark.extraListeners", "com.example.listeners.OtelSparkListener")
      .config("spark.sql.queryExecutionListeners", "com.example.listeners.OtelQueryExecutionListener")
      .getOrCreate()

    import spark.implicits._

    val n = 50000000
    val df = spark.range(n)
      .withColumn("k", (rand()*5000).cast("int"))
      .withColumn("v", rand())
      .repartition(200, $"k")

    val t0 = System.currentTimeMillis()

    val out = df.groupBy($"k")
      .agg(
        count(lit(1)).as("cnt"),
        avg($"v").as("avg_v"),
        sum($"v").as("sum_v")
      )
      .orderBy(desc("cnt"))

    out.count()

    val df1 = spark.range(0, 50000000)
      .withColumn("skew_key",
        when($"id" < 49000000, lit(1)).otherwise($"id")
      ).withColumn("payload", expr("repeat('x', 100)"))

    val result = df1
      .repartition(200, $"skew_key")
      .groupBy("skew_key")
      .count()

    result.collect()

    println(s"Elapsed ms = ${System.currentTimeMillis() - t0}")
  }
}
