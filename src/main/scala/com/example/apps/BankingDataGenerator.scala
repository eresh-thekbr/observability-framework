package com.example.apps

import org.apache.spark.sql.{SaveMode, SparkSession}
import scala.util.Random

object BankingDataGenerator {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Banking Data Gen")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val numAccounts = 50000
    val numTransactions = 2000000 // 2 Million transactions
    val numBlacklist = 1000

    println("Generating Accounts...")
    val accountTypes = Seq("Savings", "Checking", "Credit", "Investment")
    val accounts = (1 to numAccounts).map { id =>
      (id, s"Cust_$id", accountTypes(Random.nextInt(accountTypes.length)), "US", "Active")
    }.toDF("account_id", "customer_name", "account_type", "country", "status")

    accounts.write.mode(SaveMode.Overwrite).option("header", "true").csv("data/accounts.csv")

    println("Generating Blacklist...")
    val blacklist = (1 to numBlacklist).map { _ =>
      (Random.nextInt(numAccounts) + 1, "Money Laundering", System.currentTimeMillis())
    }.toDF("account_id", "reason", "flagged_date")

    blacklist.write.mode(SaveMode.Overwrite).option("header", "true").csv("data/blacklist.csv")

    println("Generating Transactions...")
    // Generate batches to simulate high volume
    val transactions = (1 to numTransactions).map { id =>
      val txnType = if (Random.nextBoolean()) "DEBIT" else "CREDIT"
      val amount = (Random.nextDouble() * 5000).formatted("%.2f").toDouble
      // Simulate clusters of time for window functions
      val time = System.currentTimeMillis() - Random.nextInt(10000000)
      (id, Random.nextInt(numAccounts) + 1, txnType, amount, s"Ref_${Random.alphanumeric.take(8).mkString}", time)
    }.toDF("txn_id", "account_id", "txn_type", "amount", "reference", "timestamp")

    transactions.write.mode(SaveMode.Overwrite).option("header", "true").csv("data/transactions.csv")

    println("Banking Data Ready!")
    spark.stop()
  }
}
