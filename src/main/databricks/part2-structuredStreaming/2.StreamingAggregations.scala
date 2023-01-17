// Databricks notebook source
// dbutils.widgets.text("storagename", "storageAcct")
// dbutils.widgets.text("storagecntner", "blobContainer")
// dbutils.widgets.text("storage_acc_key", "storageKey")

// COMMAND ----------

val strgAcct = dbutils.widgets.get("storagename")
val strgCntnr = dbutils.widgets.get("storagecntner")
val strgKey = dbutils.widgets.get("storage_acc_key")

spark.conf.set(s"fs.azure.account.key.$strgAcct.blob.core.windows.net", strgKey)
val strgDir = s"wasbs://$strgCntnr@$strgAcct.blob.core.windows.net/"

// COMMAND ----------

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import org.apache.spark.sql.streaming.Trigger
import scala.concurrent.duration._

// COMMAND ----------

val lines = spark.readStream
  .schema(
    StructType(List(
        StructField("time", TimestampType, true),
        StructField("action", StringType, true)
    ))
  ).option("maxFilesPerTrigger", 1)
  .json("/databricks-datasets/structured-streaming/events/")

// COMMAND ----------

display(lines)

// COMMAND ----------

// be sure to cancel this cell before leaving or pay dearly!!!

val lineCount = lines.selectExpr("count(*) as lineCnt")

// aggregations with distinct are not supported
// otherwise spark will need to keep track of everything

lineCount.writeStream
  .format("memory")
  .queryName("lineCount")
  .outputMode("complete") // append and update not supported on aggregations without watermark (will cover later)
  .start()

// display(lineCount)

// COMMAND ----------

// MAGIC %sql
// MAGIC -- Re-run this cell to update the count or switch the commented blocks in the above cell
// MAGIC select *
// MAGIC from lineCount

// COMMAND ----------

// MAGIC %run ../common/package

// COMMAND ----------

// doing numerical aggregations instead of metadata aggregations
// writing this one as a function to make it more flexible
def numericalAgg(aggFunc: Column => Column): Unit = {
  val stocksDf = spark.readStream
    .format("csv")
    .option("header", "false") // source files do not have headers
    .option("dateFormat", "MMM d yyyy") // setting the format of the dates
    .option("maxFilesPerTrigger", 1)
    .option("rowsPerSecond", 1)
    .schema(stocksSchema) // from common notebook
    .load(s"$strgDir/stocks/")

  val avgDf = stocksDf.select(aggFunc($"value"))

  avgDf.writeStream
    .format("memory")
    .trigger(Trigger.ProcessingTime(10.seconds))
    .queryName("aggStocks")
    .outputMode("complete")
    .start()
}
// display(sumDf)

// COMMAND ----------

// be sure to cancel this cell before leaving or pay dearly!!!
numericalAgg(avg)

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from aggStocks

// COMMAND ----------

val stocksDf = spark.readStream
  .format("csv")
  .option("header", "false") // source files do not have headers
  .option("dateFormat", "MMM d yyyy") // setting the format of the dates
  .option("maxFilesPerTrigger", 1)
  .option("rowsPerSecond", 1)
  .schema(stocksSchema) // from common notebook
  .load(s"$strgDir/stocks/")

val names = stocksDf.select($"company".alias("name"))
  .groupBy($"name")
  .count()

names.writeStream
  .format("memory")
  .queryName("names")
  .outputMode("complete")
  .start()

// COMMAND ----------

// MAGIC %sql select * from names

// COMMAND ----------

// creating a flexible grouped aggregate function
def groupedAgg(aggFunc: Column => Column): Unit = {
  val stocksDf = spark.readStream
    .format("csv")
    .option("header", "false") // source files do not have headers
    .option("dateFormat", "MMM d yyyy") // setting the format of the dates
    .option("maxFilesPerTrigger", 1)
    .option("rowsPerSecond", 1)
    .schema(stocksSchema) // from common notebook
    .load(s"$strgDir/stocks/")

  stocksDf
    .groupBy($"company").agg(aggFunc($"value"))
    .writeStream.trigger(Trigger.ProcessingTime(3.seconds))
    .format("memory").queryName("groupedAgg")
    .outputMode("complete")
    .start()
}

// COMMAND ----------

groupedAgg(avg)

// COMMAND ----------

// MAGIC %sql select * from groupedAgg
