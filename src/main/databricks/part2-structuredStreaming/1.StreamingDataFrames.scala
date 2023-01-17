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

// MAGIC %md
// MAGIC # Why use Structured Streaming?
// MAGIC * High-level
// MAGIC   * Ease of development
// MAGIC   * Interoperable with other Spark APIs
// MAGIC   * auto-optimizations

// COMMAND ----------

// MAGIC %md
// MAGIC # Structured Streaming Principles
// MAGIC * Lasy Evaluation
// MAGIC   * Transformations and Actions
// MAGIC     * Transformations describe how new DFs are obtained
// MAGIC     * Actions start executing/running Spark code
// MAGIC * Input Sources
// MAGIC   * Kafka, Flume
// MAGIC   * Distributed file system
// MAGIC   * Sockets
// MAGIC * Output Sinks
// MAGIC   * Distributed file system
// MAGIC   * Databases
// MAGIC   * Kafka
// MAGIC   * testing sinks (e.g. console, memory)

// COMMAND ----------

// MAGIC %md
// MAGIC # Streaming I/O
// MAGIC * Output Mode
// MAGIC   * append = only add new records
// MAGIC   * update = modify records in place
// MAGIC     * Only differs from append if the query has aggregations
// MAGIC   * complete = rewrite everything
// MAGIC * Not all sinks support all output modes
// MAGIC   * example: aggregations and append mode
// MAGIC * Triggers = when new data is written
// MAGIC   * default: write as soon as the current micro-batch has been processed
// MAGIC   * once: write a single micro-batch and stop
// MAGIC   * processing-time: look for new data at fixed intervals
// MAGIC   * continuous: processes each record individually (experimental at time of recording)

// COMMAND ----------

import org.apache.spark.sql.types._

// reads an example dataset from dbfs as a stream because I don't have a reliable way to set a socket up on here
val lines = spark.readStream
  .schema(
    StructType(List(
        StructField("time", TimestampType, true),
        StructField("action", StringType, true)
    ))
  ).option("maxFilesPerTrigger", 1)
  .option("rowsPerSecond", 1)
  .json("/databricks-datasets/structured-streaming/events/")

// COMMAND ----------

// performing an operation on the streaming df
val openLines = lines.where($"action" === "Open")
// This demonstrates the similarities between streaming and static/batch APIs

// COMMAND ----------

import org.apache.spark.sql.streaming.Trigger
import scala.concurrent.duration._

// starts outputting the lines to memory
// be sure to cancel this cell before leaving or pay dearly!!!
openLines.writeStream
  .format("memory")
  .trigger(
    // Trigger.Continuous(2.seconds) // experimental, every 2 seconds with or without new data
    // Trigger.Once() // runs once grabbing any added data since the last run, if any
    Trigger.ProcessingTime(10.seconds) // waits 10 seconds then runs a batch if there is new data
  ).queryName("openLines")
  .outputMode("append")
  .start()

// // This would write the lines to the console if it were running locally
// val query = lines.writeStream
//   .format("console")
//   .outputMode("append")
//   .start()

// // continues running until the query is stopped
// query.awaitTermination()  // not be required in databricks since the cell will continue running automatically

// COMMAND ----------

// MAGIC %sql
// MAGIC -- reads the streamed data, needs to be re-run to update
// MAGIC select * from openLines order by time desc

// COMMAND ----------

openLines.isStreaming // denotes that this is a streaming DF

// COMMAND ----------

// MAGIC %run ../common/package

// COMMAND ----------

// setting up to read a stream from files instead
val stocksDf = spark.readStream
  .format("csv")
  .option("header", "false") // source files do not have headers
  .option("dateFormat", "MMM d yyyy") // setting the format of the dates
  .option("maxFilesPerTrigger", 1)
  .option("rowsPerSecond", 1)
  .schema(stocksSchema) // from common notebook
  .load(s"$strgDir/stocks/")

// COMMAND ----------

// starts outputting the stream to memory
// be sure to cancel this cell before leaving or pay dearly!!!
stocksDf.writeStream
  .format("memory")
  .trigger(Trigger.ProcessingTime(10.seconds))
  .queryName("stocks")
  .outputMode("append")
  .start()
  // .awaitTermination() // can tag this onto the end instead of putting it on a separate line

// COMMAND ----------

// MAGIC %sql
// MAGIC -- reads the streamed data, needs to be re-run to update
// MAGIC select * from stocks order by date desc
