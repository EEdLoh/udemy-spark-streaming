// Databricks notebook source
// MAGIC %md
// MAGIC # Using this notebook
// MAGIC * Running all will not yield interesting results due to delete statements in some cells
// MAGIC   * These statements will cancel the stream outputs
// MAGIC * It is recommended to run the code down to the first streaming output then run each cell one at a time to demonstrate the stream functionality as data comes through then delete the resulting data before moving on to the next section

// COMMAND ----------

// dbutils.widgets.text("storagename", "storageAcct")
// dbutils.widgets.text("storagecntner", "blobContainer")
// dbutils.widgets.text("storage_acc_key", "storageKey")
// dbutils.widgets.text("adls_acct", "adlsAcct")
// dbutils.widgets.text("adls_cntnr", "adlsCntnr")

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

val adlsAcct = dbutils.widgets.get("adls_acct")
val adlsCntnr = dbutils.widgets.get("adls_cntnr")

val strgAcct = dbutils.widgets.get("storagename")
val strgCntnr = dbutils.widgets.get("storagecntner")
val strgKey = dbutils.widgets.get("storage_acc_key")

spark.conf.set(s"fs.azure.account.key.$strgAcct.blob.core.windows.net", strgKey)
val strgDir = s"wasbs://$strgCntnr@$strgAcct.blob.core.windows.net/"

// COMMAND ----------

// MAGIC %sql -- Using a temp table and insert statements instead of a socket
// MAGIC create or replace table bands (
// MAGIC   value string
// MAGIC ) using delta
// MAGIC location 'abfss://${adls_cntnr}@${adls_acct}.dfs.core.windows.net/test-dev/bands'
// MAGIC -- the ${} structure injects a variable, as it does in scala

// COMMAND ----------

// MAGIC %sql -- Using a temp table and insert statements instead of a socket
// MAGIC create or replace table guitarists (
// MAGIC   value string
// MAGIC ) using delta
// MAGIC location 'abfss://${adls_cntnr}@${adls_acct}.dfs.core.windows.net/test-dev/guitarists'
// MAGIC -- the ${} structure injects a variable, as it does in scala

// COMMAND ----------

// MAGIC %md
// MAGIC # Static to Static

// COMMAND ----------

// How it's done with static data
val guitarPlayers = spark.read
  .option("inferSchema", true)
  .json(s"$strgDir/guitarPlayers")

val guitars = spark.read
  .option("inferSchema", true)
  .json(s"$strgDir/guitars")

val bands = spark.read
  .option("inferSchema", true)
  .json(s"$strgDir/bands")

def joinCondition(left: DataFrame, right: DataFrame) = {
  left.col("band") === right.col("id")
}
val guitaristsBands = guitarPlayers.join(
  bands,
  joinCondition(guitarPlayers, bands),
  "inner" // specifying inner even though it's default
)
display(guitaristsBands)

// COMMAND ----------

// MAGIC %md
// MAGIC # Stream to Static

// COMMAND ----------

// Joining a stream with static
val bandsSchema = bands.schema
val streamedBandsDf = spark.readStream
  .format("delta")
  .table("bands")
  .select(from_json($"value", bandsSchema).as("band"))
  .selectExpr(
    // "band.*" // shorthand to select each column from a struct, can replace the below 4 rows
    "band.id as id",
    "band.name as name",
    "band.hometown as hometown",
    "band.year as year"
  )

val streamStaticBandsGuitaristsDf = streamedBandsDf.join(
  guitarPlayers,
  joinCondition(guitarPlayers, streamedBandsDf),
  "inner"
)

// COMMAND ----------

display(streamStaticBandsGuitaristsDf)

// COMMAND ----------

// MAGIC %sql
// MAGIC insert into bands
// MAGIC values '{"id":1, "name":"AC/DC", "hometown":"Sydney", "year":1973}'

// COMMAND ----------

// MAGIC %sql
// MAGIC -- Separating these to show batch processing
// MAGIC -- Wait for the previous cell to show up in the display then run this one
// MAGIC insert into bands
// MAGIC values '{"id":0, "name":"Led Zeppelin", "hometown":"London", "year":1968}'

// COMMAND ----------

// MAGIC %sql
// MAGIC -- Separating these to show batch processing
// MAGIC -- Wait for the previous cell to show up in the display then run this one
// MAGIC insert into bands
// MAGIC values '{"id":3, "name":"Metallica", "hometown":"Los Angeles", "year":1981}',
// MAGIC   '{"id":4, "name":"The Beatles", "hometown":"Liverpool", "year":1960}'

// COMMAND ----------

// MAGIC %sql -- run this to clear the test table and start again.
// MAGIC delete from bands

// COMMAND ----------

// MAGIC %md
// MAGIC ## Restricted Joins
// MAGIC * Stream to Static: Right/full outer, right semi and anti
// MAGIC * Static to Stream: Left/full outer, left semi and anti
// MAGIC   * Notice these two are both restricted such that the streamed data will not require held state

// COMMAND ----------

// MAGIC %md
// MAGIC # Stream to Stream

// COMMAND ----------

val guitaristsSchema = guitarPlayers.schema
val streamedGuitaristsDf = spark.readStream
  .format("delta")
  .table("guitarists")
  .select(from_json($"value", guitaristsSchema).as("guitarist"))
  .selectExpr(
    "guitarist.*" // shorthand to select each column from a struct, can replace the below
    // "guitarist.id as id",
    // "guitarist.name as name",
    // "guitarist.guitars as hometown",
    // "guitarist.band as year"
  )

val streamStreamBandsGuitaristsDf = streamedBandsDf.join(
  streamedGuitaristsDf,
  joinCondition(streamedGuitaristsDf, streamedBandsDf),
  "inner"
)

// COMMAND ----------

display(streamStreamBandsGuitaristsDf)

// COMMAND ----------

// MAGIC %sql
// MAGIC insert into bands
// MAGIC values '{"id":1, "name":"AC/DC", "hometown":"Sydney", "year":1973}'

// COMMAND ----------

// MAGIC %sql
// MAGIC insert into guitarists
// MAGIC values '{"id":1,"name":"Angus Young","guitars":[1],"band":1}'

// COMMAND ----------

// MAGIC %sql
// MAGIC insert into bands
// MAGIC values '{"id":0, "name":"Led Zeppelin", "hometown":"London", "year":1968}'

// COMMAND ----------

// MAGIC %sql
// MAGIC insert into guitarists
// MAGIC values '{"id":0,"name":"Jimmy Page","guitars":[0],"band":0}'

// COMMAND ----------

// MAGIC %sql
// MAGIC insert into bands
// MAGIC values '{"id":3, "name":"Metallica", "hometown":"Los Angeles", "year":1981}',
// MAGIC   '{"id":4, "name":"The Beatles", "hometown":"Liverpool", "year":1960}'

// COMMAND ----------

// MAGIC %sql
// MAGIC insert into guitarists
// MAGIC values '{"id":2,"name":"Eric Clapton","guitars":[1,5],"band":2}',
// MAGIC   '{"id":3,"name":"Kirk Hammett","guitars":[3],"band":3}'

// COMMAND ----------

// MAGIC %sql
// MAGIC delete from bands;
// MAGIC delete from guitarists;

// COMMAND ----------

// MAGIC %md
// MAGIC ## Specs
// MAGIC * Joins
// MAGIC   * Inner joins are supported
// MAGIC   * Right/left outer are supported but MUST have watermarks
// MAGIC   * Full outer joins are not supported
// MAGIC * Only Append output mode is supported
