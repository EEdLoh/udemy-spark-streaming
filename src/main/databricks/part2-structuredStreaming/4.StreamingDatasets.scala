// Databricks notebook source
// dbutils.widgets.text("storagename", "storageAcct")
// dbutils.widgets.text("storagecntner", "blobContainer")
// dbutils.widgets.text("storage_acc_key", "storageKey")
// dbutils.widgets.text("adls_acct", "adlsAcct")
// dbutils.widgets.text("adls_cntnr", "adlsCntnr")

// COMMAND ----------

val adlsAcct = dbutils.widgets.get("adls_acct")
val adlsCntnr = dbutils.widgets.get("adls_cntnr")
val adlsTemp = "abfss://%s@%s.dfs.core.windows.net/test-dev/%s"
val adlsCars = adlsTemp.format(adlsCntnr, adlsAcct, "cars")
spark.conf.set("adls.cars", adlsCars)

val strgAcct = dbutils.widgets.get("storagename")
val strgCntnr = dbutils.widgets.get("storagecntner")
val strgKey = dbutils.widgets.get("storage_acc_key")

spark.conf.set(s"fs.azure.account.key.$strgAcct.blob.core.windows.net", strgKey)
val strgDir = s"wasbs://$strgCntnr@$strgAcct.blob.core.windows.net/"

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset}

// COMMAND ----------

// MAGIC %sql -- Using a temp table and insert statements instead of a socket
// MAGIC create or replace table cars (
// MAGIC   value string
// MAGIC ) using delta
// MAGIC location '${adls.cars}'
// MAGIC -- the ${} structure injects a variable, as it does in scala

// COMMAND ----------

// MAGIC %run ../common/package

// COMMAND ----------

// MAGIC %run ../common/Car

// COMMAND ----------

// val carEncoder = Encoders.product[Car] // not necessary because of the auto-imported implicits

val carsDS: Dataset[Car] = spark.readStream
  .format("delta")
  .table("cars")
  .select(from_json($"value", carsSchema).as("car"))
  .selectExpr("car.*")  // DF with multiple columns
  .as[Car] // normally requires an encoder but spark.implicits is already imported in Databricks

// COMMAND ----------

// SQL transformations lose type info
val carNamesDF: DataFrame = carsDS.select($"Name") // DF output

// collection transformations maintain type info
val carNamesAlt: Dataset[String] = carsDS.map(_.Name)

// COMMAND ----------

// showing that streaming DSs can be output just like DFs
display(carsDS)

// COMMAND ----------

// Note the column is called `value` because the map method converts the column into unnamed Strings
display(carNamesAlt)

// COMMAND ----------

// MAGIC %sql
// MAGIC insert into cars
// MAGIC values '{"Name":"chevrolet chevelle malibu", "Miles_per_Gallon":18, "Cylinders":8, "Displacement":307, "Horsepower":130, "Weight_in_lbs":3504, "Acceleration":12, "Year":"1970-01-01", "Origin":"USA"}'

// COMMAND ----------

// MAGIC %sql
// MAGIC insert into cars
// MAGIC values 
// MAGIC '{"Name":"buick skylark 320", "Miles_per_Gallon":15, "Cylinders":8, "Displacement":350, "Horsepower":165, "Weight_in_lbs":3693, "Acceleration":11.5, "Year":"1970-01-01", "Origin":"USA"}',
// MAGIC '{"Name":"plymouth satellite", "Miles_per_Gallon":18, "Cylinders":8, "Displacement":318, "Horsepower":150, "Weight_in_lbs":3436, "Acceleration":11, "Year":"1970-01-01", "Origin":"USA"}',
// MAGIC '{"Name":"amc rebel sst", "Miles_per_Gallon":16, "Cylinders":8, "Displacement":304, "Horsepower":150, "Weight_in_lbs":3433, "Acceleration":12, "Year":"1970-01-01", "Origin":"USA"}',
// MAGIC '{"Name":"ford torino", "Miles_per_Gallon":17, "Cylinders":8, "Displacement":302, "Horsepower":140, "Weight_in_lbs":3449, "Acceleration":10.5, "Year":"1970-01-01", "Origin":"USA"}',
// MAGIC '{"Name":"ford galaxie 500", "Miles_per_Gallon":15, "Cylinders":8, "Displacement":429, "Horsepower":198, "Weight_in_lbs":4341, "Acceleration":10, "Year":"1970-01-01", "Origin":"USA"}'

// COMMAND ----------

// MAGIC %sql
// MAGIC delete from cars

// COMMAND ----------

// EX1: get count of cars with >140 HP

// This uses the DF methods. We should use DS
// display(
//   carsDS.selectExpr("count(*) filter(where Horsepower > 140) as powerCount")
// )

// Apparently this is the answer, even though there's no counting involved?
// display(carsDS.filter(_.Horsepower.getOrElse(0L) > 140))

// Here's a combo that actually counts it
display(carsDS.filter(_.Horsepower.getOrElse(0L) > 140).selectExpr("count(*) as powerCarCount"))

// COMMAND ----------

// EX2: get AVG HP

// Using DF methods
display(carsDS.selectExpr("avg(Horsepower) as avgHp"))

// COMMAND ----------

// EX3: Count cars grouped by Origin

// // using DF methods
// display(
//   carsDS.groupBy($"Origin").count()
// )

// using DS method .groupByKey
display(carsDS.groupByKey(car => car.Origin).count())


// COMMAND ----------

// MAGIC %sql
// MAGIC insert into cars
// MAGIC values '{"Name":"chevrolet impala", "Miles_per_Gallon":14, "Cylinders":8, "Displacement":454, "Horsepower":220, "Weight_in_lbs":4354, "Acceleration":9, "Year":"1970-01-01", "Origin":"USA"}',
// MAGIC   '{"Name":"plymouth fury iii", "Miles_per_Gallon":14, "Cylinders":8, "Displacement":440, "Horsepower":215, "Weight_in_lbs":4312, "Acceleration":8.5, "Year":"1970-01-01", "Origin":"USA"}',
// MAGIC   '{"Name":"pontiac catalina", "Miles_per_Gallon":14, "Cylinders":8, "Displacement":455, "Horsepower":225, "Weight_in_lbs":4425, "Acceleration":10, "Year":"1970-01-01", "Origin":"USA"}',
// MAGIC   '{"Name":"amc ambassador dpl", "Miles_per_Gallon":15, "Cylinders":8, "Displacement":390, "Horsepower":190, "Weight_in_lbs":3850, "Acceleration":8.5, "Year":"1970-01-01", "Origin":"USA"}',
// MAGIC   '{"Name":"citroen ds-21 pallas", "Miles_per_Gallon":null, "Cylinders":4, "Displacement":133, "Horsepower":115, "Weight_in_lbs":3090, "Acceleration":17.5, "Year":"1970-01-01", "Origin":"Europe"}'

// COMMAND ----------

// MAGIC %sql
// MAGIC insert into cars
// MAGIC values '{"Name":"chevrolet chevelle concours (sw)", "Miles_per_Gallon":null, "Cylinders":8, "Displacement":350, "Horsepower":165, "Weight_in_lbs":4142, "Acceleration":11.5, "Year":"1970-01-01", "Origin":"USA"}',
// MAGIC   '{"Name":"ford torino (sw)", "Miles_per_Gallon":null, "Cylinders":8, "Displacement":351, "Horsepower":153, "Weight_in_lbs":4034, "Acceleration":11, "Year":"1970-01-01", "Origin":"USA"}'

// COMMAND ----------

// MAGIC %sql
// MAGIC insert into cars
// MAGIC values '{"Name":"plymouth satellite (sw)", "Miles_per_Gallon":null, "Cylinders":8, "Displacement":383, "Horsepower":175, "Weight_in_lbs":4166, "Acceleration":10.5, "Year":"1970-01-01", "Origin":"USA"}',
// MAGIC '{"Name":"amc rebel sst (sw)", "Miles_per_Gallon":null, "Cylinders":8, "Displacement":360, "Horsepower":175, "Weight_in_lbs":3850, "Acceleration":11, "Year":"1970-01-01", "Origin":"USA"}',
// MAGIC '{"Name":"dodge challenger se", "Miles_per_Gallon":15, "Cylinders":8, "Displacement":383, "Horsepower":170, "Weight_in_lbs":3563, "Acceleration":10, "Year":"1970-01-01", "Origin":"USA"}',
// MAGIC '{"Name":"plymouth cuda 340", "Miles_per_Gallon":14, "Cylinders":8, "Displacement":340, "Horsepower":160, "Weight_in_lbs":3609, "Acceleration":8, "Year":"1970-01-01", "Origin":"USA"}',
// MAGIC '{"Name":"ford mustang boss 302", "Miles_per_Gallon":null, "Cylinders":8, "Displacement":302, "Horsepower":140, "Weight_in_lbs":3353, "Acceleration":8, "Year":"1970-01-01", "Origin":"USA"}',
// MAGIC '{"Name":"chevrolet monte carlo", "Miles_per_Gallon":15, "Cylinders":8, "Displacement":400, "Horsepower":150, "Weight_in_lbs":3761, "Acceleration":9.5, "Year":"1970-01-01", "Origin":"USA"}',
// MAGIC '{"Name":"buick estate wagon (sw)", "Miles_per_Gallon":14, "Cylinders":8, "Displacement":455, "Horsepower":225, "Weight_in_lbs":3086, "Acceleration":10, "Year":"1970-01-01", "Origin":"USA"}',
// MAGIC '{"Name":"toyota corona mark ii", "Miles_per_Gallon":24, "Cylinders":4, "Displacement":113, "Horsepower":95, "Weight_in_lbs":2372, "Acceleration":15, "Year":"1970-01-01", "Origin":"Japan"}',
// MAGIC '{"Name":"plymouth duster", "Miles_per_Gallon":22, "Cylinders":6, "Displacement":198, "Horsepower":95, "Weight_in_lbs":2833, "Acceleration":15.5, "Year":"1970-01-01", "Origin":"USA"}'

// COMMAND ----------

// MAGIC %sql
// MAGIC insert into cars
// MAGIC values '{"Name":"amc hornet", "Miles_per_Gallon":18, "Cylinders":6, "Displacement":199, "Horsepower":97, "Weight_in_lbs":2774, "Acceleration":15.5, "Year":"1970-01-01", "Origin":"USA"}',
// MAGIC '{"Name":"ford maverick", "Miles_per_Gallon":21, "Cylinders":6, "Displacement":200, "Horsepower":85, "Weight_in_lbs":2587, "Acceleration":16, "Year":"1970-01-01", "Origin":"USA"}',
// MAGIC '{"Name":"datsun pl510", "Miles_per_Gallon":27, "Cylinders":4, "Displacement":97, "Horsepower":88, "Weight_in_lbs":2130, "Acceleration":14.5, "Year":"1970-01-01", "Origin":"Japan"}',
// MAGIC '{"Name":"volkswagen 1131 deluxe sedan", "Miles_per_Gallon":26, "Cylinders":4, "Displacement":97, "Horsepower":46, "Weight_in_lbs":1835, "Acceleration":20.5, "Year":"1970-01-01", "Origin":"Europe"}',
// MAGIC '{"Name":"peugeot 504", "Miles_per_Gallon":25, "Cylinders":4, "Displacement":110, "Horsepower":87, "Weight_in_lbs":2672, "Acceleration":17.5, "Year":"1970-01-01", "Origin":"Europe"}'

// COMMAND ----------

// MAGIC %md
// MAGIC # Cleanup

// COMMAND ----------

// MAGIC %sql
// MAGIC set spark.databricks.delta.retentionDurationCheck.enabled = false;
// MAGIC 
// MAGIC delete from cars;
// MAGIC vacuum cars RETAIN 0 HOURS;
// MAGIC drop table cars;

// COMMAND ----------

dbutils.fs.rm(adlsCars, true)
