// Databricks notebook source
import org.apache.spark.sql.types._

val carsSchema = StructType(Array(
  StructField("Name", StringType),
  StructField("Miles_per_Gallon", DoubleType),
  StructField("Cylinders", LongType),
  StructField("Displacement", DoubleType),
  StructField("Horsepower", LongType),
  StructField("Wdight_in_lbs", LongType),
  StructField("Acceleration", DoubleType),
  StructField("Year", StringType),
  StructField("Origin", StringType)
))

val stocksSchema = StructType(Array(
  StructField("company", StringType),
  StructField("date", DateType),
  StructField("value", DoubleType)
))
