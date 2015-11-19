package com.ahars.parser

import org.apache.spark.sql.{SaveMode, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by ahars on 19/11/15.
  */
object ComparisonCsvParquet {

  val csvPath = "src/main/resources/data/csv/titanic.csv"
  val parquetPath = "src/main/resources/data/parquet"

  val csvFormat = "com.databricks.spark.csv"
  val defaultOptions = Map("header" -> "true",
    "inferschema" -> "true")

  val sparkConf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("parser-csv")
    .set("spark.sql.shuffle.partitions", "2")

  val sc = new SparkContext(sparkConf)
  val sqlc = new SQLContext(sc)

  def main(args: Array[String]) {

    titanicCsvToParquet()
    comparaisonCsvParquet()

    while(true) {}
  }

  def titanicCsvToParquet() = {

    val data = sqlc.read.format(csvFormat)
    .options(defaultOptions)
    .load(csvPath)

    data.write.format("parquet").mode(SaveMode.Overwrite).save(parquetPath)

    data.unpersist()
  }

  def comparaisonCsvParquet() = {

    val csv = sqlc.read.format(csvFormat).options(defaultOptions).load(csvPath).cache()
    val parquet = sqlc.read.parquet(parquetPath).cache()

    val t11 = System.currentTimeMillis()
    csv.select("Name").show()
    val t12 = System.currentTimeMillis() - t11

    val t21 = System.currentTimeMillis()
    parquet.select("Name").show()
    val t22 = System.currentTimeMillis() - t21

    val t31 = System.currentTimeMillis()
    csv.groupBy("Survived").count().show()
    val t32 = System.currentTimeMillis() - t31

    val t41 = System.currentTimeMillis()
    parquet.groupBy("Survived").count().show()
    val t42 = System.currentTimeMillis() - t41

    println("select name : " + t12 + " vs " + t22)
    println("count group by survived : " + t32 + " vs " + t42)
  }

}
