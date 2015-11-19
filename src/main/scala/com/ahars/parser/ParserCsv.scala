package com.ahars.parser

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by ahars on 19/11/15.
  */
object ParserCsv {

  val sparkConf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("parser-csv")
    .set("spark.sql.shuffle.partitions", "2")

  val sc = new SparkContext(sparkConf)
  val sqlc = new SQLContext(sc)

  def main(args: Array[String]) {
    println("hello")
  }

}
