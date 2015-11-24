package parsing

import com.ahars.parsing.SparkParser
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{functions, Row, SQLContext}
import org.scalatest.{BeforeAndAfter, Matchers, FlatSpec}

/**
  * Created by ahars on 24/11/2015.
  */
class TestParser extends FlatSpec with Matchers with BeforeAndAfter {

  private var sc: SparkContext = _
  private var sqlContext: SQLContext = _

  before {
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("test-parsing-date")

    sc = new SparkContext(sparkConf)
    sqlContext = new SQLContext(sc)
  }

  after {
    sc.stop()
  }

  "String parsed to date" should "return date value or NULL" in {
    val parsing = new SparkParser()
    val rdd = sc.parallelize(
      List("2015-02-03",
        "30301201",
        "XXXXXX",
        "2015-02-31",
        "2015-06-31",
        "2015-11-31",
        "2015-51-31",
        "100100-12-31",
        "2015-5111-31",
        "2015-11-4551"
      ))
    val rows = rdd.map(fields => Row(fields))
    val schema = StructType(Array(StructField("dat", DataTypes.StringType)))
    val df = sqlContext.createDataFrame(rows, schema)
    val dd = df.withColumn("try", functions.udf(parsing.parseDate _).apply(df("dat")))

    dd.select("try").collect() should be (Array(
      Row(parsing.format1.parse("2015-02-03")),
      Row(null),
      Row(null),
      Row(parsing.format1.parse("2015-03-03")),
      Row(parsing.format1.parse("2015-07-01")),
      Row(parsing.format1.parse("2015-12-01")),
      Row(parsing.format1.parse("2019-03-31")),
      Row(null),
      Row(null),
      Row(null)
    ))
  }

  "String parsed to boolean" should "return boolean value or NULL" in {
    val parsing = new SparkParser()
    val rdd = sc.parallelize(
      List("true",
        "false",
        "XXXXXX"
      ))
    val rows = rdd.map(fields => Row(fields))
    val schema = StructType(Array(StructField("str", DataTypes.StringType)))
    val df = sqlContext.createDataFrame(rows, schema)
    val dd = df.withColumn("bool", functions.udf(parsing.parseBoolean _).apply(df("str")))

    dd.select("bool").collect() should be (Array(
      Row(true),
      Row(false),
      Row(null)
    ))
  }
}
