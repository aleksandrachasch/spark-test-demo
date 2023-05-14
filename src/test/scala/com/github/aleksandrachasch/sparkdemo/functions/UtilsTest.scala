package com.github.aleksandrachasch.sparkdemo.functions

import com.github.aleksandrachasch.sparkdemo.SparkSessionTestWrapper
import com.github.aleksandrachasch.sparkdemo.functions.Utils._
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.DataFrame
import org.scalatest.{BeforeAndAfter, FlatSpec}

class UtilsTest extends FlatSpec with SparkSessionTestWrapper
  with Logging with BeforeAndAfter with DataFrameComparer {

  var testDf: DataFrame = _
  var studentDf: DataFrame = _

  before {

    testDf = spark.read.format("json")
      .option("mode", "FAILFAST")
      .option("multiline", true)
      .load("src/test/resources/customer-data.json")

    studentDf = spark.read.format("json")
      .option("mode", "FAILFAST")
      .load("src/test/resources/students.json")



  }

  it should "print studentDf schema and show" in {
    studentDf.printSchema
    studentDf.show
  }

  it should "print testDf schema and show" in {
    testDf.printSchema
    testDf.show
  }

  it should "return correct interaction list" in {

    import spark.implicits._

    val expectedDf = Seq("WEBSITE", "TWITTER", "INSTAGRAM").toDF("type")

    val resultDf = getAllInteractionTypes(testDf)

    //val resultDf = testDf.getAllInteractionTypes getAllInteractionTypes(testDf)

    assertSmallDatasetEquality(expectedDf, resultDf, orderedComparison = false)

  }

}
