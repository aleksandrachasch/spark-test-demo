package com.github.aleksandrachasch.sparkdemo

import org.apache.spark.sql.SparkSession

trait SparkSessionTestWrapper {

  val spark = SparkSession.builder()
    .appName("SparkTestApp")
    .master("local")
    .getOrCreate

}
