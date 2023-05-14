package com.github.aleksandrachasch.sparkdemo

import org.apache.spark.sql.SparkSession

trait SparkSessionWrapper {

  lazy val spark = SparkSession.builder()
    .appName("SparkProdApp")
    .master("yarn")
    .getOrCreate

}
