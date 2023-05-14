package com.github.aleksandrachasch.sparkdemo.functions

import com.github.aleksandrachasch.sparkdemo.SparkSessionWrapper
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, explode}

object Utils extends SparkSessionWrapper with Logging {


  def getAllInteractionTypes(df: DataFrame): DataFrame = {
    df.select(col("Accounts.InteractionGroups"))
      .select(explode(col("InteractionGroups")) as "groups")
      .select(explode(col("groups")))
      .select(explode(col("col.interactions")))
      .select("col.type")
      .distinct
  }

/**
  implicit class ImplicitFunctions(df: DataFrame) {
    def getAllInteractionTypes: DataFrame = {
      df.select(col("Accounts.InteractionGroups"))
        .select(explode(col("InteractionGroups")) as "groups")
        .select(explode(col("groups")))
        .select(explode(col("col.interactions")))
        .select("col.type")
        .distinct
    }

  }
  **/

}
