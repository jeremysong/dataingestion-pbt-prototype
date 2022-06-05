package org.jeremy.spark
package spark.job

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

object SparkJob {

  def execute(spark: SparkSession, execParams: Map[String, String]): DataFrame = {
    val schema = StructType(Array(
      StructField("customerId", LongType, true),
      StructField("hva", StringType, true)
    ))
    val df = spark.read.schema(schema).csv(execParams("inputPath"))
    val result = df.groupBy("customerId", "hva")
      .count().as("count")
      .where("customerId > 1")
    result
  }
}
