package org.jeremy.spark
package spark.job

import org.apache.spark.sql.{Row, SparkSession}

class SparkJobTest extends org.scalatest.flatspec.AnyFlatSpec {

  val spark: SparkSession = SparkSession.builder().master("local[*]").appName("SparkJob").getOrCreate()

  "Job" should "run successfully" in {
    val inputFilePath = getClass.getResource("/testFile.csv").getPath
    val output = SparkJob.execute(spark, execParams = Map("inputPath" -> inputFilePath)).collect()
      .map(row => row.mkString("(", ",", ")"))
    println(output.mkString("Array(", ", ", ")"))
  }
}
