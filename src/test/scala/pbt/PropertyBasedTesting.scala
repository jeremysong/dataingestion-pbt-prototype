package org.jeremy.spark
package pbt

import spark.SparkJob

import com.twitter.scalding.{JobTest, TypedCsv}
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.io.Source

class PropertyBasedTesting extends AnyFlatSpec with Matchers {

  val spark: SparkSession = SparkSession.builder().master("local[*]").appName("SparkJob").getOrCreate()

  /**
   * Run tests to compare the outputs from Spark job and Scalding job. It does several things
   * 1. Execute the Spark job and gets an output
   * 1. Execute the Scalding job and gets an output.
   * 1. Compare the Spark job and Scalding job.
   */
  "scalding job" should "execute successfully" in {
    // TODO input file should be generated dynamically by property-based testing
    val inputFilePath = getClass.getResource("/testFile.csv").getPath

    // Run spark code
    val output = SparkJob.execute(spark, execParams = Map("inputPath" -> inputFilePath)).collect()
      .map(row => row.mkString("(", ",", ")"))

    // Run scaling code (reference model)
    val inputData: List[(String, String)] = Source.fromFile(inputFilePath).getLines().map {
      line => {
        val splits = line.split(",")
        (splits{0}, splits{1})
      }
    }.toList

    JobTest[ScaldingJob]
      .arg("inputPath", "inputFile")
      .arg("outputPath", "outputFile")
      .source(TypedCsv[(Long, String)]("inputFile"), inputData)
      .sink[(Long, String, Long)](TypedCsv[(Long, String, Long)]("outputFile")) {
        buffer => {
          assert(buffer.map(line => line.toString()), output)
        }
      }
      .run
      .finish()
  }

  /**
   * Asserts that the expected outputs are same as the actual output.
   *
   * The order does not matter.
   *
   * @param expected the expected outputs.
   * @param actual the actual output.
   */
  private def assert(expected: Seq[String], actual: Seq[String]): Unit = {
    expected should contain theSameElementsAs actual
  }
}
