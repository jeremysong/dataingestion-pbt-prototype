package org.jeremy.spark
package scalding

import spark.SparkJob

import com.twitter.scalding.{JobTest, TypedCsv}
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.io.Source

class ScaldingJobTest extends AnyFlatSpec with Matchers {

  val spark: SparkSession = SparkSession.builder().master("local[*]").appName("SparkJob").getOrCreate()

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

  private def assert(expected: Seq[String], actual: Seq[String]): Unit = {
    expected should contain theSameElementsAs actual
  }
}
