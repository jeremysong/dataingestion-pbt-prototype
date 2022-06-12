package pbt

import com.amazon.deequ.{VerificationResult, VerificationSuite}
import com.amazon.deequ.checks.{Check, CheckLevel, CheckStatus}
import com.amazon.deequ.constraints.ConstraintStatus
import job.SparkJob
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalacheck.Shrink
import org.scalatest.AppendedClues
import org.scalatest.matchers.should.Matchers
import org.scalatest.propspec.AnyPropSpec
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class PropertyBasedTesting extends AnyPropSpec with ScalaCheckDrivenPropertyChecks with Matchers with AppendedClues {

  val spark: SparkSession = SparkSession.builder().master("local[*]").appName("SparkJob").getOrCreate()

  implicit def noShrink[T]: Shrink[T] = Shrink.shrinkAny

  /**
   * Run tests to compare the outputs from Spark job and Scalding job. It does several things
   * 1. Execute the Spark job and gets an output
   * 1. Execute the Scalding job and gets an output.
   * 1. Compare the Spark job and Scalding job.
   */
  property("spark job should pass pbt") {
    forAll (InputDataGen.dataSets(spark)) { inputDataSets =>
      val output = SparkJob.execute(spark, execParams = Map())

      val verificationResult = VerificationSuite()
        .onData(output)
        .addCheck(
          Check(CheckLevel.Error, "unit testing my data")
            .hasSize(_ > 5)
            .areComplete(Seq("customerId", "hva", "sum"))
            .isContainedIn("hva", Array("sns_1st", "sns_nth"))
            .isPositive("customerId")
            .isNonNegative("sum")
            .isPrimaryKey("customerId", "hva")
            .hasNumberOfDistinctValues("hva", _ <= 2)
      ).run()

      verify(verificationResult, inputDataSets, output)
    }
  }

  private def verify(verificationResult: VerificationResult, inputDataSets: Map[String, List[Any]], outputData: DataFrame) = {
    verificationResult.status shouldBe CheckStatus.Success withClue {
      val resultsForAllConstraints = verificationResult.checkResults
        .flatMap { case (_, checkResult) => checkResult.constraintResults }

      val errors = resultsForAllConstraints
        .filter {
          _.status != ConstraintStatus.Success
        }
        .map { result => s"${result.constraint}: ${result.message.get}" }

      val inputData = inputDataSets.map(entry => entry._1 + ": " + entry._2.mkString("Array(", ", ", ")")).mkString("\n\t")

      s"""
         |InputData:
         |    $inputData
         |
         |OutputData:
         |    ${outputData.collect().mkString("Array(", ", ", ")")}
         |
         |We found errors in the data:
         |  ${errors.mkString("\n")}

         |""".stripMargin
    }
  }
}
