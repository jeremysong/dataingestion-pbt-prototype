package pbt

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalacheck.{Arbitrary, Gen}

class InputDataGen {

  def customerId: Arbitrary[Long] = Arbitrary(Gen.frequency(
    1 -> 1,
    1 -> 2,
    1 -> 3,
    1 -> 4,
    1 -> 5,
    5 -> Arbitrary.arbLong.arbitrary
  ))

  def hvaRow: Gen[Hva] = for {
    customerId <- customerId.arbitrary
    hva <- Gen.oneOf("sns_1st", "sns_nth", "", null)
  } yield Hva(customerId, hva)

  @Input(name = "HVA", kclass = classOf[Hva])
  def hvaData: Gen[List[Hva]] = Gen.choose(200, 1000) flatMap { size => Gen.listOfN(size, hvaRow)}

  def genderRow: Gen[Gender] = for {
    customerId <- customerId.arbitrary
    gender <- Gen.oneOf("M", "F", "", null)
  } yield Gender(customerId, gender)

  @Input(name = "GENDER", kclass = classOf[Gender])
  def genderData: Gen[List[Gender]] = Gen.choose(200, 1000) flatMap { size => Gen.listOfN(size, genderRow)}
}

object InputDataGen {

  def dataSets(spark: SparkSession): Gen[Map[String, List[_]]] = {
    val inputDataGen = new InputDataGen
    for {
      hvaData <- inputDataGen.hvaData
      genderData <- inputDataGen.genderData
    } yield {
      spark.createDataFrame(hvaData).createOrReplaceTempView("HVA")
      spark.createDataFrame(genderData).createOrReplaceTempView("GENDER")
      Map("HVA" -> hvaData, "GENDER" -> genderData)
    }
  }
}

case class Hva(customerId: Long, hva: String)

case class Gender(customerId: Long, gender: String)