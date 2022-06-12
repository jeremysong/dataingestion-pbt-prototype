package job

import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkJob {

  def execute(spark: SparkSession, execParams: Map[String, String]): DataFrame = {
    spark.sql(
      """
        |SELECT HVA.customerId, hva, count(1) as sum FROM HVA, GENDER
        |    WHERE HVA.customerId == GENDER.customerId
        |    AND HVA.customerId > 1
        |    AND HVA.hva is not NULL AND HVA.hva != ''
        |    AND GENDER.gender == 'F'
        |    GROUP BY HVA.customerId, HVA.hva
        |""".stripMargin
      )
  }
}
