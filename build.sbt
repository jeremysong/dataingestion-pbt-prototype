ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.15"

val sparkVersion = "3.2.1"

lazy val root = (project in file("."))
  .settings(
    name := "spark-pbt-prototype",
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.scalactic" %% "scalactic" % "3.2.12",
  "org.scalatest" %% "scalatest" % "3.2.12" % "test",
  "org.scalatestplus" %% "scalacheck-1-16" % "3.2.12.0" % "test"
)

libraryDependencies += "com.amazon.deequ" % "deequ" % "2.0.1-spark-3.2"
libraryDependencies += "org.scalacheck" %% "scalacheck" % "1.16.0"
