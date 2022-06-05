ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.15"

val sparkVersion = "3.2.1"

lazy val root = (project in file("."))
  .settings(
    name := "spark-pbt-prototype",
    idePackagePrefix := Some("org.jeremy.spark")
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.scalactic" %% "scalactic" % "3.2.12",
  "org.scalatest" %% "scalatest" % "3.2.12" % "test"
)

libraryDependencies ++= {
  val scaldingVersion = "0.17.4"

  Seq(
    "com.twitter" %% "scalding-core"    % scaldingVersion,
    "com.twitter" %% "scalding-commons" % scaldingVersion,
    "com.twitter" %% "scalding-repl"    % scaldingVersion,
  )
}

resolvers ++= Seq(
  Resolver.sonatypeRepo("releases"),
  "Concurrent Maven Repo" at "https://conjars.org/repo",
  "Twitter Maven Repo" at "https://maven.twttr.com"
)