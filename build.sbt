ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

lazy val root = (project in file("."))
  .settings(
    name := "sparkGettingStarted"
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.3.2",
  "org.apache.spark" %% "spark-sql" % "3.3.2",
  "com.databricks" %% "spark-xml" % "0.16.0",
  "org.postgresql" % "postgresql" % "42.5.4",
  // "nl.gn0s1s" % "sbt-dotenv" % "3.0.0"
)
