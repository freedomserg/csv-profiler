name := "csv_profiler"

version := "0.1"

scalaVersion := "2.12.11"
val sparkVersion = "3.0.0"
val circeVersion = "0.12.3"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.rogach" %% "scallop" % "3.4.0",
  "org.scalatest" %% "scalatest" % "3.2.0" % Test
)

libraryDependencies ++= Seq(
  "io.circe" %% "circe-core",
  "io.circe" %% "circe-generic",
  "io.circe" %% "circe-parser"
).map(_ % circeVersion)

parallelExecution in Test := false
