name := "SensorStatisticsData"

version := "0.1"

scalaVersion := "2.12.6"
lazy val sparkVersion = "3.1.1"

libraryDependencies++=Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.scalatest" %% "scalatest" % "3.0.8" % Test,
  "org.mockito" % "mockito-core" % "2.8.47" % "test"
)