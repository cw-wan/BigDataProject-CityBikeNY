ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.15"

lazy val root = (project in file("."))
  .settings(
    name := "New_York_CityBike"
  )

libraryDependencies ++= Seq(
  // Spark core & SQL
  "org.apache.spark" %% "spark-core" % "3.3.0",
  "org.apache.spark" %% "spark-sql" % "3.3.0",
  // Spark MLlib for ML
  "org.apache.spark" %% "spark-mllib" % "3.3.0",
  // Hadoop client
  "org.apache.hadoop" % "hadoop-client" % "3.3.1",
  // log
  "org.apache.logging.log4j" % "log4j-api" % "2.17.2",
  "org.apache.logging.log4j" % "log4j-core" % "2.17.2"
)
