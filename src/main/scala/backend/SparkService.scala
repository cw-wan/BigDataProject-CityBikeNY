package backend

import org.apache.spark.sql.{SparkSession, DataFrame}
import common.Constants
import org.apache.spark.sql.functions._

class SparkService(spark: SparkSession) {

  import spark.implicits._


  private def loadParquetData(path: String): DataFrame = {
    val df = spark.read.parquet(path)
    df.cache()
  }

  private def loadCSVData(path: String): DataFrame = {
    val df = spark.read.option("header", "true").csv(path)
    df.cache()
  }

  private val pathStation = s"${Constants.HDFS_PROCESSED_DATA_PATH}/station"
  private val pathZone = s"${Constants.HDFS_PROCESSED_DATA_PATH}/zone"
  private val pathZoneTS = s"${Constants.HDFS_PROCESSED_DATA_PATH}/zonets"

  val stationDF: DataFrame = loadParquetData(pathStation)
  val zoneDF: DataFrame = loadParquetData(pathZone)
  val zoneTSDF: DataFrame = loadCSVData(pathZoneTS)

  def getAllStations: Seq[Map[String, String]] = {
    stationDF.collect().map { row =>
      row.schema.fieldNames.map(field => field -> row.getAs[Any](field).toString).toMap
    }.toSeq
  }

  def getAllZones: Seq[Map[String, String]] = {
    zoneDF.collect().map { row =>
      row.schema.fieldNames.map(field => field -> row.getAs[Any](field).toString).toMap
    }.toSeq
  }

  def getZoneTSByTime(query: String): Seq[Map[String, String]] = {
    val normalizedQuery = query.replace(" ", "+")
    val filteredDF = zoneTSDF
      .filter(col("time") === normalizedQuery)
    filteredDF.show()
    filteredDF.collect().map { row =>
      row.schema.fieldNames.map(field =>
        field -> Option(row.getAs[Any](field)).map(_.toString).getOrElse("null")
      ).toMap
    }.toSeq
  }
}
