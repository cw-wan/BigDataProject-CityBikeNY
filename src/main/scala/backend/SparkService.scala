package backend

import org.apache.spark.sql.{SparkSession, DataFrame}
import common.Constants

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

  val stationDF: DataFrame = loadParquetData(pathStation)
  val zoneDF: DataFrame = loadParquetData(pathZone)

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
}
