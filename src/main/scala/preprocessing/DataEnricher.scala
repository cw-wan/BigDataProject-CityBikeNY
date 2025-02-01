package preprocessing

import common.Constants
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DataEnricher {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Data Enricher with Zones")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // --------------------------------------------------------------------
    // 1. Read processed City Bike data
    // --------------------------------------------------------------------
    val bikeDataPath = s"${Constants.HDFS_PROCESSED_DATA_PATH}/citybike"
    val bikeDF = spark.read.parquet(bikeDataPath)
    println(s"Loaded cleaned bike data, record count: ${bikeDF.count()}")
    bikeDF.show(5)
    bikeDF.printSchema()

    // --------------------------------------------------------------------
    // 2. Read processed Weather data
    // --------------------------------------------------------------------
    val weatherDataPath = s"${Constants.HDFS_PROCESSED_DATA_PATH}/weather"
    val weatherDF = spark.read.parquet(weatherDataPath)
    println(s"Loaded cleaned weather data, record count: ${weatherDF.count()}")
    weatherDF.show(5)
    weatherDF.printSchema()

    // --------------------------------------------------------------------
    // 3. Extract unique station information from bikeDF
    //    We union the start and end station info and then remove duplicates
    // --------------------------------------------------------------------
    val startStationDF = bikeDF.select(
      col("start_station_id").as("station_id"),
      col("start_station_name").as("station_name"),
      col("start_lat").as("station_lat"),
      col("start_lng").as("station_lng")
    ).na.drop()

    val endStationDF = bikeDF.select(
      col("end_station_id").as("station_id"),
      col("end_station_name").as("station_name"),
      col("end_lat").as("station_lat"),
      col("end_lng").as("station_lng")
    ).na.drop()

    // Union, distinct, and keep only one row per station_id
    val stationDF = startStationDF
      .union(endStationDF)
      .distinct()
      .groupBy("station_id")
      .agg(
        first("station_name").as("station_name"),
        first("station_lat").as("station_lat"),
        first("station_lng").as("station_lng")
      )

    stationDF.show(5, truncate = false)
    println(s"Number of unique stations: ${stationDF.count()}")

    // --------------------------------------------------------------------
    // 4. Cluster stations into 10 zones based on (station_lat, station_lng) using KMeans
    // --------------------------------------------------------------------
    // 4.1. Create a feature vector for lat/lng
    val assembler = new VectorAssembler()
      .setInputCols(Array("station_lat", "station_lng"))
      .setOutputCol("features")

    val stationFeaturesDF = assembler.transform(stationDF.na.drop(Seq("station_lat","station_lng")))

    // 4.2. Train KMeans with k
    val k = 50
    val kmeans = new KMeans()
      .setK(k)
      .setSeed(42)
      .setFeaturesCol("features")
      .setPredictionCol("zone_id")

    val kmeansModel = kmeans.fit(stationFeaturesDF)

    // 4.3. Assign each station to a zone (prediction => zone_id)
    val stationWithZoneDF = kmeansModel.transform(stationFeaturesDF)
      .drop("features") // optional: remove the temporary features column

    stationWithZoneDF.show(5, truncate = false)

    // --------------------------------------------------------------------
    // 5. Build a zoneDF (zone_id, zone_lat, zone_lng) from cluster centers
    //    Each zone_id is the cluster ID, zone_lat, zone_lng are cluster centroid
    // --------------------------------------------------------------------
    val clusterCenters = kmeansModel.clusterCenters.zipWithIndex.map { case (center, idx) =>
      (idx, center(0), center(1)) // center(0) = lat centroid, center(1) = lng centroid
    }

    import spark.implicits._
    val zoneDF = clusterCenters.toSeq.toDF("zone_id", "zone_lat", "zone_lng")

    zoneDF.show(truncate = false)

    // --------------------------------------------------------------------
    // 6. Calculate demand and supply aggregated at the zone level
    //    (zone_id, time) => total demand, total supply
    // --------------------------------------------------------------------
    // 6.1. Calculate station-level demand
    val demandDF = bikeDF
      .groupBy(col("start_station_id"), col("started_at_full"))
      .count()
      .withColumnRenamed("count", "station_demand")

    // Join to get zone_id for each station, then group by (zone_id, time)
    val zoneDemandDF = demandDF
      .join(stationWithZoneDF, stationWithZoneDF("station_id") === demandDF("start_station_id"))
      .groupBy("zone_id", "started_at_full")
      .agg(sum("station_demand").as("demand"))
      .withColumnRenamed("started_at_full", "time")

    zoneDemandDF.show(5)

    // 6.2. Calculate station-level supply
    val supplyDF = bikeDF
      .groupBy(col("end_station_id"), col("ended_at_full"))
      .count()
      .withColumnRenamed("count", "station_supply")

    val zoneSupplyDF = supplyDF
      .join(stationWithZoneDF, stationWithZoneDF("station_id") === supplyDF("end_station_id"))
      .groupBy("zone_id", "ended_at_full")
      .agg(sum("station_supply").as("supply"))
      .withColumnRenamed("ended_at_full", "time")

    zoneSupplyDF.show(5)

    // --------------------------------------------------------------------
    // 7. Replace stationDF with zoneDF in the crossJoin with weatherDF
    //    Then join with zoneDemandDF and zoneSupplyDF
    // --------------------------------------------------------------------
    spark.conf.set("spark.sql.crossJoin.enabled", true)

    // Cartesian product between zone and weather => (zone_id, time, weatherCols...)
    val zoneTimeDF = zoneDF.crossJoin(weatherDF)

    println(s"Number of records in zoneTimeDF (zone x weather): ${zoneTimeDF.count()}")

    // Join with zoneDemandDF
    val zoneTimeWithDemand = zoneTimeDF
      .join(
        zoneDemandDF,
        zoneTimeDF("zone_id") === zoneDemandDF("zone_id") &&
          zoneTimeDF("time") === zoneDemandDF("time"),
        "left"
      )
      .drop(zoneDemandDF("zone_id"))
      .drop(zoneDemandDF("time"))

    // Join with zoneSupplyDF
    val zoneTimeWithDemandSupply = zoneTimeWithDemand
      .join(
        zoneSupplyDF,
        zoneTimeWithDemand("zone_id") === zoneSupplyDF("zone_id") &&
          zoneTimeWithDemand("time") === zoneSupplyDF("time"),
        "left"
      )
      .drop(zoneSupplyDF("zone_id"))
      .drop(zoneSupplyDF("time"))

    // --------------------------------------------------------------------
    // 8. Select necessary columns, fill null demand/supply with 0
    // --------------------------------------------------------------------
    val zoneTimeSeriesDF = zoneTimeWithDemandSupply
      .select(
        col("zone_id"),
        col("time"),
        col("demand"),
        col("supply")
      )
      .na.fill(0, Seq("demand", "supply"))

    zoneTimeSeriesDF.show(5, truncate = false)
    println(s"Number of records in zoneTimeSeriesDF: ${zoneTimeSeriesDF.count()}")

    // --------------------------------------------------------------------
    // 9. Write out the results
    // --------------------------------------------------------------------
    val outputStation = s"${Constants.HDFS_PROCESSED_DATA_PATH}/station"
    val outputZone = s"${Constants.HDFS_PROCESSED_DATA_PATH}/zone"
    val outputZoneTS = s"${Constants.HDFS_PROCESSED_DATA_PATH}/zonets"

    // stationWithZoneDF => each station with assigned zone_id
    stationWithZoneDF.write.mode("overwrite").parquet(outputStation)

    // zoneDF => basic zone info (zone_id, zone_lat, zone_lng)
    zoneDF.write.mode("overwrite").parquet(outputZone)

    // zoneTimeSeriesDF => final data at zone/time level with weather and demand/supply
    zoneTimeSeriesDF.write.mode("overwrite").parquet(outputZoneTS)

    // test writing
    val testDF = spark.read.parquet(outputZoneTS)
    testDF.show()
    println(s"Successfully written records: ${testDF.count()}")
    testDF.printSchema()

    spark.stop()
  }
}
