package preprocessing

import org.apache.spark.sql.{DataFrame, SparkSession}
import common.Constants
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object DataCleaner {
  def main(args: Array[String]): Unit = {
    // create SparkSession
    val spark = SparkSession.builder()
      .appName("Hadoop Preprocessor")
      .master("local[*]") // local mode for development
      .getOrCreate()

    // read raw citybike data (csv) from hdfs
    val cityBikeDataPath = s"${Constants.HDFS_RAW_DATA_PATH}/citybike"
    val cityBikeSchema = new StructType(Array[StructField](
      StructField("ride_id", DataTypes.StringType, nullable = false, Metadata.empty),
      StructField("rideable_type", DataTypes.StringType, nullable = false, Metadata.empty),
      StructField("started_at", DataTypes.TimestampType, nullable = false, Metadata.empty),
      StructField("ended_at", DataTypes.TimestampType, nullable = false, Metadata.empty),
      StructField("start_station_name", DataTypes.StringType, nullable = false, Metadata.empty),
      StructField("start_station_id", DataTypes.StringType, nullable = false, Metadata.empty),
      StructField("end_station_name", DataTypes.StringType, nullable = false, Metadata.empty),
      StructField("end_station_id", DataTypes.StringType, nullable = false, Metadata.empty),
      StructField("start_lat", DataTypes.DoubleType, nullable = false, Metadata.empty),
      StructField("start_lng", DataTypes.DoubleType, nullable = false, Metadata.empty),
      StructField("end_lat", DataTypes.DoubleType, nullable = false, Metadata.empty),
      StructField("end_lng", DataTypes.DoubleType, nullable = false, Metadata.empty),
      StructField("member_casual", DataTypes.StringType, nullable = false, Metadata.empty)
    ))
    var cityBikeDF: DataFrame = spark.read
      .option("header", "true")
      .option("recursiveFileLookup", "true")
      .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
      .option("mode", "DROPMALFORMED")
      .schema(cityBikeSchema)
      .csv(cityBikeDataPath)

    cityBikeDF.show()
    println(s"#Record in CityBike: ${cityBikeDF.count()}")
    cityBikeDF = cityBikeDF.withColumn(
      "duration",
      (unix_timestamp(col("ended_at")) - unix_timestamp(col("started_at"))) / 3600.0
    )
    // filter out abnormal data, whose duration is negative or above 24 hours
    cityBikeDF = cityBikeDF.filter(col("duration").between(0, 24))
    println(s"After cleaning, record count: ${cityBikeDF.count()}")

    // add truncated timestamp to facilitate future analysis
    cityBikeDF = cityBikeDF
      .withColumn("started_at_full", date_trunc("hour", col("started_at")))
      .withColumn("ended_at_full", date_trunc("hour", col("ended_at")))

    cityBikeDF.show()
    cityBikeDF.printSchema()

    // read raw weather data
    val weatherDataPath = s"${Constants.HDFS_RAW_DATA_PATH}/weather"
    val weatherSchema = new StructType(Array[StructField](
      StructField("dt", DataTypes.LongType, nullable = true, Metadata.empty),
      StructField("dt_iso", DataTypes.StringType, nullable = true, Metadata.empty),
      StructField("timezone", DataTypes.StringType, nullable = true, Metadata.empty),
      StructField("city_name", DataTypes.StringType, nullable = true, Metadata.empty),
      StructField("lat", DataTypes.DoubleType, nullable = true, Metadata.empty),
      StructField("lon", DataTypes.DoubleType, nullable = true, Metadata.empty),
      StructField("temp", DataTypes.DoubleType, nullable = true, Metadata.empty),
      StructField("visibility", DataTypes.DoubleType, nullable = true, Metadata.empty),
      StructField("dew_point", DataTypes.DoubleType, nullable = true, Metadata.empty),
      StructField("feels_like", DataTypes.DoubleType, nullable = true, Metadata.empty),
      StructField("temp_min", DataTypes.DoubleType, nullable = true, Metadata.empty),
      StructField("temp_max", DataTypes.DoubleType, nullable = true, Metadata.empty),
      StructField("pressure", DataTypes.DoubleType, nullable = true, Metadata.empty),
      StructField("sea_level", DataTypes.DoubleType, nullable = true, Metadata.empty),
      StructField("grnd_level", DataTypes.DoubleType, nullable = true, Metadata.empty),
      StructField("humidity", DataTypes.DoubleType, nullable = true, Metadata.empty),
      StructField("wind_speed", DataTypes.DoubleType, nullable = true, Metadata.empty),
      StructField("wind_deg", DataTypes.DoubleType, nullable = true, Metadata.empty),
      StructField("wind_gust", DataTypes.DoubleType, nullable = true, Metadata.empty),
      StructField("rain_1h", DataTypes.DoubleType, nullable = true, Metadata.empty),
      StructField("rain_3h", DataTypes.DoubleType, nullable = true, Metadata.empty),
      StructField("snow_1h", DataTypes.DoubleType, nullable = true, Metadata.empty),
      StructField("snow_3h", DataTypes.DoubleType, nullable = true, Metadata.empty),
      StructField("clouds_all", DataTypes.DoubleType, nullable = true, Metadata.empty),
      StructField("weather_id", DataTypes.IntegerType, nullable = true, Metadata.empty),
      StructField("weather_main", DataTypes.StringType, nullable = true, Metadata.empty),
      StructField("weather_description", DataTypes.StringType, nullable = true, Metadata.empty),
      StructField("weather_icon", DataTypes.StringType, nullable = true, Metadata.empty),
    ))
    var weatherDF: DataFrame = spark.read
      .option("header", "true")
      .option("recursiveFileLookup", "true")
      .schema(weatherSchema)
      .csv(weatherDataPath)

    // take care of timestamp and drop useless columns
    weatherDF = weatherDF
      .drop("city_name", "dt_iso", "timezone", "sea_level", "grnd_level", "weather_id", "weather_icon", "pressure", "temp_min", "temp_max", "lat", "lon", "dew_point", "rain_3h", "snow_3h")
      .withColumn(
        "time",
        from_utc_timestamp(from_unixtime(col("dt")), "America/New_York")
      )
      .drop("dt").dropDuplicates("time")

    // take care of missing data
    val columnsToFill = Seq("visibility")
    val sortedDF = weatherDF.orderBy("time")
    val windowSpec = Window.orderBy("time").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    weatherDF = columnsToFill.foldLeft(sortedDF) { (dfAcc, colName) =>
      dfAcc.withColumn(
        colName,
        last(colName, ignoreNulls = true).over(windowSpec)
      )
    }

    weatherDF.show()
    println(s"#Record in WeatherDF: ${weatherDF.count()}")
    weatherDF.printSchema()

    // 1) save cleaned city bike data
    val outputPathCityBikeParquet = s"${Constants.HDFS_PROCESSED_DATA_PATH}/citybike"

    cityBikeDF
      .write
      .mode("overwrite")
      .parquet(outputPathCityBikeParquet)

    println(s"Cleaned CityBike data written to $outputPathCityBikeParquet")

    // 2) save cleaned weather data
    val outputPathWeatherParquet = s"${Constants.HDFS_PROCESSED_DATA_PATH}/weather"

    weatherDF
      .write
      .mode("overwrite")
      .parquet(outputPathWeatherParquet)

    println(s"Cleaned Weather data written to $outputPathWeatherParquet")

    spark.stop()
  }
}
