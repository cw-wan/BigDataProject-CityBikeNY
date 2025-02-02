package common

object Constants {
  // Kafka
  val KAFKA_BROKER = "localhost:9092"
  val KAFKA_TOPIC_BIKE = "bike_topic"

  // HDFS path
  val HDFS_BASE_PATH = "hdfs://localhost:9000"
  val HDFS_RAW_DATA_PATH = s"$HDFS_BASE_PATH/data/raw"
  val HDFS_PROCESSED_DATA_PATH = s"$HDFS_BASE_PATH/data/processed"
  val HDFS_MODEL_PATH = s"$HDFS_BASE_PATH/model"

  // Other constants
  val DATE_FORMAT = "yyyy-MM-dd HH:mm:ss"
}
