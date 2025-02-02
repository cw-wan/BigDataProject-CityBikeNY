package common

object Constants {
  // HDFS path
  val HDFS_BASE_PATH = "hdfs://localhost:9000"
  val HDFS_RAW_DATA_PATH = s"$HDFS_BASE_PATH/data/raw"
  val HDFS_PROCESSED_DATA_PATH = s"$HDFS_BASE_PATH/data/processed"
}
