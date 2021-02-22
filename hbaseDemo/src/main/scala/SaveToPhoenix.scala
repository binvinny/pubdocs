
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.phoenix.spark._

import org.apache.phoenix.spark.datasource.v2.PhoenixDataSource
object SaveToPhoenix {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("phoenix-test")
      .master("local")
      .getOrCreate()

    // Load data from TABLE1
    val df = spark.sqlContext
      .read
      .format("phoenix")
      .options(Map("table" -> "TABLE1", PhoenixDataSource.ZOOKEEPER_URL -> "phoenix-server:2181"))
      .load

    df.filter(df("COL1") === "test_row_1" && df("ID") === 1L)
      .select(df("ID"))
      .show
  }
}
