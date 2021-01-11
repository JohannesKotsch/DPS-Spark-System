import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{FloatType, IntegerType}
import org.apache.spark.sql.functions._

object LocalExample extends App {
  override def main(args: Array[String]) {
    val stockFile = "exampledata/stock.csv" // Should be some file on your system
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()

    import spark.implicits._

    val stockDFrame: DataFrame = spark.read
      .option("header", true)
      .csv(stockFile)
      .cache()

    stockDFrame
      .withColumn("StartPrice", stockDFrame("StartPrice") cast FloatType)
      .withColumn("TradedVolume", stockDFrame("TradedVolume") cast IntegerType)
      .withColumn("TotalVolume", $"StartPrice" * $"TradedVolume")
      .select("SecurityType", "TotalVolume")
      .groupBy("SecurityType")
      .agg(sum("TotalVolume"))
      .show()


    spark.stop()
  }
}
