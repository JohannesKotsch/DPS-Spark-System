import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object SparkStreamingFromDirectory {

  def main(args: Array[String]): Unit = {

    val sparkSession:SparkSession = SparkSession.builder()
      .master("local[3]")
      .appName("lse")
      .getOrCreate()

    val fileStreamDf = sparkSession.readStream
      .option("header", "true")
      .csv("examplestreamingfolder/")

  }
}
