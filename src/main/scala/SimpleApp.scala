import org.apache.spark.streaming.Seconds
import org.apache.spark.{SparkConf, SparkContext}

object SimpleApp extends App {
  override def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Simple Application")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    System.setProperty("com.amazonaws.services.s3.enableV4", "true")
    sc.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    sc.hadoopConfiguration.set("com.amazonaws.services.s3.enableV4", "true")
    sc.hadoopConfiguration.set("fs.s3a.endpoint", "s3.eu-central-1.amazonaws.com")

    sc.hadoopConfiguration.set("fs.s3a.access.key", args(0))
    sc.hadoopConfiguration.set("fs.s3a.secret.key", args(1))

    val ssc = new org.apache.spark.streaming.StreamingContext(
      sc, Seconds(60))
    val lines = ssc
      .textFileStream("s3a://deutsche-boerse-xetra-pds/2021-01-11/")

//    lines.filter(line => line.contains("a")).count().print()
//    lines.foreachRDD(rdd => rdd.filter(line => line.contains("A")).count())


    ssc.start() // Start the computation
    ssc.awaitTermination() // Wait for the computation to terminate
  }
}

