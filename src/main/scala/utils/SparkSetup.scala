package utils

import org.apache.spark.sql.SparkSession
import com.globalmentor.apache.hadoop.fs.BareLocalFileSystem
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.rdd.RDD

object SparkSetup {
  def createSession(): SparkSession = {
    val sparkBuilder = SparkSession.builder()
      .appName("Sessions Analyzer")
      .master("local[*]")

    if (System.getProperty("os.name").startsWith("Windows")) {
      sparkBuilder.config("spark.hadoop.fs.file.impl", classOf[BareLocalFileSystem].getName)
    }

    val spark = sparkBuilder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    spark
  }

  def readRawSessions(spark: SparkSession, path: String): RDD[String] = {
    val hadoopConf = new Configuration()
    hadoopConf.set("textinputformat.record.delimiter", "SESSION_START")

    // wholetextfile не работает, т.к. не понятно как передать кодировку
    spark.sparkContext.newAPIHadoopFile(
      path,
      classOf[TextInputFormat],
      classOf[LongWritable],
      classOf[Text],
      hadoopConf
    ).map { case (_, text) =>
      new String(text.getBytes, 0, text.getLength, "Windows-1251")
    }.filter(_.trim.nonEmpty)
  }
}