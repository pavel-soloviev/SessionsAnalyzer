import org.apache.spark.sql.SparkSession
import com.globalmentor.apache.hadoop.fs.BareLocalFileSystem
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.io.{LongWritable, Text}

import models.UserSession
import analytics.AnalyticsProcessor

object task_run {
  def main(args: Array[String]): Unit = {
    val startTime = System.nanoTime()

    val sparkBuilder = SparkSession.builder()
      .appName("Sessions Analyzer")
      .master("local[*]")

    if (System.getProperty("os.name").startsWith("Windows")) {
      sparkBuilder.config("spark.hadoop.fs.file.impl", classOf[BareLocalFileSystem].getName)
    }

    val spark = sparkBuilder.getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val errorsAcc = sc.longAccumulator("ParseErrors")

    val logFilePath = "src/main/resources/sessions/*"
    val hadoopConf = new Configuration()
    hadoopConf.set("textinputformat.record.delimiter", "SESSION_START")

    // читаем сессии
    val rawSessionsRDD = sc.newAPIHadoopFile(
      logFilePath,
      classOf[TextInputFormat],
      classOf[LongWritable],
      classOf[Text],
      hadoopConf
    ).map { case (_, text) =>
      new String(text.getBytes, 0, text.getLength, "Windows-1251")
    }.filter(_.trim.nonEmpty)

    // парсим логи
    val sessionsRDD = rawSessionsRDD.map { text =>
      UserSession.parse(text, errorsAcc)
    }.cache()

    val totalSessions = sessionsRDD.count()

    // решение поставленных задач (аналитика)
    AnalyticsProcessor.runAnalysis(sessionsRDD, errorsAcc, totalSessions)

    val endTime = System.nanoTime()
    val durationSeconds = (endTime - startTime) / 1e9d
    println(f"\nОбщее время выполнения программы: $durationSeconds%.2f секунд")

    spark.stop()
  }
}