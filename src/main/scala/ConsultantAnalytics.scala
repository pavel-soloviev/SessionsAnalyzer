import org.apache.spark.sql.SparkSession
import scala.collection.mutable.ListBuffer
import com.globalmentor.apache.hadoop.fs.BareLocalFileSystem
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.io.{LongWritable, Text}

object ConsultantAnalytics {
  def main(args: Array[String]): Unit = {

    val startTime = System.nanoTime() // замерим общее время выполнения программы

    val sparkBuilder = SparkSession.builder()
      .appName("Sessions Analyzer")
      .master("local[*]")

    // нужно, чтобы обойти проблемы с правами доступа на винде
    if (System.getProperty("os.name").startsWith("Windows")) {
      sparkBuilder.config("spark.hadoop.fs.file.impl", classOf[BareLocalFileSystem].getName)
    }

    val spark = sparkBuilder.getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val logFilePath = "src/main/resources/sessions/*"

    val hadoopConf = new Configuration()
    hadoopConf.set("textinputformat.record.delimiter", "SESSION_START")

    // Читаем все файлы по маске, разбивая их на лету по SESSION_START
    val sessionsRDD = sc.newAPIHadoopFile(
        logFilePath,
        classOf[TextInputFormat],
        classOf[LongWritable],
        classOf[Text],
        hadoopConf
      ).map { case (_, text) => text.toString }
      .filter(_.trim.nonEmpty)

    // парсим каждую сессию
    val parsedRDD = sessionsRDD.map { sessionText =>
      val lines = sessionText.split("\n").map(_.trim).filter(_.nonEmpty)

      var countACC = 0
      var qsSearchIds = Set.empty[String]
      val qsDocOpens = ListBuffer.empty[((String, String), Int)] // ((Дата, DocId), 1)

      var lastWasQS = false // предыдущая строка начиналась с QS (считаем, что "QS" и запрос всегда на одной стоке)
      var lastWasCardSearchEnd = false // предыдущая строка содержала CARD_SEARCH_END

      for (line <- lines) {
        if (lastWasQS) {
          val parts = line.split(" ")
          if (parts.nonEmpty) {
            qsSearchIds += parts.head // сохраняем ID быстрого поиска
          }
          lastWasQS = false
        }

        else if (lastWasCardSearchEnd) {
          // ищем документ с идентификатором ACC_45616 в результатах поиска через карточку
          if (line.contains("ACC_45616")) {
            countACC += 1
          }
          lastWasCardSearchEnd = false
        }

        // проверяем текущую строку, чтобы выставить флаги для следующей
        if (line.startsWith("QS")) {
          lastWasQS = true
        } else if (line.startsWith("CARD_SEARCH_END")) {
          lastWasCardSearchEnd = true
        } else if (line.startsWith("DOC_OPEN")) {
          val parts = line.split(" ")
          // защита от ошибок формата лога
          if (parts.length >= 4) {
            val dateTime = parts(1)
            val searchId = parts(2)
            val docId = parts(3)

            /* проверяем, относится ли это открытие к быстрому поиску (совпадают id поиска и id в логе doc_open)
            и сортируем по дате
            */
            if (qsSearchIds.contains(searchId)) {
              val dateRaw = dateTime.split("_")(0)
              val dateParts = dateRaw.split("\\.")

              if (dateParts.length == 3) {
                val isoDate = s"${dateParts(2)}-${dateParts(1)}-${dateParts(0)}"
                qsDocOpens += (((isoDate, docId), 1))
              }
            }
          }
        }
      }
      // возвращаем результат по отдельной сессии
      (countACC, qsDocOpens.toList)
    }

    // 1
    val totalACC_45616 = parsedRDD.map(_._1).sum()
    println(s"Количество поисков документа ACC_45616 в карточке: ${totalACC_45616.toInt}")

    // 2
    val dailyDocOpens = parsedRDD
      .flatMap(_._2)
      .reduceByKey(_ + _)
      .sortByKey()

    println("\nОткрытия документов из быстрого поиска по дням:")
    dailyDocOpens.collect().foreach { case ((date, docId), count) =>
      println(s"Дата: $date | Документ: $docId | Открытий: $count")
    }

    val endTime = System.nanoTime()
    val durationSeconds = (endTime - startTime) / 1e9d
    println(f"\nОбщее время выполнения программы: $durationSeconds%.2f секунд")

    spark.stop()
  }
}
