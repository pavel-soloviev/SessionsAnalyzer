import org.apache.spark.sql.SparkSession
import scala.collection.mutable.ListBuffer
import com.globalmentor.apache.hadoop.fs.BareLocalFileSystem
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.io.{LongWritable, Text}
//import java.time.LocalDate


case class QuickSearch(id: String, query: String, results: List[String])

case class CardSearch(param0: List[String], param134: List[String], resultsLine: String)

case class DocOpen(isoDate: String, searchId: String, docId: String)

case class UserSession(
                        date: String,
                        quickSearches: List[QuickSearch],
                        cardSearches: List[CardSearch],
                        docOpens: List[DocOpen]
                      )

object ConsultantAnalytics {
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

    val logFilePath = "src/main/resources/sessions/*"

    val hadoopConf = new Configuration()
    hadoopConf.set("textinputformat.record.delimiter", "SESSION_START")

    // читаем все сессии
    val rawSessionsRDD = sc.newAPIHadoopFile(
        logFilePath,
        classOf[TextInputFormat],
        classOf[LongWritable],
        classOf[Text],
        hadoopConf
      ).map { case (_, text) =>
        new String(text.getBytes, 0, text.getLength, "Windows-1251")
      }
      .filter(_.trim.nonEmpty)


    // пирсинг логов
    val parsedSessionsRDD = rawSessionsRDD.map { sessionText =>
      val lines = sessionText.split("\n").map(_.trim).filter(_.nonEmpty)

      val qsList = ListBuffer.empty[QuickSearch]
      val cardList = ListBuffer.empty[CardSearch]
      val opensList = ListBuffer.empty[DocOpen]

      /* запомним дату сессии, т.к. в некоторых логах есть баг и после doc_open не пишется дата;
       для таких doc_open будем считать, что документ открыли в тот же день, когда началась сессия */
      var sessionIsoDate = "UNKNOWN_DATE"
      if (lines.nonEmpty) {
        val startDateTimeRaw = lines.head // "08.02.2020_07:46:04"
        val startDateRaw = startDateTimeRaw.split("_")(0)
        val startDateParts = startDateRaw.split("\\.")
        sessionIsoDate = s"${startDateParts(2)}-${startDateParts(1)}-${startDateParts(0)}"
      }

      var pendingQsQuery: Option[String] = None // хранит запрос из строки QS до следующей строки
      var lastWasCardSearchEnd = false

      var inCardSearch = false // true если мы парсим параметры поиска через КП
      val currentParam0 = ListBuffer.empty[String]
      val currentParam134 = ListBuffer.empty[String]

      for (line <- lines) {
        if (pendingQsQuery.isDefined) {
          val parts = line.split("\\s+")
          if (parts.nonEmpty) {
            val id = parts.head
            val results = parts.tail.toList
            qsList += QuickSearch(id, pendingQsQuery.get, results)
          }
          pendingQsQuery = None
        } else if (lastWasCardSearchEnd) {
          cardList += CardSearch(currentParam0.toList, currentParam134.toList, resultsLine = line)
          currentParam0.clear()
          currentParam134.clear()
          lastWasCardSearchEnd = false
        }

        if (line.startsWith("QS")) {
          val startIdx = line.indexOf("{")
          val endIdx = line.lastIndexOf("}")
          val queryText = line.substring(startIdx + 1, endIdx)
          pendingQsQuery = Some(queryText)
        } else if (line.startsWith("CARD_SEARCH_START")) {
          inCardSearch = true
        } else if (line.startsWith("CARD_SEARCH_END")) {
          inCardSearch = false
          lastWasCardSearchEnd = true
        } else if (inCardSearch) {
          if (line.startsWith("$0 ")) {
            currentParam0 += line.stripPrefix("$0 ").trim
          } else if (line.startsWith("$134 ")) {
            currentParam134 += line.stripPrefix("$134 ").trim
          }
        } else if (line.startsWith("DOC_OPEN")) {
          val parts = line.split("\\s+")

          // стадартный лог (DOC_OPEN + Дата + ID_поиска + ID_документа)
          if (parts.length >= 4) {
            val dateTime = parts(1)
            val searchId = parts(2)
            val docId = parts(3)

            val dateRaw = dateTime.split("_")(0)
            val dateParts = dateRaw.split("\\.")

            if (dateParts.length == 3) {
              val isoDate = s"${dateParts(2)}-${dateParts(1)}-${dateParts(0)}"
              opensList += DocOpen(isoDate, searchId, docId)
            } else {
              println("Неизвестный формат даты.")
            }
          }
          // лог с ошибкой (DOC_OPEN + ID_поиска + ID_документа)
          else if (parts.length == 3) {
            val searchId = parts(1)
            val docId = parts(2)
            opensList += DocOpen(sessionIsoDate, searchId, docId)
          } else {
            println("Неизвестный формат doc_open.")
          }
        }
      }

      UserSession(sessionIsoDate, qsList.toList, cardList.toList, opensList.toList)
    }

    parsedSessionsRDD.cache()


    // 1. Поиск документа ACC_45616 в карточке
    val accSearchVariantsRDD = parsedSessionsRDD.flatMap { session =>
      session.cardSearches.flatMap { card =>
        val targetEng = "ACC_45616"
        val targetRus = "АСС_45616" // первые три буквы на кириллице

        val foundVariants = scala.collection.mutable.ListBuffer.empty[String]

        // параметр $0
        card.param0.foreach { p =>
          if (p.contains(targetEng)) foundVariants += "Параметр $0 (Английское ACC)"
          if (p.contains(targetRus)) foundVariants += "Параметр $0 (Русское АСС)"
        }

        // параметр $134
        card.param134.foreach { p =>
          if (p.contains(targetEng)) foundVariants += "Параметр $134 (Английское ACC)"
          if (p.contains(targetRus)) foundVariants += "Параметр $134 (Русское АСС)"
        }

        foundVariants.distinct.toList
      }
    }

    val variantsCount = accSearchVariantsRDD.countByValue()

    println("\nДетализация поисков документа ACC_45616 в карточке")
    val totalACC_45616 = variantsCount.values.sum
    println(s"Общее количество поисков: $totalACC_45616")

    variantsCount.foreach { case (variantName, count) =>
      println(s" - $variantName: $count")
    }

    // 2. Открытия документов из быстрого поиска
    val dailyDocOpens = parsedSessionsRDD.flatMap { session =>
        // множество ID быстрых поисков для этой сессии
        val qsIds = session.quickSearches.map(_.id).toSet

        // оставляем только те открытия документов, которые были из быстрого поиска
        val qsOpens = session.docOpens.filter(open => qsIds.contains(open.searchId))

        qsOpens.map(open => ((open.isoDate, open.docId), 1))
      }
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
