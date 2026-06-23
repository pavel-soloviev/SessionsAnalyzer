package analytics

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator

import models.UserSession

object AnalyticsProcessor {

  def runAnalysis(sessionsRDD: RDD[UserSession], parseErrorsAcc: LongAccumulator, totalSessions: Long): Unit = {

    // 1. Аналитика: Поиск ACC_45616 в карточке
    val accSearchVariantsRDD = sessionsRDD.flatMap { session =>
      session.cardSearches.flatMap { card =>
        val targetEng = "ACC_45616"
        val targetRus = "АСС_45616"

        val foundVariants = scala.collection.mutable.ListBuffer.empty[String]

        card.param0.foreach { p =>
          if (p.contains(targetEng)) foundVariants += "Параметр $0 (Английское ACC)"
          if (p.contains(targetRus)) foundVariants += "Параметр $0 (Русское АСС)"
        }

        card.param134.foreach { p =>
          if (p.contains(targetEng)) foundVariants += "Параметр $134 (Английское ACC)"
          if (p.contains(targetRus)) foundVariants += "Параметр $134 (Русское АСС)"
        }

        foundVariants.distinct.toArray
      }
    }

    val variantsCount = accSearchVariantsRDD.countByValue()

    println(s"\nВсего обработано сессий: $totalSessions")
    println(s"Ошибок при парсинге логов: ${parseErrorsAcc.value}")

    println("\nДетализация поисков документа ACC_45616 в карточке")
    val totalACC_45616 = variantsCount.values.sum
    println(s"Общее количество поисков: $totalACC_45616")
    variantsCount.foreach { case (variantName, count) =>
      println(s" - $variantName: $count")
    }

    // 2. Аналитика: Открытия документов из быстрого поиска
    val dailyDocOpens = sessionsRDD.flatMap { session =>
        val qsIds = session.quickSearches.map(_.id).toSet
        val qsOpens = session.docOpens.filter(open => qsIds.contains(open.searchId))
        qsOpens.map(open => ((open.isoDate, open.docId), 1))
      }
      .reduceByKey(_ + _)
      .sortByKey()

    println("\nОткрытия документов из быстрого поиска по дням:")
    dailyDocOpens.collect().foreach { case ((date, docId), count) =>
      println(s"Дата: $date | Документ: $docId | Открытий: $count")
    }
  }
}