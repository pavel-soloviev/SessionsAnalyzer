package analytics

import org.apache.spark.rdd.RDD
import models.UserSession

object DocOpensAnalyzer {
  def run(sessionsRDD: RDD[UserSession]): Unit = {
    val dailyDocOpens = sessionsRDD.flatMap { session =>
        session.quickSearches.flatMap { qs =>
          qs.docOpens.map { open =>
            ((open.timestamp.toLocalDate.toString, open.docId), 1)
          }
        }
      }
      .reduceByKey(_ + _)
      .sortByKey()

    println("\nОткрытия документов из быстрого поиска по дням:")
    dailyDocOpens.collect().foreach { case ((date, docId), count) =>
      println(s"Дата: $date | Документ: $docId | Открытий: $count")
    }
  }
}

