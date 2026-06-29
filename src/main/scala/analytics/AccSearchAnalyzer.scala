package analytics

import org.apache.spark.rdd.RDD
import models.UserSession

object AccSearchAnalyzer {
  def run(sessionsRDD: RDD[UserSession]): Unit = {

    val searchValues = Array(
      ("ACC_45616", "(Английское ACC)"),
      ("АСС_45616", "(Русское АСС)")
    )
    val targetFields = Set("$0", "$134")

    val accSearchVariantsRDD = sessionsRDD.flatMap { session =>
      session.cardSearches.flatMap { card =>
        card.params
          .filter { case (field, _) => targetFields.contains(field) }
          .flatMap { case (field, valuesArray) =>
            searchValues.collect {
              case (search, name) if valuesArray.exists(_.contains(search)) =>
                s"Параметр $field $name"
            }
          }
          .toSet
      }
    }

    val variantsCount = accSearchVariantsRDD.countByValue()

    println("\nДетализация поисков документа ACC_45616 в карточке")
    val totalACC_45616 = variantsCount.values.sum
    println(s"Общее количество поисков: $totalACC_45616")

    variantsCount.foreach { case (variantName, count) =>
      println(s" - $variantName: $count")
    }
  }
}

