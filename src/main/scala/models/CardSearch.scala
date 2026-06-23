package models

import org.apache.spark.util.LongAccumulator
import scala.collection.mutable

case class CardSearch(
                       id: String,
                       params: Map[String, Array[String]],
                       results: Array[String],
                       openedDocs: Array[DocOpen] = Array.empty
                     )

object CardSearch {
  def parse(iterator: BufferedIterator[String], errorsAcc: LongAccumulator): Option[CardSearch] = {
    iterator.next()  // пропускаем CARD_SEARCH_START

    val paramsMap = mutable.Map.empty[String, mutable.ListBuffer[String]]
    var searchId = "UNKNOWN_CARD_ID"
    var results = Array.empty[String]
    var keepParsing = true

    while (iterator.hasNext && keepParsing) {
      val line = iterator.next()

      if (line.startsWith("CARD_SEARCH_END")) {
        if (iterator.hasNext) {
          val resultLine = iterator.next()
          val parts = resultLine.split("\\s+")
          if (parts.nonEmpty) {
            searchId = parts.head
            results = parts.tail
          }
        } else {
          errorsAcc.add(1L)
        }
        keepParsing = false
      } else if (line.startsWith("$")) {
        // парсим параметры
        val parts = line.split("\\s+", 2)
        if (parts.length == 2) {
          val paramName = parts(0)
          val paramValue = parts(1)

          paramsMap.getOrElseUpdate(paramName, mutable.ListBuffer.empty) += paramValue
        }
      }
    }

    val finalParams = paramsMap.map { case (k, v) => (k, v.toArray) }.toMap

    Some(CardSearch(id = searchId, params = finalParams, results = results))
  }
}