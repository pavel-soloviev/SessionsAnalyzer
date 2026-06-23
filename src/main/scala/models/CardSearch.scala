package models

import org.apache.spark.util.LongAccumulator

import scala.collection.mutable.ListBuffer

case class CardSearch(param0: Array[String], param134: Array[String], resultsLine: String)

object CardSearch {
  def parse(iterator: BufferedIterator[String], errorsAcc: LongAccumulator): Option[CardSearch] = {
    iterator.next() // пропускаем оглощаем CARD_SEARCH_START

    val param0 = ListBuffer.empty[String]
    val param134 = ListBuffer.empty[String]
    var resultsLine = ""
    var keepParsing = true

    while (iterator.hasNext && keepParsing) {
      val line = iterator.next()

      if (line.startsWith("CARD_SEARCH_END")) {
        if (iterator.hasNext) {
          resultsLine = iterator.next()
        } else {
          errorsAcc.add(1L)
        }
        keepParsing = false
      } else if (line.startsWith("$0 ")) {
        param0 += line.stripPrefix("$0 ").trim
      } else if (line.startsWith("$134 ")) {
        param134 += line.stripPrefix("$134 ").trim
      }
    }

    Some(CardSearch(param0.toArray, param134.toArray, resultsLine))
  }
}