package models

import org.apache.spark.util.LongAccumulator

case class QuickSearch(id: String, query: String, results: Array[String])

object QuickSearch {
  /**
   * Парсит блок быстрого поиска.
   * Ожидает, что итератор находится на строке, начинающейся с "QS".
   */
  def parse(iterator: BufferedIterator[String], errorsAcc: LongAccumulator): Option[QuickSearch] = {
    val qsLine = iterator.next() // Поглощаем строку QS

    val startIdx = qsLine.indexOf("{")
    val endIdx = qsLine.lastIndexOf("}")
    val query = if (startIdx != -1 && endIdx != -1) qsLine.substring(startIdx + 1, endIdx) else ""

    // Результаты и ID находятся на следующей строке
    if (iterator.hasNext) {
      val resultLine = iterator.next()
      val parts = resultLine.split("\\s+")
      if (parts.nonEmpty) {
        Some(QuickSearch(id = parts.head, query = query, results = parts.tail))
      } else {
        errorsAcc.add(1L)
        None
      }
    } else {
      errorsAcc.add(1L)
      None
    }
  }
}