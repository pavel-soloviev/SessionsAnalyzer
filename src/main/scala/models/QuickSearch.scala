package models

import org.apache.spark.util.LongAccumulator

case class QuickSearch(
                        id: String,
                        query: String,
                        results: Array[String],
                        openedDocs: Array[DocOpen] = Array.empty
                      )

object QuickSearch {
  def parse(iterator: BufferedIterator[String], errorsAcc: LongAccumulator): Option[QuickSearch] = {
    val qsLine = iterator.next() // пропускаем строку с QS

    val startIdx = qsLine.indexOf("{")
    val endIdx = qsLine.lastIndexOf("}")
    val query = if (startIdx != -1 && endIdx != -1) qsLine.substring(startIdx + 1, endIdx) else ""

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