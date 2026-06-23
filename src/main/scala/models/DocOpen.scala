package models

import org.apache.spark.util.LongAccumulator

case class DocOpen(isoDate: String, searchId: String, docId: String)

object DocOpen {
  /**
   * Парсит открытие документа.
   * Ожидает, что итератор находится на строке, начинающейся с "DOC_OPEN".
   */
  def parse(iterator: BufferedIterator[String], sessionIsoDate: String, errorsAcc: LongAccumulator): Option[DocOpen] = {
    val line = iterator.next() // Поглощаем DOC_OPEN
    val parts = line.split("\\s+")

    // Стандартный лог (DOC_OPEN + Дата + ID_поиска + ID_документа)
    if (parts.length >= 4) {
      val dateTime = parts(1)
      val searchId = parts(2)
      val docId = parts(3)

      val dateParts = dateTime.split("_")(0).split("\\.")
      if (dateParts.length == 3) {
        val isoDate = s"${dateParts(2)}-${dateParts(1)}-${dateParts(0)}"
        Some(DocOpen(isoDate, searchId, docId))
      } else {
        errorsAcc.add(1L)
        None
      }
    }
    // Лог с ошибкой (DOC_OPEN + ID_поиска + ID_документа)
    else if (parts.length == 3) {
      Some(DocOpen(sessionIsoDate, searchId = parts(1), docId = parts(2)))
    } else {
      errorsAcc.add(1L) // Неизвестный формат
      None
    }
  }
}