package models

import org.apache.spark.util.LongAccumulator
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.util.Try

case class DocOpen(timestamp: LocalDateTime, searchId: String, docId: String)

object DocOpen {
  private val formatter = DateTimeFormatter.ofPattern("dd.MM.yyyy_HH:mm:ss")

  def parse(iterator: BufferedIterator[String], sessionStartTime: LocalDateTime, errorsAcc: LongAccumulator): Option[DocOpen] = {
    val line = iterator.next() // пропускаем DOC_OPEN
    val parts = line.split("\\s+")

    // стандартный лог (DOC_OPEN + Дата_Время + ID_поиска + ID_документа)
    if (parts.length >= 4) {
      val dateTimeStr = parts(1) // например: "08.02.2020_07:46:04"
      val searchId = parts(2)
      val docId = parts(3)

      Try(LocalDateTime.parse(dateTimeStr, formatter)).toOption match {
        case Some(parsedTimestamp) =>
          Some(DocOpen(parsedTimestamp, searchId, docId))
        case None =>
          errorsAcc.add(1L) // ошибка формата даты
          None
      }
    }
    // лог с ошибкой (DOC_OPEN + ID_поиска + ID_документа)
    else if (parts.length == 3) {
      // подставляем время начала сессии вместо отсутствующего времени открытия
      Some(DocOpen(sessionStartTime, searchId = parts(1), docId = parts(2)))
    } else {
      errorsAcc.add(1L) // неизвестный формат
      None
    }
  }
}