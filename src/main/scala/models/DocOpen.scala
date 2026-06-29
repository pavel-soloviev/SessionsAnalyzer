package models

import utils.ParseErrors
import java.time.LocalDateTime
import java.time.format.{DateTimeFormatter, DateTimeParseException}

case class DocOpen(timestamp: LocalDateTime, searchId: String, docId: String)

object DocOpen {
  private val formatter = DateTimeFormatter.ofPattern("dd.MM.yyyy_HH:mm:ss")

  def parse(iterator: BufferedIterator[String], sessionStartTime: LocalDateTime, errorsAcc: ParseErrors): Option[DocOpen] = {
    val line = iterator.next()
    val parts = line.split("\\s+")

    // стандартный лог (DOC_OPEN + Дата_Время + ID_поиска + ID_документа)
    if (parts.length >= 4) {
      val dateTimeStr = parts(1)
      val searchId = parts(2)
      val docId = parts(3)

      try {
        val parsedTimestamp = LocalDateTime.parse(dateTimeStr, formatter)
        Some(DocOpen(parsedTimestamp, searchId, docId))
      } catch {
        case _: DateTimeParseException =>
          errorsAcc.docDateParseError.add(1L)
          None
      }
    }
    // лог с ошибкой (DOC_OPEN + ID_поиска + ID_документа)
    else if (parts.length == 3) {
      Some(DocOpen(sessionStartTime, searchId = parts(1), docId = parts(2)))
    } else {
      errorsAcc.docUnknownFormat.add(1L)
      None
    }
  }
}