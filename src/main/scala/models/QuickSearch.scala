package models

import utils.ParseErrors
import java.time.LocalDateTime
import java.time.format.{DateTimeFormatter, DateTimeParseException}
import java.util.Locale

case class QuickSearch(
                        timestamp: Option[LocalDateTime],
                        id: String,
                        query: String,
                        results: Array[String],
                        docOpens: Array[DocOpen] = Array.empty
                      )

object QuickSearch {
  private val fmtEnglish = DateTimeFormatter.ofPattern("EEE,_d_MMM_yyyy_HH:mm:ss_Z", Locale.ENGLISH)
  private val fmtStandard = DateTimeFormatter.ofPattern("d.MM.yyyy_HH:mm:ss")

  def parse(iterator: BufferedIterator[String], errorsAcc: ParseErrors): Option[QuickSearch] = {
    val qsLine = iterator.next()

    val startIdx = qsLine.indexOf("{")
    val endIdx = qsLine.lastIndexOf("}")

    if (startIdx == -1 || endIdx == -1) {
      errorsAcc.qsUnexpectedEOF.add(1L)
      return None
    }

    val dateStr = qsLine.substring(2, startIdx).trim

    val timestampOpt = if (dateStr.nonEmpty) {
      try {
        Some(LocalDateTime.parse(dateStr, fmtEnglish))
      } catch {
        case _: DateTimeParseException =>
          try {
            Some(LocalDateTime.parse(dateStr, fmtStandard))
          } catch {
            case _: DateTimeParseException =>
              errorsAcc.qsDateParseError.add(1L)
              None
          }
      }
    } else {
      None
    }

    val query = qsLine.substring(startIdx + 1, endIdx)

    if (iterator.hasNext) {
      val nextLine = iterator.head
      // если следующая строка это системное событие, значит результатов у поиска нет
      if (nextLine.startsWith("QS") || nextLine.startsWith("CARD") || nextLine.startsWith("DOC") || nextLine.startsWith("SESSION")) {

        errorsAcc.qsMissingResults.add(1L)
        Some(QuickSearch(timestampOpt, id = "NO_ID", query = query, results = Array.empty))

      } else {
        val resultLine = iterator.next()
        val parts = resultLine.split("\\s+")
        Some(QuickSearch(timestampOpt, id = parts.head, query = query, results = parts.tail))
      }
    } else {
      errorsAcc.qsMissingResults.add(1L)
      Some(QuickSearch(timestampOpt, id = "NO_ID", query = query, results = Array.empty))
    }
  }
}