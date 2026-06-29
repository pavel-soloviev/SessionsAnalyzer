package models

import utils.ParseErrors
import java.time.LocalDateTime
import java.time.format.{DateTimeFormatter, DateTimeParseException}
import java.util.Locale
import scala.collection.mutable

case class CardSearch(
                       timestamp: Option[LocalDateTime],
                       id: String,
                       params: Map[String, Array[String]],
                       results: Array[String],
                       docOpens: Array[DocOpen] = Array.empty
                     )

object CardSearch {
  private val fmtEnglish = DateTimeFormatter.ofPattern("EEE,_d_MMM_yyyy_HH:mm:ss_Z", Locale.ENGLISH)
  private val fmtStandard = DateTimeFormatter.ofPattern("d.MM.yyyy_HH:mm:ss")

  def parse(iterator: BufferedIterator[String], errorsAcc: ParseErrors): Option[CardSearch] = {
    val startLine = iterator.next()
    val startParts = startLine.split("\\s+")

    val timestampOpt = if (startParts.length > 1) {
      val dateStr = startParts(1)
      try {
        Some(LocalDateTime.parse(dateStr, fmtEnglish))
      } catch {
        case _: DateTimeParseException =>
          try {
            Some(LocalDateTime.parse(dateStr, fmtStandard))
          } catch {
            case _: DateTimeParseException =>
              errorsAcc.cardDateParseError.add(1L)
              None
          }
      }
    } else {
      None
    }

    val paramsMap = mutable.Map.empty[String, mutable.ListBuffer[String]]
    var searchId = "UNKNOWN_CARD_ID"
    var results = Array.empty[String]
    var keepParsing = true // оставил в пользу читаемости

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
          errorsAcc.cardUnexpectedEOF.add(1L)
        }
        keepParsing = false
      } else if (line.startsWith("$")) {
        val parts = line.split("\\s+", 2)
        if (parts.length == 2) {
          val paramName = parts(0)
          val paramValue = parts(1)
          paramsMap.getOrElseUpdate(paramName, mutable.ListBuffer.empty) += paramValue
        }
      }
    }

    val finalParams = paramsMap.map { case (k, v) => (k, v.toArray) }.toMap

    Some(CardSearch(timestamp = timestampOpt, id = searchId, params = finalParams, results = results))
  }
}