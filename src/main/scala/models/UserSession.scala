package models

import utils.ParseErrors
import java.time.LocalDateTime
import java.time.format.{DateTimeFormatter, DateTimeParseException}
import scala.collection.mutable.ListBuffer

case class UserSession(
                        startTime: LocalDateTime,
                        quickSearches: Array[QuickSearch],
                        cardSearches: Array[CardSearch],
                        docOpens: Array[DocOpen]
                      )

class SessionBuilder {
  var startTime: LocalDateTime = LocalDateTime.MIN
  val qsList: ListBuffer[QuickSearch] = ListBuffer.empty
  val cardList: ListBuffer[CardSearch] = ListBuffer.empty
  val opensList: ListBuffer[DocOpen] = ListBuffer.empty

  def build(): UserSession = {
    val opensBySearchId = opensList.groupBy(_.searchId)

    val linkedQs = qsList.map { qs =>
      val docsForThisSearch = opensBySearchId.getOrElse(qs.id, ListBuffer.empty).toArray
      qs.copy(docOpens = docsForThisSearch)
    }.toArray

    val linkedCs = cardList.map { cs =>
      val docsForThisSearch = opensBySearchId.getOrElse(cs.id, ListBuffer.empty).toArray
      cs.copy(docOpens = docsForThisSearch)
    }.toArray

    UserSession(startTime, linkedQs, linkedCs, opensList.toArray)
  }
}

object UserSession {
  private val formatter = DateTimeFormatter.ofPattern("dd.MM.yyyy_HH:mm:ss")

  def parse(sessionText: String, errorsAcc: ParseErrors): UserSession = {
    val iterator = sessionText.split("\n").map(_.trim).filter(_.nonEmpty).iterator.buffered
    val builder = new SessionBuilder()

    if (iterator.hasNext) {
      val firstLine = iterator.head
      try {
        builder.startTime = LocalDateTime.parse(firstLine, formatter)
      } catch {
        case _: DateTimeParseException =>
          errorsAcc.sessionDateParseError.add(1L)
          builder.startTime = LocalDateTime.MIN
      }
    }

    while (iterator.hasNext) {
      val line = iterator.head

      /* оставил в таком формате, т.к. в моем понимании прописывать единый интерфейс
       для всех классов и вставлять им префиксы более трудозатратно и не быстрее
       */
      if (line.startsWith("QS")) {
        QuickSearch.parse(iterator, errorsAcc).foreach(builder.qsList.+=)
      } else if (line.startsWith("CARD_SEARCH_START")) {
        CardSearch.parse(iterator, errorsAcc).foreach(builder.cardList.+=)
      } else if (line.startsWith("DOC_OPEN")) {
        DocOpen.parse(iterator, builder.startTime, errorsAcc).foreach(builder.opensList.+=)
      } else {
        iterator.next()
      }
    }

    builder.build()
  }
}