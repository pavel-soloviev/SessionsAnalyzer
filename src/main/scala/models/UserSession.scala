package models

import org.apache.spark.util.LongAccumulator
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.collection.mutable.ListBuffer
import scala.util.Try

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
    // группируем все открытия документов по ID поиска
    val opensBySearchId = opensList.groupBy(_.searchId)

    // раскладываем документы по быстрым поискам
    val linkedQs = qsList.map { qs =>
      val docsForThisSearch = opensBySearchId.getOrElse(qs.id, ListBuffer.empty).toArray
      qs.copy(openedDocs = docsForThisSearch)
    }.toArray

    // раскладываем документы по карточкам поиска
    val linkedCs = cardList.map { cs =>
      val docsForThisSearch = opensBySearchId.getOrElse(cs.id, ListBuffer.empty).toArray
      cs.copy(openedDocs = docsForThisSearch)
    }.toArray

    UserSession(startTime, linkedQs, linkedCs, opensList.toArray)
  }
}

object UserSession {
  private val formatter = DateTimeFormatter.ofPattern("dd.MM.yyyy_HH:mm:ss")

  def parse(sessionText: String, errorsAcc: LongAccumulator): UserSession = {
    val iterator = sessionText.split("\n").map(_.trim).filter(_.nonEmpty).iterator.buffered
    val builder = new SessionBuilder()

    if (iterator.hasNext) {
      val firstLine = iterator.head
      builder.startTime = Try(LocalDateTime.parse(firstLine, formatter)).getOrElse(LocalDateTime.MIN)
    }

    while (iterator.hasNext) {
      val line = iterator.head

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