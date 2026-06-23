package models

import org.apache.spark.util.LongAccumulator

import scala.collection.mutable.ListBuffer

case class UserSession(
                        date: String,
                        quickSearches: Array[QuickSearch],
                        cardSearches: Array[CardSearch],
                        docOpens: Array[DocOpen]
                      )

// Вспомогательный класс для накопления состояния в процессе парсинга
class SessionBuilder {
  var date: String = "UNKNOWN_DATE"
  val qsList: ListBuffer[QuickSearch] = ListBuffer.empty
  val cardList: ListBuffer[CardSearch] = ListBuffer.empty
  val opensList: ListBuffer[DocOpen] = ListBuffer.empty

  def build(): UserSession = UserSession(date, qsList.toArray, cardList.toArray, opensList.toArray)
}

object UserSession {
  /**
   * Принимает сырой текст сессии (разделенный по SESSION_START) и возвращает готовый объект.
   */
  def parse(sessionText: String, errorsAcc: LongAccumulator): UserSession = {
    // Создаем буферизированный итератор
    val iterator = sessionText.split("\n").map(_.trim).filter(_.nonEmpty).iterator.buffered
    val builder = new SessionBuilder()

    // Пытаемся вытащить дату сессии из первой строки
    if (iterator.hasNext) {
      val firstLine = iterator.head
      val startDateRaw = firstLine.split("_")(0)
      val startDateParts = startDateRaw.split("\\.")
      if (startDateParts.length == 3) {
        builder.date = s"${startDateParts(2)}-${startDateParts(1)}-${startDateParts(0)}"
      }
    }

    // Маршрутизация по типам событий
    while (iterator.hasNext) {
      val line = iterator.head // Только "подсматриваем" строку, не извлекая ее

      if (line.startsWith("QS")) {
        QuickSearch.parse(iterator, errorsAcc).foreach(builder.qsList.+=)
      } else if (line.startsWith("CARD_SEARCH_START")) {
        CardSearch.parse(iterator, errorsAcc).foreach(builder.cardList.+=)
      } else if (line.startsWith("DOC_OPEN")) {
        DocOpen.parse(iterator, builder.date, errorsAcc).foreach(builder.opensList.+=)
      } else {
        // Если строка не подошла ни под один шаблон (например, дата в начале), просто пропускаем
        iterator.next()
      }
    }

    builder.build()
  }
}