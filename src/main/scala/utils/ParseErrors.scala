package utils

import org.apache.spark.SparkContext
import org.apache.spark.util.LongAccumulator

class ParseErrors(sc: SparkContext) extends Serializable {
  val sessionDateParseError: LongAccumulator = sc.longAccumulator("Session_DateParseError")

  val qsDateParseError: LongAccumulator = sc.longAccumulator("QuickSearch_DateParseError")
  val qsMissingResults: LongAccumulator = sc.longAccumulator("QuickSearch_MissingResults")
  val qsUnexpectedEOF: LongAccumulator = sc.longAccumulator("QuickSearch_UnexpectedEOF")

  val cardDateParseError: LongAccumulator = sc.longAccumulator("CardSearch_DateParseError")
  val cardUnexpectedEOF: LongAccumulator = sc.longAccumulator("CardSearch_UnexpectedEOF")

  val docDateParseError: LongAccumulator = sc.longAccumulator("DocOpen_DateParseError")
  val docUnknownFormat: LongAccumulator = sc.longAccumulator("DocOpen_UnknownFormat")

  def printReport(): Unit = {
    println("\nОтчет об ошибках парсинга")
    println(s"Сессия (ошибка формата даты):          ${sessionDateParseError.value}")
    println(s"QuickSearch (ошибка формата даты):     ${qsDateParseError.value}")
    println(s"QuickSearch (нет результатов):         ${qsMissingResults.value}")
    println(s"QuickSearch (оборван в конце):         ${qsUnexpectedEOF.value}")
    println(s"CardSearch (ошибка формата даты):      ${cardDateParseError.value}")
    println(s"CardSearch (оборван в конце):          ${cardUnexpectedEOF.value}")
    println(s"DocOpen (ошибка формата даты):         ${docDateParseError.value}")
    println(s"DocOpen (неизвестный формат):          ${docUnknownFormat.value}")

    val total = sessionDateParseError.value + qsDateParseError.value + qsMissingResults.value +
      qsUnexpectedEOF.value + cardDateParseError.value + cardUnexpectedEOF.value +
      docDateParseError.value + docUnknownFormat.value
    println(s"---------------------------------")
    println(s"Итого ошибок: $total\n")
  }
}