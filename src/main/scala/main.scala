import models.UserSession
import analytics.{AccSearchAnalyzer, DocOpensAnalyzer}
import utils.{SparkSetup, ParseErrors}

object main {
  def main(args: Array[String]): Unit = {
    // инициализация Spark и менеджера ошибок
    val spark = SparkSetup.createSession()
    val errorsAcc = new ParseErrors(spark.sparkContext)

    // чтение и парсинг логов
    val logFilePath = "src/main/resources/sessions/*"
    val rawSessionsRDD = SparkSetup.readRawSessions(spark, logFilePath)

    val sessionsRDD = rawSessionsRDD.map { text =>
      UserSession.parse(text, errorsAcc)
    }.cache()

    // запускаем вычисления
    val totalSessions = sessionsRDD.count()

    // выводим отчет об ошибках
    errorsAcc.printReport()
    println(s"Всего обработано сессий: $totalSessions")

    // запуск аналитики независимыми вызовами
    AccSearchAnalyzer.run(sessionsRDD)
    DocOpensAnalyzer.run(sessionsRDD)

    spark.stop()
  }
}