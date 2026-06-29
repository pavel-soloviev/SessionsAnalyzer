import models.UserSession
import analytics.{AccSearchAnalyzer, DocOpensAnalyzer}
import utils.{SparkSetup, ParseErrors}

object main {
  def main(args: Array[String]): Unit = {
    // 1. Инициализация Spark и менеджера ошибок
    val spark = SparkSetup.createSession()
    val errorsAcc = new ParseErrors(spark.sparkContext)

    // 2. Чтение и парсинг логов
    val logFilePath = "src/main/resources/sessions/*"
    val rawSessionsRDD = SparkSetup.readRawSessions(spark, logFilePath)

    val sessionsRDD = rawSessionsRDD.map { text =>
      UserSession.parse(text, errorsAcc)
    }.cache()

    // Форсируем вычисление RDD
    val totalSessions = sessionsRDD.count()

    // Выводим детальный отчет об ошибках
    errorsAcc.printReport()
    println(s"Всего обработано сессий: $totalSessions")

    // 3. Запуск аналитики независимыми вызовами
    AccSearchAnalyzer.run(sessionsRDD)
    DocOpensAnalyzer.run(sessionsRDD)

    spark.stop()
  }
}