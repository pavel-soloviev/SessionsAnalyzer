name := "Consultant Analitics"

version := "0.1"

scalaVersion := "2.12.18"

val sparkVersion = "3.5.0" // Актуальная версия Spark

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql"  % sparkVersion,
  "com.globalmentor" % "hadoop-bare-naked-local-fs" % "0.1.0"
)