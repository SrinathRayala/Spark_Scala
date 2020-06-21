package ScalaBasics

import org.apache.spark.sql.SparkSession

object DFvsDS extends App {

  val spark = SparkSession.builder()
    .appName("DataSet Basics")
    .master("local")
    .getOrCreate()

  var startTime: Long = 0
  var endTime: Long = 0

  startTime = System.currentTimeMillis()

  val creditsDF = spark.read
    .option("inferSchema", true)
    .csv("C:\\Users\\psadmin\\IdeaProjects\\untitled1\\resources\\DataSet\\comma\\companylist_noheader.csv")

  println(creditsDF.count())

  endTime = System.currentTimeMillis()

  println("Time " + (endTime - startTime) / 1000)
}
