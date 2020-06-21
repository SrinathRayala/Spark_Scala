package ScalaBasics

import org.apache.spark.sql.SparkSession

object DataSetOperatons extends App {
val spark = SparkSession.builder().appName("DataSet Basics").master("local").getOrCreate()
  import spark.implicits._

  val ratingsDS = spark.read
    .option("header",true)
    .option("inferSchema",true)
    .csv("C:\\Users\\psadmin\\IdeaProjects\\untitled1\\resources\\DataSet\\comma\\companylist_noheader.csv")
    .as[Ratings]

  ratingsDS.show()

  val filterDemo = ratingsDS.filter(ratingobj => ratingobj.DDD == "MMM")

  filterDemo.show()
  println("dd" + filterDemo.count())

  val whereDemo = ratingsDS.where(ratingsDS("DDD") === "WBAI")

  whereDemo.show()

  case class Ratings1(DDD: String)

  val selectedcolumns = ratingsDS.select(("DDD")).as[Ratings1]

  selectedcolumns.show()
}
