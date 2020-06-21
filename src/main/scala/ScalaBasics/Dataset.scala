package ScalaBasics
import org.apache.spark.sql.SparkSession

case class Ratings(DDD : String)
object Dataset extends App {

  val spark =  SparkSession
    .builder()
    .appName("Dataset Basics")
    .master("local")
    .getOrCreate()

  import spark.implicits._

  val tableDS = spark.read
    .option("header",true)
    //.option("inferSchema",false)
    .csv("C:\\Users\\psadmin\\IdeaProjects\\untitled1\\resources\\DataSet\\comma\\companylist_noheader.csv")
    .as[Ratings]
  tableDS.show()

}
