package ScalaBasics
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf,SparkContext}
object FileToHive {

  def main(args: Array[String]): Unit = {

    val spark  = new SparkSession
        .Builder()
      .master("yarn")
      .appName("clouder")
      .config("spark","test")
      .enableHiveSupport()
      .getOrCreate()

    val stocksDF = spark.read.option("header","true")
      .option("inferSchema","true")
      .csv("C:\\Users\\psadmin\\IdeaProjects\\untitled1\\resources\\DataSet\\comma\\companylist_noheader.csv")
    stocksDF.write.saveAsTable("spark_course.nse_stocks")
  }

}
