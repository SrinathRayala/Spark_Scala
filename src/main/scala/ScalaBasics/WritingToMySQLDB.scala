package ScalaBasics
import java.util.Properties

import org.apache.spark.sql.SaveMode
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
object WritingToMySQLDB {

    def main(args: Array[String]): Unit = {
      val spark = new SparkSession.Builder().master("local")
        .appName("test").getOrCreate()

      val url = "jdbc:mysql://35.232.46.143:3306/retail_db"
      val prop = new Properties()
      val table = "orders"
      prop.put("user", "user")
      prop.put("password", "password")
      Class.forName("com.mysql.jdbc.Driver")



      val nseStocks = spark.read
        .option("header", "true")
        .option("inferSchema","true")
        .csv("C:\\Users\\psadmin\\IdeaProjects\\untitled1\\resources\\DataSet\\comma\\companylist_noheader.csv")

      println("==========================" + nseStocks.count())

      nseStocks.write.mode(SaveMode.Overwrite).jdbc(url,table,prop)
  }

}
