package SparkBasics1

import org.apache.spark.sql.SparkSession

object DFWithCSV {
  def main(args: Array[String]): Unit = {
    val spark = new SparkSession.Builder().master("local").getOrCreate()
    val df= spark.read.option("header",true)
      .option("inferSchema",true)
      .csv("C:\\Users\\psadmin\\IdeaProjects\\untitled1\\resources\\DataSet\\comma\\companylist_noheader.csv")
    df.printSchema()
    df.show()

  }

}
