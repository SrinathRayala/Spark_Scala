package SparkBasics1

import org.apache.spark.sql.SparkSession

object RDDWithCSVFile {
  def main(args: Array[String]): Unit ={
    val spark = SparkSession
      .builder()
      .appName("Creating RDD with CSV files")
      .master("local")
      .getOrCreate()

   //val csvRDD = spark.sparkContext.csvFile("\\resources\\DataSet\\companylist_noheader.csv")
    val csvRDD = spark.sparkContext.textFile("C:\\retail_db\\data-master\\nyse\\companylist_noheader.csv")
    val csvcommaRDD = spark.sparkContext.textFile("C:\\Users\\psadmin\\IdeaProjects\\untitled1\\resources\\DataSet\\comma\\companylist_noheader.csv")
    val header = csvRDD.first()
    val csvRDDWithoutHeader = csvRDD.filter(line => line != header)
      //  csvRDD.take(5).foreach(println)
      //  csvRDDWithoutHeader.take(5).foreach(println)

    val csvcomma = csvcommaRDD.
    map(oi => (oi.split(",")(0),oi.split(",")(1),oi.split(",")(2)))
    // csvcomma.take(5).foreach(println)

    val CSVcomma1 = csvcommaRDD.map(line => {
      val colArray = line.split(",")
      //List(colArray(0),colArray(1),colArray(2))
      Array(colArray(0),colArray(1),colArray(2)).mkString(":")
    }).take(4).foreach(println)

    //println(CSVFileData.collect)
  }

}
