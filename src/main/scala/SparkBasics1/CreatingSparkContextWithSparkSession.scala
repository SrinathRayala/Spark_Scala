package SparkBasics1

import org.apache.spark.sql.SparkSession

object CreatingSparkContextWithSparkSession {
  def main(args: Array[String]): Unit = {
    val sparkSession =SparkSession.builder()
      .appName("CreateSparkContext with Spark Session")
      .master("local")
      .getOrCreate()

    val array = Array(1,2,3,4,5)
    val arrayRDD = sparkSession.sparkContext.parallelize(array,2)
    arrayRDD.foreach(println)
    val fileRDD = sparkSession.sparkContext.textFile("C:\\retail_db\\data-master\\retail_db\\order_items\\part-00000",5)
    println("Num RDD count: ", fileRDD.count())
    fileRDD.take(10).foreach(println)
  }

}
