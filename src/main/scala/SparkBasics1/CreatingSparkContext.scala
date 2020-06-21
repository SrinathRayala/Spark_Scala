package SparkBasics1

import org.apache.spark.{SparkConf, SparkContext}

object CreatingSparkContext {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().
      setMaster("local[2]").
      setAppName("First Spark Application")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val array = Array(1,2,3,4,5)
    val arrayRDD = sc.parallelize(array,2)
    println("Num RDD: ",arrayRDD.count())
    arrayRDD.foreach(println)
    //Read the File

    val fileRDD = sc.textFile("C:\\retail_db\\data-master\\retail_db\\order_items\\part-00000",5)

    println("Num RDD in File",fileRDD.count())
   //fileRDD.foreach(println)


  }

}
