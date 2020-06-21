package SparkBasics1

import org.apache.spark.{SparkConf, SparkContext}

object GetRevenuePerOrder {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().
      setMaster("local[2]").
      setAppName("Get revenue per order")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val orderItems = sc.textFile("C:\\retail_db\\data-master\\retail_db\\order_items\\part-00000")
    val revenuePerOrder = orderItems.
      map(oi => (oi.split(",")(1).toInt, oi.split(",")(4).toFloat)).
      reduceByKey(_ + _).
      map(oi => oi._1 + "," + oi._2)
    println(revenuePerOrder.collect.foreach(println))
    println(revenuePerOrder.collect)
  }

}
