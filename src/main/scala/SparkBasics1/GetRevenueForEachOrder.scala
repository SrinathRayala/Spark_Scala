package SparkBasics1

import scala.io.Source

object GetRevenueForEachOrder {
  def main(args: Array[String]): Unit={
    val inputPath = "C:\\retail_db\\data-master\\retail_db\\order_items\\part-00000"
    val orderItems: Seq[String]=Source.
      fromFile(inputPath).getLines.toList
    val revenueForEachOrderId = orderItems.groupBy(k => k.split(",")(1).toInt)
    revenueForEachOrderId.take(10).foreach(println)
  }
}
