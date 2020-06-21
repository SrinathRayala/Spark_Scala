package SparkBasics1

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{rank, sum}

object GetDailyProductRevenueDF {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.
      builder().
      master("local").
      appName("Get Daily ProductRevenue").
      getOrCreate()
    spark.sparkContext.setLogLevel("Error")
    spark.conf.set("Spark.sql.shuffle.partitions", "2")

    import spark.implicits._

    val orders =spark.read.json("C:\\retail_db\\data-master\\retail_db_json\\orders")
    val orderItems = spark.read.json("C:\\retail_db\\data-master\\retail_db_json\\order_items")

    val DailyProductRevenue = orders.
      where("order_status in ('COMPLETE','CLOSED')").join(orderItems,$"order_id" === $"order_item_order_id").
      groupBy("order_date","order_item_product_id").
      agg(sum("order_item_subtotal").alias("daily_product_revenue"))

    val rnk = rank().
      over(Window.partitionBy("order_date").
      orderBy($"daily_product_revenue" desc))
    val topNDailyProducts =DailyProductRevenue.withColumn("rnk",rnk)
    val topN = 5
    topNDailyProducts.where("rnk <= " + topN).
      orderBy($"order_date", $"rnk").show

     }
}
