package SparkBasics1

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{rank, round, sum}

object GetTopNDailyProductsDF {
 def main(args: Array[String]): Unit = {
   val spark = SparkSession.builder().master(master="local[*]").appName(name = "Get Daily").getOrCreate()
   spark.sparkContext.setLogLevel("Error")
   spark.conf.set("spark.sql.shuffle.partitions" ,"2")
   val orders = spark.read.json("C:\\retail_db\\data-master\\retail_db_json\\orders")
   val orderItems = spark.read.json("C:\\retail_db\\data-master\\retail_db_json\\order_items")
   //Complete or Closed orders
    val ordersFiltered = orders.where("order_status in ('COMPLETE', 'CLOSED')")
   //Join with order_items
   import spark.implicits._
   val ordersJoin = ordersFiltered.join(orderItems, $"order_id" === $"order_item_product_id")

   //Aggregate Using order_date and order_product_id
   val dailyProductRevenue = ordersJoin.groupBy("order_date","order_item_product_id").agg(round(sum("order_item_subtotal"),2).alias("daily_product_revenue"))
   val spec = Window.partitionBy("order_date").orderBy($"daily_product_revenue" desc)

   val topN = 3
   val topNDailyProducts = dailyProductRevenue.withColumn("rnk" , rank() over spec).where("rnk <= " + topN).orderBy("order_date","rnk").drop("rnk")
   topNDailyProducts.write.json("C:\\rnk\\daily_products_")
 }
}
