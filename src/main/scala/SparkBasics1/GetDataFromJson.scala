package SparkBasics1

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.sum

object GetDataFromJson {
  def main(args: Array[String]): Unit = {
    /* val conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]") */
   /* val sc = new SparkContext(conf) */
    /* val props = ConfigFactory.load()
    val env = args(0) */

    val spark = SparkSession.builder().master("local[*]").appName("Get Daily").getOrCreate()
    spark.sparkContext.setLogLevel("Error")
    spark.conf.set("Spark.sql.shuffle.partitions","2")
    /* val sc = new org.apache.spark.sql.SQLContext(sc)*/
    val ordersDF = spark.read.json("C:\\retail_db\\data-master\\retail_db_json\\orders")
    val orderItemsDF = spark.read.json("C:\\retail_db\\data-master\\retail_db_json\\order_items")
    val ordersFiltered = ordersDF.where("order_status in ('COMPLETE', 'CLOSED')")
    import spark.implicits._
    val ordersJoin = ordersFiltered.join(orderItemsDF,$"order_id" === $"order_item_order_id")
    val dailyProductRevenue = ordersJoin.groupBy("order_date", "order_item_product_id").agg(sum("order_item_subtotal").alias("daily_product_revenue"))
    dailyProductRevenue.orderBy($"order_date" , $"daily_product_revenue".desc).write.json("C:\\output\\daily_products")
    /*ordersDF.printSchema()
    println(ordersDF.show) */
    println(dailyProductRevenue.show)
  }

}
