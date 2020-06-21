package SparkBasics1

import org.apache.spark.sql.SparkSession

object DataFrameDemo_ {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.
      builder().
      appName("Data Frame Demo").
      master("local").
      getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val ordersDF = spark.read.json("C:\\retail_db\\data-master\\retail_db_json\\orders")
    // ordersDF.createTempview("orders")
   // spark.sql("select * from orders").show
  }
}
