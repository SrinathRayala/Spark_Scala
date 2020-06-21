package SparkBasics1

import org.apache.spark.sql.SparkSession

object CreateTempTableFromDF {
  def main(args: Array[String]): Unit = {

    val spark  = new SparkSession.Builder().appName("DF").master("local").getOrCreate()

    val CustomerDF = spark.read.json("C:\\retail_db\\data-master\\retail_db_json\\products")

    CustomerDF.registerTempTable("Customer_table")
    val sqlQuery = "select product_image,product_name from Customer_table where product_category_id ='2'"
 // val talbeDF = spark.sql("select * from  Customer_table limit 10")
    val tableDFF =  spark.sql(sqlQuery)
    tableDFF.show(5)

  }

}
