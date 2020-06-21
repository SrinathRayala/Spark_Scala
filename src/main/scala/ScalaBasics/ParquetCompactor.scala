package ScalaBasics

import ScalaBasics.ParquetCompactor.Purchase
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession

object ParquetCompactor {

  case class Purchase(customer_id:Int,purchase_id:Int,date: String,time:String,tz:String,amount:Double)
  def main(args: Array[String]): Unit = {

    val spark = new sql.SparkSession
    .Builder()
      .master("local")
      .appName("String")
      .getOrCreate()

    spark.sparkContext.setLogLevel("Error")
    import spark.implicits._

    val purchaseDF = List(
      Purchase(121,234,"2017-92-12","20:20","UTC",500.99),
      Purchase(121,234,"2017-92-12","20:20","UTC",500.99),
      Purchase(121,234,"2017-92-12","20:20","UTC",500.99),
      Purchase(121,234,"2017-92-12","20:20","UTC",500.99)).toDF()
    purchaseDF.write.parquet("C:\\Spark_Problems\\Spark-master\\Spark-2.1\\output\\parquet_Compactor")

    val df = spark.read.parquet("C:\\Spark_Problems\\Spark-master\\Spark-2.1\\output\\parquet_Compactor")
    df.show()
    print("Count before dropping:" + df.count())

    val dropDF = df.dropDuplicates("customer_id")
    println("Count after dropping :" + dropDF.count())
dropDF.show()
  }

}
