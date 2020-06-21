package SparkBasics1

import org.apache.spark.sql.SparkSession

object DFBasicOperations {
  def main(args: Array[String]): Unit = {
    val spark = new SparkSession.Builder().master("local")
  .appName("test").getOrCreate()

    val customDF = spark.read.json("C:\\retail_db\\data-master\\retail_db_json\\products")
    customDF.printSchema()
    val customerSchema = customDF.schema
    println(customerSchema)

    val colNames = customDF.columns //Column names
    println(colNames.mkString(","))

    val customerDescription = customDF.describe("product_category_id") //Field count min max and values will return
    customerDescription.show()

    val colAndTypes =customDF.dtypes // dType - will give column name and column type
    println("Column Names: ")
    colAndTypes.foreach(println)

    customDF.head(5).foreach(println)
customDF.show(5)

    //Select columns
    customDF.select("product_category_id","product_price")
      //.where("product_price == 50.0").show(10)
     // .filter(customDF("product_category_id") === "50.0").show(10)
      .groupBy("product_category_id").count().show(10)
  }

}
