package SparkBasics1

import org.apache.spark.sql.SparkSession

object CreatingAvroFile {
def main(args: Array[String]): Unit = {
  val spark = new SparkSession
  .Builder()
    .appName("Json file formats")
    .master("local")
    .getOrCreate()
 // val avroDF = spark.read.format("com.databricks.spark.avro")
  //  .load("C:\\retail_db\\data-master\\retail_db_json\\products")
//avroDF.printSchema()
 // avroDF.write.format("com.databrics.spark.avro").save("outputpath")
}

}
