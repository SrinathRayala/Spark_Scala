package SparkBasics1

import org.apache.spark.sql.SparkSession

object CreatingDifferentFileFormats {
    def main(args: Array[String]): Unit = {
      val spark = new SparkSession
      .Builder()
        .appName("Json file formats")
        .master("local")
        .getOrCreate()

      val jasonDF = spark.read.json("C:\\retail_db\\data-master\\retail_db_json\\products")
      jasonDF.printSchema()
      jasonDF.show()
      println("Count " + jasonDF.count())

      val orcDF = spark.read.orc("C:\\retail_db\\data-master\\retail_db_json\\products")
      orcDF.printSchema()
      orcDF.show()
      println("Count "+ orcDF.count())

      val parqueDF = spark.read.parquet("")
      parqueDF.printSchema()
      parqueDF.show()

      //Orc and Parque schema mostly same.
      //Json file is different
      //Avro files not support spark 2.X you need to download dependence.
    }
  }
