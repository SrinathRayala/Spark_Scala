package ScalaBasics

import org.apache.spark.sql
import org.apache.spark.sql.SparkSession

object FileCompression {
  case class DadaFrameSample(name: String,actor: String)
  def main(args: Array[String]): Unit = {
    val spark = new sql.SparkSession
    .Builder()
      .master("local")
      .appName("String")
      .getOrCreate()
    spark.sparkContext.setLogLevel("Error")


    val df = spark.createDataFrame(
      DadaFrameSample("Sri","GT") ::
      DadaFrameSample("Sree","BB") ::
      DadaFrameSample("sri","DD") :: Nil).toDF().cache()

    df.write.mode("overwrite")
      .format("parquet")
      .option("compression","none")
      .mode("overwrite").save("C:\\Spark_Problems\\Spark-master\\Spark-2.1\\output\\file_no_compression_parq")

    df.write.mode("overwrite")
      .format("parquet")
      .option("compression","gzip")
      .mode("overwrite")
      .save("C:\\Spark_Problems\\Spark-master\\Spark-2.1\\output\\file_with_gzip_parq")

    df.write.mode("overwrite")
      .format("parquet")
      .option("compression","snappy")
      .mode("overwrite")
      .save("C:\\Spark_Problems\\Spark-master\\Spark-2.1\\output\\file_with_snappy_parq")


  //  df.write.mode("overwrite")
    //  .format("orc")
   //   .option("compression","zlib")
   //   .mode("overwrite")
    //  .save("C:\\Spark_Problems\\Spark-master\\Spark-2.1\\output\\file_with_zlib_orc")

  }

}
