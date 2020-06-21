package SparkBasics1

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object Spark_Typed_Sql {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("test").getOrCreate()
 spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

  //  val ladata = spark.read.schema(Encoders.product[data].schema).option("header", true).option("delimiter","\t").csv("C:\\Spark_Problems\\la.data.64.County_1.CSV").select(trim('series_id) as "series_id",'year,'period,'value).as[data]

    val tschema =StructType(Array(
      StructField("id",StringType),
      StructField("year",IntegerType),
      StructField("period",StringType),
      StructField("value",DoubleType)
    ))
    val ladata1 = spark.read.schema(tschema).csv("C:\\Spark_Problems\\la.data.64.County_1.CSV").toDF()
    val seris_Data = spark.read.textFile("C:\\Spark_Problems\\la.series.TXT").map{ line =>
      val p = line.split("\t").map(_.trim)
      series(p(0),p(2),p(3),p(6))
    }.cache()
    val joined1 = ladata1.joinWith(seris_Data,'series_id === 'id)

      ladata1.show()
    seris_Data.show()
    joined1.show()
  }
}
