package SparkBasics1

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object CreatingDFWithSparkSession {
  def main(args: Array[String]): Unit={
    val spark = SparkSession.builder().appName("").master("local").getOrCreate()
  val rdd = spark.sparkContext.parallelize((Array("1","2","3","4")))
    val schema = StructType(
      StructField("Integers as String",StringType,true) :: Nil
    )
    val rowRDD = rdd.map(element => Row(element))
    val df = spark.createDataFrame(rowRDD,schema)
    df.printSchema()
    df.show(3)
  }

}
