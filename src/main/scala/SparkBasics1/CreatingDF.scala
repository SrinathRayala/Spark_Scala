package SparkBasics1

import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object CreatingDF {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().
      setMaster("local[2]").
      setAppName("First Spark Application")
    val sc = new SparkContext(conf)
    val SQLContext = new SQLContext(sc)
   val rdd = sc.parallelize(Array(1,2,3,4,5))
    val schema = StructType (
      StructField("Number", IntegerType, false) :: Nil
    )
    val rowRDD = rdd.map(line => Row(line))
    val df = SQLContext.createDataFrame(rowRDD,schema)
df.printSchema()
    df.show()
  }

}
