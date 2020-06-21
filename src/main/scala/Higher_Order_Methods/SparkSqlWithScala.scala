package Higher_Order_Methods
import org.apache.spark.sql.SparkSession
object SparkSqlWithScala {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("local").getOrCreate()
    /*
    val stationRDD = spark.sparkContext.textFile("/SparkTest0331.data/.txt").map { line =>
      val id = line.substring(0,11)
      val lat = line.substring(12,20).toDouble
      val lon = line.substring(21,30).toDouble
      val name = line.substring(41,71)
      Row(id,lat,lon,name)
       }
    val stations= spark.createDataFrame(stationRDD,sschema).cache()
    stations.createOrReplaceTempView("SparkTest0331.data")
  //  val purseSQL = spark.sql("SELECT SID,DATE FROM DATA WHERE MTYPE= "TMAX"" )
  */
  }
}
