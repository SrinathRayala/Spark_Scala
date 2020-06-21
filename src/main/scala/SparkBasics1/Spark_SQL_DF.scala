package SparkBasics1

import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

object Spark_SQL_DF {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("Sprk Sql").getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

    val tschema = StructType(Array(
      StructField("sid",StringType),
      StructField("date",DateType),
      StructField("mtype",StringType),
      StructField("value",DoubleType)
    ))

    val data2019 = spark.read.schema(tschema).option("dateFormat","yyyyMMdd").csv("C:\\Spark_Problems\\2019_1.CSV").toDF()

    val sschema = StructType(Array(
      StructField("sid", StringType),
      StructField("lat",DoubleType),
      StructField("lon",DoubleType),
      StructField("name",StringType)
    ))
    val stationRDD = spark.sparkContext.textFile("C:\\Spark_Problems\\ghcnd-stations.TXT").map { line =>
      val id = line.substring(0,11)
      val lat = line.substring(12,20).toDouble
      val lon = line.substring(21,30).toDouble
      val name = line.substring(41,71)
      Row(id,lat,lon,name)
    }

    val stations = spark.createDataFrame(stationRDD,sschema).cache()

    data2019.createOrReplaceTempView("data2019")
    val pureSQL = spark.sql("""
        SELECT sid,date,value as tmax FROM data2019
      """)
   // data2019.show()
   // data2019.schema.printTreeString()

    val tmax2019 = data2019.filter($"mtype" === "TMAX").limit(100).drop("mtype").withColumnRenamed("value","tmax")
    tmax2019.describe().show()
    val tmin2019 = data2019.filter('mtype === "TMIN").limit(100).drop("mtype").withColumnRenamed("value","tmin")



    //val join2019 =  tmax2019.join(tmin2019,tmax2019("sid") === tmin2019("sid") && tmax2019("date") === tmin2019("date"))
    val join2019_1 =  tmax2019.join(tmin2019,Seq("sid","date"))

    val averageTemp2019 = join2019_1.select('sid,'date,('tmax + 'tmin)/20*1.8+32) .withColumnRenamed("((((tmax + tmin) / 20) * 1.8) + 32)","tave")

    val stationTemp2019_1 = averageTemp2019.groupBy('sid).agg(avg('tave))
    val joinedData2019 = stationTemp2019_1.join(stations,Seq("sid"))

    val localData = joinedData2019.collect()
    //  tmax2019.show()
    tmin2019.show()
    join2019_1.show()
    averageTemp2019.show()
    stations.show()
    stationTemp2019_1.show()
    joinedData2019.show()
    pureSQL.show()
    spark.stop()

  }

}
