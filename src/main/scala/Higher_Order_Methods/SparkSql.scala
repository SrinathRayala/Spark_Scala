package Higher_Order_Methods
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.types.StringType

object SparkSql {
 def  main(args: Array[String]) :Unit ={

   val spark  = SparkSession.builder().master("local").appName("Spark Sql")
  //   option("header",true)
   .getOrCreate()
   import spark.implicits._

   spark.sparkContext.setLogLevel("Error")

val tschema = StructType(Array(
   StructField("Day",StringType),
  StructField("JD",StringType),
  StructField("State_id",StringType)
))

   // val SparkTest0331.data = spark.read.schema(tschema).csv("C:\\Spark_Problems\\BigDataAnalyticswithSpark-master\\MN212142_9392.csv")
   val data = spark.read.schema(tschema).option("dateFormat","yyyyMMdd").csv("C:\\Spark_Problems\\BigDataAnalyticswithSpark-master\\MN212142_9392.csv")

   //val JD340 = SparkTest0331.data.filter($"JD" ==="340")
   val JD340 = data.filter('JD ==="340").limit(1000).drop("Day").withColumnRenamed("State_id","StateID")
   val JD339 = data.filter($"JD" === "339").limit(1000).drop("Day").withColumnRenamed("State_id","StateID")

   // val combinedTemps = JD340.join(JD339,JD340("JD") === JD339("JD"))
   val combinedTemps = JD340.join(JD339,Seq("JD"))
   val averageTemp = combinedTemps.select('JD,'StateID)

   data.show()
   data.printSchema()
   data.show()
   JD340.show()
  // averageTemp.show()
   spark.stop()

    }


}
