package ScalaBasics

import org.apache.spark.{SparkConf, SparkContext, sql}
import org.apache.spark.sql.{SparkSession, types}
import org.apache.spark.sql.types.{BooleanType, IntegerType, StructField, StructType,StringType}

object ScalaFireDepartmentCalls {

  def main(args: Array[String]): Unit = {

    val spark = new sql.SparkSession
        .Builder()
      .master("local")
      .appName("String")
      .getOrCreate()
    spark.sparkContext.setLogLevel("Error")
    val fireSeviceCallsDF1 = spark.read.format("csv")
      .option("header", true)
     //.option("mode", DropMalformedMode)
      .option("inferSchema",true)
        .load("C:\\Spark_Problems\\Spark-master\\Spark-2.1\\input\\Fire_Department_Calls.csv")

    fireSeviceCallsDF1.printSchema()
   // fireSeviceCallsDF1.show(true)

    val fireSchema = StructType(Array(StructField("CallNumber", IntegerType, true),
      StructField("UnitID", StringType, true),
      StructField("IncidentNumber", IntegerType, true),
      StructField("CallType", StringType, true),
      StructField("CallDate", StringType, true),
      StructField("WatchDate", StringType, true),
      StructField("ReceivedDtTm", StringType, true),
      StructField("EntryDtTm", StringType, true),
      StructField("DispatchDtTm", StringType, true),
      StructField("ResponseDtTm", StringType, true),
      StructField("OnSceneDtTm", StringType, true),
      StructField("TransportDtTm", StringType, true),
      StructField("HospitalDtTm", StringType, true),
      StructField("CallFinalDisposition", StringType, true),
      StructField("AvailableDtTm", StringType, true),
      StructField("Address", StringType, true),
      StructField("City", StringType, true),
      StructField("ZipcodeofIncident", IntegerType, true),
      StructField("Battalion", StringType, true),
      StructField("StationArea", StringType, true),
      StructField("Box", StringType, true),
      StructField("OriginalPriority", StringType, true),
      StructField("Priority", StringType, true),
      StructField("FinalPriority", IntegerType, true),
      StructField("ALSUnit", BooleanType, true),
      StructField("CallTypeGroup", StringType, true),
      StructField("NumberofAlarms", IntegerType, true),
      StructField("UnitType", StringType, true),
      StructField("Unitsequenceincalldispatch", IntegerType, true),
      StructField("FirePreventionDistrict", StringType, true),
      StructField("SupervisorDistrict", StringType, true),
      StructField("NeighborhoodDistrict", StringType, true),
      StructField("Location", StringType, true),
      StructField("RowID", StringType, true)))

    val fireServiceCall2DF = spark.read.format("csv")
      .option("header",true)
      .schema(fireSchema)
      .csv("C:\\Spark_Problems\\Spark-master\\Spark-2.1\\input\\Fire_Department_Calls.csv")

    fireServiceCall2DF.printSchema()
    fireServiceCall2DF.show(true)

    import spark.implicits._

   val colNames = fireServiceCall2DF.columns //Column names
    println(colNames.mkString(","))

    fireServiceCall2DF.columns.foreach{x => println(x)}

    //fireServiceCall2DF.columns.foreach(println())

    fireServiceCall2DF.select("CallType").show(5)

    fireServiceCall2DF.select("CallType").distinct().show()

    spark.sparkContext.setLogLevel("Error")

    fireServiceCall2DF.select("CallType").groupBy("CallType").count().orderBy($"Count".desc).show()

    val from_pattern1 = "MM/dd/yyyy"
    val to_pattern1 = "yyyy-MM-dd"

    val from_pattern2 = "MM/dd/yyyy hh:mm:ss aa"
    val to_pattern2 = "MM/dd/yyyy hh:mm:ss aa"

    import org.apache.spark.sql.functions._
val fireServiceCallsTsDF = fireServiceCall2DF
  .withColumn("CallDateTS", unix_timestamp(fireServiceCall2DF("CallDate"), from_pattern1).cast("timestamp")).drop("CallDate")
  .withColumn("WatchDateTS", unix_timestamp(fireServiceCall2DF("WatchDate"), from_pattern1).cast("timestamp")).drop("WatchDate")
  .withColumn("ReceivedDtTmTS", unix_timestamp(fireServiceCall2DF("ReceivedDtTm"), from_pattern2).cast("timestamp")).drop("ReceivedDtTm")
  .withColumn("EntryDtTmTS", unix_timestamp(fireServiceCall2DF("EntryDtTm"), from_pattern2).cast("timestamp")).drop("EntryDtTm")
  .withColumn("DispatchDtTmTS", unix_timestamp(fireServiceCall2DF("DispatchDtTm"), from_pattern2).cast("timestamp")).drop("DispatchDtTm")
  .withColumn("ResponseDtTmTS", unix_timestamp(fireServiceCall2DF("ResponseDtTm"), from_pattern2).cast("timestamp")).drop("ResponseDtTm")
  .withColumn("OnSceneDtTmTS", unix_timestamp(fireServiceCall2DF("OnSceneDtTm"), from_pattern2).cast("timestamp")).drop("OnSceneDtTm")
  .withColumn("TransportDtTmTS", unix_timestamp(fireServiceCall2DF("TransportDtTm"), from_pattern2).cast("timestamp")).drop("TransportDtTm")
  .withColumn("HospitalDtTmTS", unix_timestamp(fireServiceCall2DF("HospitalDtTm"), from_pattern2).cast("timestamp")).drop("HospitalDtTm")
  .withColumn("AvailableDtTmTS", unix_timestamp(fireServiceCall2DF("AvailableDtTm"), from_pattern2).cast("timestamp")).drop("AvailableDtTm")
    fireSeviceCallsDF1.printSchema()
    fireServiceCallsTsDF.show()

    fireServiceCallsTsDF.select(year(fireServiceCallsTsDF("CallDateTS"))).distinct().orderBy($"year(CallDateTS)".desc).show


    fireServiceCallsTsDF.select((dayofyear(fireServiceCallsTsDF("CallDateTS"))),(year(fireServiceCallsTsDF("CallDateTS")))).show


    fireServiceCallsTsDF.filter(year(fireServiceCallsTsDF("CallDateTS")) === "2017").
      filter(dayofyear(fireServiceCallsTsDF("CallDateTS")) >= 150).select(dayofyear(fireServiceCallsTsDF("CallDateTS")))
      .distinct().orderBy($"dayofyear(CallDateTS)".desc).show()


    fireServiceCallsTsDF.filter(year(fireServiceCallsTsDF("CallDateTS")) === "2017").
      filter(dayofyear(fireServiceCallsTsDF("CallDateTS")) >= 100).groupBy(dayofyear(fireServiceCallsTsDF("CallDateTS")))
      .count().orderBy($"dayofyear(CallDateTS)".desc).show()


    fireServiceCallsTsDF.repartition(6).createOrReplaceTempView("fireServiceView")
    spark.catalog.cacheTable("fireServiceView")
    println(spark.table("fireServiceView").count())
    println(spark.catalog.isCached("fireServiceView"))

    import spark.sql

    sql("SELECT * FROM fireServiceView").show()

    sql("select NeighborhoodDistrict,count(NeighborhoodDistrict) as NeighborhoodDistrict_count from fireServiceView where year(CallDateTS) == '2017' group by NeighborhoodDistrict order by NeighborhoodDistrict_count desc").show

    val incidentsDF = spark.read.format("csv")
      .option("header",true)
      .csv("C:\\Spark_Problems\\Spark-master\\Spark-2.1\\input\\Fire_Incidents.csv")
      .withColumnRenamed("Incident Number","IncidentNumber").cache()
    incidentsDF.printSchema()
    incidentsDF.show()
    println(incidentsDF.count())

    val fireServiceDF = spark.table("fireServiceView")
    val joinedDF = fireServiceDF.join(incidentsDF,fireServiceDF.col("IncidentNumber") === incidentsDF.col("IncidentNumber"))

    joinedDF.filter(year(fireServiceDF("CallDateTS")) === "2000")
      .filter(fireServiceDF.col("NeighborhoodDistrict") === "Tenderloin")
      .groupBy("Primary Situation").count().orderBy(desc("count")).show()
  }

}
