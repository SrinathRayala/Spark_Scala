package SparkBasics1

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object FireDepartmentCalls {
  val spark = SparkSession.builder.appName("Fire-Department-calls").master("local[*]").getOrCreate()
  def main(args: Array[String]): Unit ={
    val fireServiceCallsDF1 = spark.read
      .format("csv")
      .option("header", "true")
      .option("mode", "DROPMALFORMED")
      .option("inferSchema", "true")
      .csv("C:\\Spark_Problems\\Spark-master\\Spark-2.1\\input\\Fire_Department_Calls.csv")
    fireServiceCallsDF1.printSchema()
    fireServiceCallsDF1.show(true)
    val fireSchema = StructType(Array(StructField("CallNumber", IntegerType, true),
      StructField("UnitID",StringType, true ),
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
      StructField("RowID", StringType, true)
    ))

    val fireServiceCallsDF = spark.read.format("csv")
      .option("header","true")
      .option("mode","DROPMALFORMED")
      .schema(fireSchema)
      .csv("C:\\Spark_Problems\\Spark-master\\Spark-2.1\\input\\Fire_Department_Calls.csv")

    import spark.implicits._
    fireServiceCallsDF.select("callType").show(5)
    fireServiceCallsDF.select("callType").distinct().show(5,false)
    fireServiceCallsDF.select("callType").groupBy("callType").count().orderBy($"count".desc).show


    val from_pattern1 = "MM/dd/yyyy"
    val to_pattern1 = "yyyy-MM-dd"

    val from_pattern2 = "MM/dd/yyyy hh:mm:ss aa"
    val to_pattern2 = "MM/dd/yyyy hh:mm:ss aa"

    import org.apache.spark.sql.functions._
    val fireServiceCallsTsDF = fireServiceCallsDF
      .withColumn("CallDateTS", unix_timestamp(fireServiceCallsDF("CallDate"), from_pattern1).cast("timestamp")).drop("CallDate")
      .withColumn("WatchDateTS", unix_timestamp(fireServiceCallsDF("WatchDate"), from_pattern1).cast("timestamp")).drop("WatchDate")
      .withColumn("ReceivedDtTmTS", unix_timestamp(fireServiceCallsDF("ReceivedDtTm"), from_pattern2).cast("timestamp")).drop("ReceivedDtTm")
      .withColumn("EntryDtTmTS", unix_timestamp(fireServiceCallsDF("EntryDtTm"), from_pattern2).cast("timestamp")).drop("EntryDtTm")
      .withColumn("DispatchDtTmTS", unix_timestamp(fireServiceCallsDF("DispatchDtTm"), from_pattern2).cast("timestamp")).drop("DispatchDtTm")
      .withColumn("ResponseDtTmTS", unix_timestamp(fireServiceCallsDF("ResponseDtTm"), from_pattern2).cast("timestamp")).drop("ResponseDtTm")
      .withColumn("OnSceneDtTmTS", unix_timestamp(fireServiceCallsDF("OnSceneDtTm"), from_pattern2).cast("timestamp")).drop("OnSceneDtTm")
      .withColumn("TransportDtTmTS", unix_timestamp(fireServiceCallsDF("TransportDtTm"), from_pattern2).cast("timestamp")).drop("TransportDtTm")
      .withColumn("HospitalDtTmTS", unix_timestamp(fireServiceCallsDF("HospitalDtTm"), from_pattern2).cast("timestamp")).drop("HospitalDtTm")
      .withColumn("AvailableDtTmTS", unix_timestamp(fireServiceCallsDF("AvailableDtTm"), from_pattern2).cast("timestamp")).drop("AvailableDtTm")

    fireServiceCallsTsDF.select(year(fireServiceCallsTsDF("CallDateTS"))).distinct().orderBy($"year(CallDateTS)".desc).show()
      fireServiceCallsTsDF.select((dayofyear(fireServiceCallsTsDF("CallDateTS"))), (year(fireServiceCallsTsDF("CallDateTS")))).show
  }
}
