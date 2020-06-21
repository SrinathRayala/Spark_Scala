package ScalaBasics

import org.apache.spark.{SparkConf, SparkContext, sql}
import org.apache.spark.sql.SparkSession

case class Person(name:String,empid: Int)

case class Empployee(empid:Int,emp_name:String)

object TestDataFrame {
 def main(args: Array[String]) : Unit = {
   val spark = new sql.SparkSession
   .Builder()
     .master("local")
     .appName("String")
     .getOrCreate()
   val person = Array(Person("john",1),Person("mike",2))
   val employee = Array(Empployee(1,"Aruba"))

   val personDF = spark.createDataFrame(person)
   val employeeDF = spark.createDataFrame(employee)
   val joinDF = personDF.join(employeeDF,Seq("empid"),"left")
   joinDF.write.partitionBy("Name").parquet("C:\\Spark_Problems\\Spark-master\\Spark-2.1\\output\\parquet")
   joinDF.show()
 }
}
