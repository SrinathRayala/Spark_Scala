package ScalaBasics
import org.apache.spark.{SparkConf, SparkContext, sql}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ScalaFireDepartment {
  def main(args: Array[String]): Unit = {
    val spark = new SparkSession.Builder().master("local").getOrCreate()
    spark.sparkContext.setLogLevel("Error")
    import spark.implicits._
    //Best Salary and Send best Salary of employees
    val dataRDD = spark.read.format("csv")
      .option("header",true)
      .load("C:\\Spark_Problems\\Spark-master\\Spark-2.1\\input\\pbs.csv").rdd
    val filteredDF = dataRDD.map(x => (x(2).toString(),x(3).toString.replace("$","").toDouble)).toDF("dept","salary").dropDuplicates()
   // filteredDF.show()
    val maxSalDF = filteredDF.groupBy("dept").agg(max(filteredDF.col("salary")).as("MaxSal")).sort("dept")
    // val maxSalDF1 = filteredDF.reduce
    maxSalDF.show
    val subDF = filteredDF.except(maxSalDF)
    val ScndMaxSalDF = subDF.groupBy("dept").agg(max(subDF.col("salary")).as("SecMaxSal")).sort("dept")
    ScndMaxSalDF.show

    val ResDF = maxSalDF.join(ScndMaxSalDF, Seq("dept")).sort("dept").toDF()
    ResDF.show()

  //  ResDF.coalesce(1).write.option("header",true).csv("C:\\Spark_Problems\\Spark-master\\Spark-2.1\\output\\pbs.csv")

    val empSalDF = dataRDD.map(x => (x(0).toString(),x(2).toString(),x(3).toString.replace("$","").toDouble)).toDF("name","dept","Salary")

    val empSal1DF = empSalDF.join(maxSalDF,Seq("dept")).sort("dept").toDF()
    empSal1DF.show()

    val finalResDF = empSal1DF.withColumn("diffSal",(empSal1DF.col("MaxSal") - empSal1DF.col("Salary")))
    finalResDF.coalesce((1)).write.option("header",true).csv("C:\\Spark_Problems\\Spark-master\\Spark-2.1\\output\\pbs2.csv")
  }

}
