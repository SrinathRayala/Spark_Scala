package SparkBasics1

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.max

object FireDepartmentCalls2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DataFrame-ProblemStatement")
      .master("local[2]")
      .getOrCreate()
    import spark.implicits._
    val dataRDD = spark.read.format("csv").option("header","true").load("C:\\Spark_Problems\\Spark-master\\Spark-2.1\\input\\pbs.csv").rdd
    val filteredDF =dataRDD.map(x => (x(2).toString(),x(3).toString().replace("$","").toDouble)).toDF("dept","salary").dropDuplicates().toDF
val maxSalDF = filteredDF.groupBy("dept").agg(max(filteredDF.col("salary")).as("MaxSal")).sort("dept")
   // maxSalDF.show
    val subDF = filteredDF.except(maxSalDF)
val ScndMaxSalDF = subDF.groupBy("dept").agg(max(subDF.col("Salary")).as("SecMaxSal")).sort("dept")
// ScndMaxSalDF.show
    val problem1ResDF = maxSalDF.join(ScndMaxSalDF, Seq("dept")).sort("dept").toDF()
    // problem1ResDF.show()

    problem1ResDF.coalesce(1).write.option("header","true").csv("C:\\SparkTest\\Output\\file1.csv")

    val problem2DF = dataRDD.map(x => (x(0).toString(), x(2).toString(), x(3).toString().replace("$","").toDouble)).toDF("name","dept","salary").dropDuplicates().toDF()

val resDF = problem2DF.join(maxSalDF, Seq("dept")).sort("dept").toDF()
    val problem2ResDF = resDF.withColumn("diffsal",(resDF.col("MaxSal") - resDF.col("Salary")))
    problem2ResDF.coalesce(1).write.option("header","true").csv("C:\\SparkTest\\Output\\file2.csv")

  }

}
