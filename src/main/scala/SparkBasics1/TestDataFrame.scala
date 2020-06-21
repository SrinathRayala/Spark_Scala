package SparkBasics1

import org.apache.spark.sql.SparkSession

object TestDataFrame {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("test").master("local[1]").getOrCreate()
    val person = Array(Person("John", 1), Person("Mike", 2))
    val employee = Array(Employee(1, "Aruba"))
    val personDf = session.createDataFrame(person)
    val employeeDf = session.createDataFrame(employee)
    val joinDf = personDf.join(employeeDf, Seq("empId"), "left")
    joinDf.write.partitionBy("name").parquet("output/test")
    joinDf.show()
  }
}
