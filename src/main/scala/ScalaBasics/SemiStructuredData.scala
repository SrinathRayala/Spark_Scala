package ScalaBasics
import org.apache.spark.sql.{SQLContext, SparkSession, functions}
import org.apache.spark.{SparkConf, SparkContext}

object SemiStructuredData {

    case class University(name: String,numStudents: Long,yearFounded: Long)
    def main(args: Array[String]): Unit = {
      val spark = SparkSession.builder().appName("SemiStructureData").master("local").getOrCreate()
      import spark.implicits._
      val sc = spark.sparkContext
      val SqlContext = new SQLContext(sc)
      val schools = SqlContext.read.json("C:\\Spark_Problems\\Spark-master\\Spark-2.1\\input\\schools.json").as[University]
      schools.printSchema()

      val res = schools.map(s => s"${s.name} is ${2017 - s.yearFounded} years old")
      res.foreach{x => println(x)}


  }

}
