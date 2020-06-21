package ScalaBasics

import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import org.bouncycastle.crypto.prng.drbg.DualECPoints

object SaveToHive {

  // case class Purchase(customer_id: Int, purchase: Int)
  /* case class Point(x: Double,y:Double)
  case class Segment(from: Point, to: Point)
  case class Line(name: String, points: Arry[Point])
  case class NamePoints(name: String, points: Map[String,Point])
  case class NameAndMaybePoint(name:String,point: Option[Point])
 */

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("WordCountDS Application").master("local").getOrCreate()

    import spark.implicits._

    val ds = spark.read.text("C:\\Spark_Problems\\Spark-master\\Spark-2.1\\input\\README.md").as[String]
    val result = ds.flatMap(_.split(" "))
      .filter(_!="") //Filter empty words
      //.toDF()
        .groupBy($"value")
      .agg(count("*") as "numOccurances")
        .orderBy($"numOccurances" desc)
    result.foreach{x => println(x)}




   // purchaseDF.coalesce(1).write.mode(SaveMode.Append).insertInto("Sales")
  }

}
