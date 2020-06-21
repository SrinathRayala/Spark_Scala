package ScalaBasics

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions
import org.apache.spark.sql.types
import org.apache.spark.sql.Column


object DatasetBasic {

  case class Number(i:Int, english:String, french:String)
  def main(args: Array[String]): Unit ={

    val spark = SparkSession.builder().appName("SemiStructureData").master("local").getOrCreate()

    import spark.implicits._

    val numbers = Seq(
      Number(1,"one","un"),
      Number(2,"two","deux"))
    val numbersDS = numbers.toDS()

    println("**** Case Classs DataSet types")
    numbersDS.dtypes.foreach(println(_))

    println("*** filter by one column and fetch another")
    numbersDS.where($"i"> 1).select($"english",$"french").show()

    val anotherDS = spark.createDataset(numbers)

    println("*** Case class Dataset types")
    anotherDS.dtypes.foreach(println(_))

  }

}
