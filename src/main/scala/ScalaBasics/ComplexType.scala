package ScalaBasics
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ComplexType {

  case class Point(x:Double,y:Double)
  case class Segment(from: Point,to:Point)
  case class Line(name:String,points: Array[Point])
  case class NamePoints(name: String,points:Map[String,Point])
  case class NameAndMaybePoint(name:String,point: Option[Point])

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Dataset-ComplexType").master("local").getOrCreate()
spark.sparkContext.setLogLevel("error")
    import spark.implicits._

    println("*** Nested Case Classes")

    val segment = Seq(
      Segment(Point(1.0,2.0),Point(3.0,4.0)),
    Segment(Point(8.0,2.0),Point(3.0,14.0))
    )
    val SegmentsDS = segment.toDS()

    SegmentsDS.printSchema()
    SegmentsDS.show()
println("*** filter by one column and fetch another")
    SegmentsDS.where($"from".getField("x") < 9).select($"to").show()


    println("*** Example 2: arrays")

    val lines = Seq(
      Line("a",Array(Point(0.0,0.0),Point(2.0,4.0))),
      Line("b",Array(Point(-1.0,0.0)))
    )
 val linesDS = lines.toDS()
    linesDS.printSchema()
    linesDS.show()

    println("*** filter by an anrray element")
    linesDS.where($"points".getItem(2).getField("y") < 7.0).select($"name",size($"points").as("count")).show()
  }

}
