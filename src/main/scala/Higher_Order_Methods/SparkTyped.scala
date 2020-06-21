package Higher_Order_Methods

import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Encoders


case class Series(sid: String,area:String,measure:String,title:String)
case class LAData(id:String,period:String,value:Double)
object SparkTyped {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Temp Data").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val spark = SparkSession.builder().master("local").getOrCreate()

    import spark.implicits._

     /*val countyData = spark.read.schema(Encoders.product[LAData].schema) // Schema to build from Case class..not required to write the PrintSchema.
      .option("header",true)
        .option("delimiter","\t").csv("C:\\Spark_Problems\\la.SparkTest0331.data.30.Minnesota.TXT")
      .select((trim('id) as "id",'year,'period,'value).as[LAData].
      sample(false,0.1).cache()
      */
      val countyData = spark.read.textFile("C:\\Spark_Problems\\la.SparkTest0331.data.30.Minnesota.TXT").map { line =>
        val p = line.split("\t").map(_.trim)
        LAData(p(0), p(2), p(3).toDouble)
      }

    val series = spark.read.textFile("C:\\Spark_Problems\\la.SparkTest0331.series.TXT").map { line =>
      val p = line.split("\t").map(_.trim) //it will trim extra space
      Series(p(0),p(2),p(3),p(6))
    }.cache()

    val zipData = spark.read.schema(Encoders.product[LAData].schema).option("header",true).csv("").as[LAData].filter('lat.isNotNull).cache()

    val joined1 = countyData.joinWith(series, 'id === 'sid)  //joining typed dataset is difficult
    countyData.show()
    series.show()
    joined1.show()
    spark.stop()


  }

}
