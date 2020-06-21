package Higher_Order_Methods

import org.apache.spark.{SparkConf,SparkContext}
import org.apache.spark.SparkContext.toString

case class LAarea(code: String,text: String)
case class LAseries(id:String,area:String,measure:String,title:String)
case class LAdata(id: String,year: Int,period:Int,value:Double)
object RDdUnemployment {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("Temp Data")
    val sc = new SparkContext(conf)

    sc.setLogLevel("WARN")

    val LAareas = sc.textFile("C:\\Spark_Problems\\la.SparkTest0331.area.TXT").filter(!_.contains("area_type")).map {line =>
      val p = line.split("\t").map(_.trim)
      LAarea(p(1),p(2))
    }.cache()
    LAareas.take(10).foreach(println)

    val LAseries1 = sc.textFile("C:\\Spark_Problems\\la.SparkTest0331.series.TXT").filter(!_.contains("area_type")).map {line =>
      val p = line.split("\t").map(_.trim)
      LAseries(p(0),p(2),p(3),p(6))
    }.cache()
    LAseries1.take(10).foreach(println)


    val LAdata1 = sc.textFile("C:\\Spark_Problems\\la.SparkTest0331.data.30.Minnesota.TXT").filter(!_.contains("series_id")).map{line =>
      val p = line.split("\t").map(_.trim)
      LAdata(p(0),p(1).toInt,p(2).drop(1).toInt,p(3).toDouble)
    }.cache()
    LAdata1.take(10).foreach(println)

    val rates = LAdata1.filter(_.id.endsWith("03"))
    val decadeGroups = rates.map(d => (d.id,d.year/10) -> d.value)
    val decadeAverages = decadeGroups.aggregateByKey(0.0 -> 0)({case ((s,c),d) =>
      (s+d,c+1)
    },{ case ((s1,c1),(s2,c2)) => (s1+s2,c1+c2)}).mapValues(t => t._1/t._2)   //Combined by key
    decadeAverages.take(5).foreach(println)

    val maxDecade = decadeAverages.map{case ((id,dec),av) =>  id ->(dec*10,av)}.
      reduceByKey {case((d1,a1),(d2,a2)) => if (a1 >= a2)(d1,a1) else (d2,a2)}
    maxDecade.take(5).foreach(println)

    val seriesPairs = LAseries1.map(s =>s.id ->s.title)
      val joinedMasDecades = seriesPairs.join(maxDecade)
    joinedMasDecades.take(10).foreach(println)

    val dataByArea = joinedMasDecades.mapValues{ case (a,(b,c)) => (a,b,c)}.
      map{case (id,t) => id.drop(3).dropRight(2) -> t}  //It will drop the first 3 character and last 2 char from endof the string
    dataByArea.take(5).foreach(println)

    val fullyJoined = LAareas.map(a => a.code ->a.text).join(dataByArea)

    fullyJoined.take(5).foreach(println)


    sc.stop()

  }

}
