package SparkBasics1

import org.apache.spark.{SparkConf, SparkContext}

object Spark_not_Member {
  def toDoubleOrNeg(s:String): Double ={
    try {
      s.toDouble
    }
    catch {
      case _:NumberFormatException => -1
    }
  }
def main(args: Array[String]): Unit = {
  val conf = new SparkConf().setAppName("Temp Data").setMaster("local")
  val sc = new SparkContext(conf)

  val lines = sc.textFile("C:\\Spark_Problems\\BigDataAnalyticswithSpark-master\\MN212142_9392_1").filter(!_.contains("Day"))

  val data = lines.flatMap{ line =>
    val p = line.split(",")
    if (p(7) == "." || p(8) == "." || p(9) == ".") Seq.empty else
      Seq(TempSchema1(p(0).toInt, p(1).toInt, p(2).toInt, p(4).toInt, toDoubleOrNeg(p(5)), toDoubleOrNeg(p(6)), p(7).toDouble, p(8).toDouble, p(9).toDouble)) //This will convert into list.
  }

  /*
    def main(args: Array[String]): Unit = {
      val source = scala.io.Source.fromFile("C:\\Spark_Problems\\BigDataAnalyticswithSpark-master\\MN212142_9392_1")
      val lines = source.getLines().drop(1)
    val data = lines.flatMap{ line =>
      val p = line.split(",")
      if(p(6) == "." || p(7) =="." || p(8) ==".") Seq.empty else
        Seq(TempSchema1(p(0).toInt, p(1).toInt, p(2).toInt, p(4).toInt, toDoubleOrNeg(p(5)), toDoubleOrNeg(p(6)), p(7).toDouble, p(8).toDouble, p(9).toDouble))
    }.toArray
      //data.foreach(println)
    // println(linesDF.max()(Ordering.by(_.TMAX)))
    */
  println(data)
  val maxTemp = data.map(_.tmax).max
  val hotDays = data.filter(_.tmax == maxTemp)
  println(s" Hot Days are ${hotDays.collect().mkString(", ")}")
  println(data.max() (Ordering.by(_.tmax)))
    println(data.reduce((td1, td2) => if (td1.tmax >= td2.tmax) td1 else td2))

  val rainyCount = data.filter(_.precip >= 1.0).count()
  println(s"There are ${rainyCount} rainy days There is ${rainyCount*100.0/data.count()} percent")

val (r1,c1) = data.aggregate((0.0 ->0)) ({case ((sum,cnt),td) =>
if (td.precip < 1.0) (sum,cnt) else (sum+td.precip,cnt+1)},{case ((s1,c1),(s2,c2)) => (s1+s2,c1+c2)})

val (rainySum,rainyCount2) = data.aggregate(0.0 -> 0)({case ((sum,cnt),td) =>
    if(td.precip < 1.0) (sum,cnt) else (sum+td.tmax,cnt+1)
},{case ((s1,c1),(s2,c2)) =>
  (s1+s2,c1+c2)}
)
  data.map(_.precip).foreach(println)
  println(s"Average Rainy temp is ${rainySum/rainyCount2}")

  val rainyTemps = data.flatMap(td => if(td.precip < 1.0) Seq.empty else Seq(td.tmax))
  rainyTemps.foreach(println)

  println(s"Average Rainy temp is ${rainyTemps.sum/rainyTemps.count}")

  val monthGroups = data.groupBy(_.Month)
  monthGroups.collect().foreach(println)

  val monthlyTemp = monthGroups.map{ case (m,days) =>
  m -> days.foldLeft(0.0)((sum,td) => sum +td.tmax)/days.size
  }
  monthlyTemp.collect().foreach(println)

  monthlyTemp.sortBy(_._2).foreach(println)

  println("StDev of Highs: " +data.map(_.tmax).stdev())
}
}
