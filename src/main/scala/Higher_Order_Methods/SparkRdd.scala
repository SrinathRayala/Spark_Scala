package Higher_Order_Methods

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf,SparkContext}
import Higher_Order_Methods.TempData

object SparkRdd {
  def main(args: Array[String]): Unit = {
    val spark = new SparkConf().setAppName("Temp Data").setMaster("local")
    val sc = new SparkContext(spark)

    sc.setLogLevel("Error")
    val lines = sc.textFile("C:\\Spark_Problems\\BigDataAnalyticswithSpark-master\\MN212142_9392_1").filter(!_.contains("Day"))


    val data = lines.flatMap { line =>
      val p = line.split(",")
      if(p(6) == "." || p(7) =="." || p(8) ==".") Seq.empty else
      Seq(TempData(p(0).toInt,p(1).toInt,p(2).toInt,p(3).toInt,
        TempData.toDoubleOrNeg(p(4)),TempData.toDoubleOrNeg(p(5)),p(6).toDouble,p(7).toDouble,p(8).toDouble))
    }.cache()

    val maxTemp = data.map(_.tmax).max
    val hotDays = data.filter(_.tmax == maxTemp)
    println(s"Hot Days are ${hotDays.collect().mkString(", ")}")

    println(data.max()(Ordering.by(_.tmax)))
    println(data.reduce((td1,td2) => if (td1.tmax >= td2.tmax) td1 else td2))

    val rainyCount = data.filter(_.precip >= 1.0).count()
    println(s"${rainyCount*100.0/data.count()} percent")

    val keyedByYear = data.map(td => td.year -> td)
    val averageTempByYear = keyedByYear.aggregateByKey(0.0 -> 0) ({ case((sum,cnt),td) =>
      (sum+td.tmax, cnt+1)
    }, {case ((s1,c1),(s2,c2)) => (s1+s2,c1+c2)})
    println(data.count())
    averageTempByYear.foreach(println)

  }

}
