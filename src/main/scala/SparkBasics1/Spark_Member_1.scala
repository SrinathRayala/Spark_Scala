package SparkBasics1

import org.apache.spark.{SparkConf, SparkContext}

object Spark_Member_1 {
  var series_id: Boolean = _

  var year: Boolean = _

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("Spark RDD")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    /*
    val LAlines = sc.textFile("C:\\Spark_Problems\\la.data.30.Minnesota.TXT").take(5)
    // println(LAlines.size)
    LAlines.foreach(println)
    */

    val LAarea = sc.textFile("C:\\Spark_Problems\\la.area.TXT").filter(!_.contains("area_type")).map{line =>
      val p = line.split("\t").map(_.trim)
      area(p(1),p(2))
    }
    val LAseries = sc.textFile("C:\\Spark_Problems\\la.series.TXT").filter(!_.contains("series_id")).map { line =>
      val p = line.split("\t").map(_.trim)
      series(p(0),p(2),p(3),p(6))
    }

    val LAdata = sc.textFile("C:\\Spark_Problems\\la.data.30.Minnesota.TXT").filter(!_.contains("series_id")).map { line =>
      val p = line.split("\t").map(_.trim)
      data1(p(0),p(1).toInt,p(2).drop(1).toInt,p(3).toDouble)
    }.cache()


    val LAdata1 = sc.textFile("C:\\Spark_Problems\\la.data.30.Minnesota_1.TXT").filter(!_.contains("series_id")).map{line =>
      val p = line.split("\t").map(_.trim)
      data1(p(0),p(1).toInt,p(2).drop(1).toInt,p(3).toDouble)
    }

    val rates = LAdata1.filter(_.id.endsWith("03"))
    val decadeGroups = rates.map(line => (line.id,line.year/10) -> line.value)
    val decadeAverages = decadeGroups.aggregateByKey(0.0 -> 0)({case ((s,c),d) =>
      (s+d,c+1)
      },{case ((s1,c1) ,(s2,c2)) => (s1+s2,c1+c2)}).mapValues(t =>t._1/t._2)


    val rates1 = LAdata1.filter(_.id.endsWith("04"))
    val decateGroups1 = rates1.map(line => (line.id,line.year/10) -> line.value)
      val decateAverages1 = decateGroups1.aggregateByKey(0.0 -> 0)(
        {case ((s,c),d) => (s+d,c+1)},
        {case ((s1,c1),(s2,c2)) => (s1+s2,c1+c2)}
      ).mapValues(t =>t._1/t._2)

    val maxDecade = decateAverages1.map {case((id,dec),av) => id -> (dec,av)}.reduceByKey{case ((d1,a1),(d2,a2)) => if (a1 >= a2) (d1,a1) else (d2,a2)}

    //val seriesPairs = LAseries.filter(_.series_id == "LASST270000000000003" || _.series_id == "LASST270000000000003").map(s =>s.series_id -> s.series_title)
    val seriesPairs = LAseries.map(s =>s.series_id -> s.series_title)
    val joinedMaxDecades = seriesPairs.join(maxDecade)

    val dataByArea = joinedMaxDecades.mapValues{ case(a,(b,c)) => (a,b,c)}.map{ case(id,t) => id.drop(3).dropRight(2) -> t}

    val fullJoined = LAarea.map(a =>a.area_code ->a.area_text).join(dataByArea)
/*
    val decadeAverages = decadeGroups.aggregateByKey(0.0 -> 0)({case ((s,c),d) =>
      (s+d,c+1)
    },{ case ((s1,c1),(s2,c2)) => (s1+s2,c1+c2)}).mapValues(t => t._1/t._2)   //Combined by key
*/
    // println(LAarea)
   // val L1 = LAarea.isEmpty()
   // println(L1)
    //LAarea.foreach(println)
    //LAseries.foreach(println)
    //LAdata1.foreach(println)
    decadeAverages.foreach(println)
    //decateAverages1.foreach(println)
    maxDecade.foreach(println)
    //rates.map(d =>d ).foreach(println)
    //joinedMaxDecades.take(5).foreach(println)

    dataByArea.foreach(println)
    fullJoined.foreach(println)
sc.stop()



  }
}
