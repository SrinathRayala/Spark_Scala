package ScalaBasics
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Filter {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Spark Filter Example").setMaster("local")

    val sc = new SparkContext(conf)

    println("------Printing Spark Configs")

    //sc.getConf.getAll.foreach(f => println(f))
    val x = sc.parallelize(List("Transformation demo", "Filter Demo","Filter Spark demo"))
    val lines1 = x.filter(line => line.contains("Spark") || line.contains("demo"))
    println("\n")
    println("______________________")
    lines1.collect().foreach{line => println(line)}

    val lines2 = x.filter(line => ! line.contains("Filter"))

    lines2.collect().foreach(line => println(line))

    val lines3 = x.filter(line => line.contains("Spark")).count()

    println("Count is :" + lines3)


  }
}
