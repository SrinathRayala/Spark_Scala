package SparkBasics1

import org.apache.spark.{SparkConf, SparkContext}
object CountWord {
  def main(args: Array[String]): Unit =
{
  System.setProperty("hadoop.home.dir", "c:\\winutil\\")
  val logFile = "C:\\IFRToolLog.txt"
  val conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]")
  val sc = new SparkContext(conf)
  val logData = sc.textFile(logFile, 2).cache()
  val numAs = logData.filter(line => line.contains("a")).count()
  val numBs = logData.filter(line => line.contains("b")).count()
  println("Lines with A : %s,  Lines with B :%s".format(numAs,numBs) )
}
}
