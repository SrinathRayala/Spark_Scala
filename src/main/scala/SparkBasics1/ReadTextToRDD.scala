package SparkBasics1

import org.apache.spark.{SparkConf, SparkContext, sql}

object ReadTextToRDD {
  def main(args: Array[String]): Unit = { // configure spark
    val sparkConf = new SparkConf().setAppName("Read Text to RDD").setMaster("local[2]").set("spark.executor.memory", "2g")
    // start a spark context
    val sc = new SparkContext(sparkConf)
    // provide path to input text file
    val path = "C:/password.txt"
    // read text file to RDD
    val lines = sc.textFile(path)
    // collect RDD for printing
    for (line <- lines.collect) {
      System.out.println(line)

      val spark = new sql.SparkSession
      .Builder()
        .master("local")
        .appName("String")
        .getOrCreate()

      spark.sparkContext.setLogLevel("Error")
      import spark.implicits._

      val rdd = sc.parallelize(Seq(temp("foo",  0.5), temp("bar",  0.0)))
      val rows = rdd.map({case temp(val1:String,val3:Double) => temp(val1,val3)}).toDF()
      rows.columns.toList

    }
  }
}
