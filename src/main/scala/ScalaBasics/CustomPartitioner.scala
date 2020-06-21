package ScalaBasics
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner,SparkContext,SparkConf}
import org.apache.spark.SparkContext._

class SpecialPartitioner extends Partitioner {
  def numPartitions = 10
  def getPartition(key:Any): Int ={
    key match{
      case (x, y:Int, z) => y % numPartitions
      case _ => throw new ClassCastException
    }
  }
}

object CustomPartitioner {

  def analyze[T](r: RDD[T]) : Unit = {
    val partitions = r.glom()

    println(partitions.count() + " partitions")

    partitions.zipWithIndex().collect().foreach {
      case (a,i) => {
        println("partition "+ i +" contents(count " + a.count(_ => true) +"):" + a.foldLeft("")((e,s) => e + " " + s))
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val spark = new SparkConf().setAppName("Streaming").setMaster("local")
    val sc = new SparkContext(spark)
    val triplets = for(x <- 1 to 3; y <- 1 to 20; z <- 'a' to 'd') yield ((x,y,z), x*y)

    val defaultRDD = sc.parallelize(triplets,10)
    println("with default partitioning")
    analyze(defaultRDD)

    val deliberateRDD = defaultRDD.partitionBy(new SpecialPartitioner())

println("with deliberate partitioning")
    analyze(deliberateRDD)
  }

}
