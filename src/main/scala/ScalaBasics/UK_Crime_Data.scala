
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.SaveMode
import scala.annotation.tailrec
import org.apache.spark.{SparkConf, SparkContext}

object UK_Crime_Data {
  def main(args: Array[String]): Unit = {

    def fixColumns(df:DataFrame): DataFrame ={
      @tailrec def fixNext(colsLeft: List[String], curDF: DataFrame): DataFrame ={
        colsLeft match {
          case Nil => curDF
          case col :: rest =>
            val Array(firstToken,remainingTokens @ _*) = col.split(" ")
            val newName = firstToken.toLowerCase + remainingTokens.map(_.capitalize).mkString("")
            fixNext(rest,curDF.withColumnRenamed(col,newName))
        }
      }
      fixNext(df.columns.toList, df)
    }


    val spark = new SparkSession
    .Builder()
      .appName("Json file formats")
      .master("local")
      .getOrCreate()

    val schema = spark
      .read
      .option("header","true")
      .option("inferschema","true")
      .csv("C:\\Spark_Problems\\2018-12\\2018-12-btp-street.CSV").schema

    schema.foreach(println)

    def convert(from:String,to:String) = {
      val df = spark.read.option("header",true).schema(schema).csv(from)

     fixColumns(df).drop("crimeID").write.mode(SaveMode.Overwrite).parquet(to)//parquet("C:\\Spark_Problems\\2018-12")
      df.drop("crimeID").write.mode(SaveMode.Overwrite).parquet(to)//parquet("C:\\Spark_Problems\\2018-12")
      df.show()
      df
    }
       convert("C:\\Spark_Problems\\2018-12\\2018-12-btp-street.CSV","C:\\Spark_Problems\\2018-12\\2018-12-btp-street.parquet")
    val df = spark.read.parquet("C:\\Spark_Problems\\2018-12\\2018-12-btp-street.parquet")
    df.printSchema()
    df.show()
  }
}
