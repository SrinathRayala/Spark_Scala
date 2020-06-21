package SparkBasics1

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object ImposingOwnSchema {
  def main(args: Array[String]): Unit  ={
    val spark = new SparkSession.Builder().appName("Schema").master("local").getOrCreate()

    val namesDF = spark.read
      .option("header",true)
      .option("inferSchema",true)
      .csv("C:\\\\Users\\\\psadmin\\\\IdeaProjects\\\\untitled1\\\\resources\\\\DataSet\\\\comma\\\\companylist_noheader.csv")
    namesDF.printSchema()
    val ownSchema = StructType(
      StructField("DDD", StringType,true) ::
      StructField("3D", StringType,true) ::
      StructField("51.37", StringType,true) :: Nil)

    val namesWithOwnSchema = spark.read.option("header",true)
      .schema(ownSchema)
      .csv("C:\\Users\\psadmin\\IdeaProjects\\untitled1\\resources\\DataSet\\comma\\companylist_noheader.csv")
    namesWithOwnSchema.printSchema()
      }

}
