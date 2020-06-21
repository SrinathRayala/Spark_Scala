package ScalaBasics
import java.sql.DriverManager
import java.util.Properties
import org.apache.spark.sql.SparkSession

object MySQLFromDF  extends App {
  val spark = SparkSession.builder()
    .master("local")
    .appName("Sample Job")
    .getOrCreate()

  val url = "jdbc:mysql://35.232.46.143:3306/retail_db"
  val prop = new Properties()
  val table = "orders"
  prop.put("user", "user")
  prop.put("password", "password")
Class.forName("com.mysql.jdbc.Driver")
  val query = "select * from orders"
  val mysqlDF = spark.read.jdbc(url,s"($query) as cust_table",prop)
  val selectCustomers = mysqlDF.select("order_id").groupBy("order_id").count()
  selectCustomers.show(50)
mysqlDF.show(50)
}
