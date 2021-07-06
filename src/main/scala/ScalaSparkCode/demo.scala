package ScalaSparkCode
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.types.StringType

object demo extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .appName("PhoneNumberStandardisation")
    .master("local")
    .getOrCreate()

  import spark.implicits._
  case class custcase(cid: Int, name: String, add : String,phone :String, email :String,
                      credit_card_num :String , social_security_number :String)


  var dataset = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("/Users/himanshubhardwaj/Downloads/cust_Data.csv")
      .as[custcase]

  dataset.show(10)
  spark.stop()
}
