package ScalaSparkCode
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object demo extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .appName("PhoneNumberStandardisation")
    .master("local")
    .getOrCreate()

  val dataset = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("/Users/himanshubhardwaj/Downloads/cust_Data.csv")

  dataset.show(10)
  spark.stop()
}
