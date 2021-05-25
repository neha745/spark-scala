package ScalaSparkCode

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import java.util.Date
import java.time.format.{DateTimeFormatter, DateTimeParseException}
import java.time.LocalDate

object AnyDateFormatChange{

  def main(args : Array[String])= {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("DfOperation")
      .master("local")
      .getOrCreate()

    import spark.implicits._
    val dateData = spark.read.
      option("header", "true")
      .option("inferSchema", "true")
      .csv("/Users/himanshubhardwaj/date_dataset.csv")
    dateData.show(10)



    def DateChange(str : String) :String= {

      println("start")
      val arr  = Array("dd/MM/yyyy","yyyy/MM/dd","yyyy.MM.dd","dd-MM-yyyy","yyyy-MM-dd","dd.MM.yyyy")
      var date:LocalDate = null
      var s:String = null

      for(i <- arr){
        try {
          val formatter = DateTimeFormatter.ofPattern(i)
          date =  LocalDate.parse(str, formatter)
          s= date.toString


        }
        catch  { case e: DateTimeParseException => null
        case e: java.text.ParseException  => null}

      }
      s
    }


    val DateConverter: String=>String = ( strDate : String)=> {DateChange(strDate)}
    val DateconvertUDF = udf(DateConverter)

    //println(DateChange("23-11-1993") )
    val Data = dateData.withColumn("New", DateconvertUDF(col("date")))
      .withColumn("New_Date" ,to_timestamp(col("New"),"yyyy-MM-dd"))
    Data.show(10)
    Data.printSchema()

    spark.stop()
  }
}
