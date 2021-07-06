package ScalaSparkCode

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{udf, when, _}

import scala.util.matching.Regex

//reformat the creditCardNumber and social security number and mask the creditcard only last 4 digit should be visible then
//  reformat the socialsecuritynumber.
//find cc ending with 9 and begining with 8795 using regex and fetch all the last 4 digit of ssc for each customer
//if i want to find the list of starting 4 digit of all credit cards to calculate the number of visa cards or master card issued
object PhoneNumberStandardisation {
  def main(args : Array[String])= {

    Logger.getLogger("org").setLevel(Level.ERROR)

    def numChange(str :String) :String={

      val p1 = """0?(\d{10})""".r
      val p2 = """(\d{3})[-,](\d{4})[-,](\d{3})""".r
      val p3 = """\+91-?(\d{10})""".r
      println()
      val retrn= str match {

        case p1(phone) => "+91" +phone//("""\+91$1""".r).toString()
        case p2(ph1,ph2,ph3) => "+91"+ph1+ph2+ph3
        case p3(ph) => "+91"+ph
        case _ => "wrong data"
      }
      retrn  }

    val spark = SparkSession.builder()
      .appName("PhoneNumberStandardisation")
      .master("local")
      .getOrCreate()


    val dataset = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("/Users/himanshubhardwaj/Downloads/cust_Data.csv")


    val pConverter: String=>String = ( strphn : String)=> {numChange(strphn)}
    val PhnconvertUDF = udf(pConverter)

    dataset.withColumn("phone_number", PhnconvertUDF(col("phone"))).show(10)

    dataset.filter(col("credit_Card_num").rlike("9$")).show(10)



    dataset.withColumn("social_security_number", regexp_extract(col("social_security_number"),
      "(\\d{4})()\\d{4}(\\d{4})" ,3)).show(10)


    dataset.withColumn("credit_Card_redacted", regexp_replace(col("credit_Card_num"),
      "(\\d{4})()\\d{4}(\\d{4})", "XXXX-XXXX-$3" )).show(10)


    //def getVisaInfo()
    val crd_info =dataset.withColumn("Card_Name" ,
      when(col("credit_Card_num").rlike("^8795"), lit("Visa"))
        . when(col("credit_Card_num").rlike("^9999"), lit("Master"))
        .when(col("credit_Card_num").rlike("^4709"), lit("Rupay"))
        .otherwise(lit("Old card")))

    crd_info.printSchema()
    println("the total number of different cards issued to customer would be\n")
    crd_info.select("Card_Name").groupBy("Card_Name").count().show(10)


    spark.stop()
  }

}


//DOCUMENTATION
//LINE 53-   so here we can search for any pattern in a column using rlike,for eg numbers ending from 9.
//LINE 58-//here we are extracting the last 4 digit of ssc for that we use regex extract so first we need to give the pattern
//this would be a regex pattern with grouping  but we wont use '.r' it is already understood and then we select the group id we want
//LINE 61- here we are formatting the data we take a column use regex replace and then again give a pattern and specify how my
//o/p data should look like, here $3 means 3rd group created
//LINE 66 ONWARDS - so here we are trying to find out the num of different types of card issued to customers so first using when-
//otherwise clause we will find the different types of card and then we will use groupBy to group and count.




