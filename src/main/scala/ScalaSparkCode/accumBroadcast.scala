package ScalaSparkCode

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, split, udf,explode,collect_list,size,count}
import scala.util.matching.Regex
import org.apache.spark.sql.types.{IntegerType,ArrayType}
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import java.lang.String._


object accumBroadcast {

  def main(args: Array[String]): Unit = {


    Logger.getLogger("org").setLevel(Level.ERROR)
    // now using accumulator variable to count the number of asset owned by every customer excluding null values in asset
    val spark = SparkSession.builder()
      .appName("accum")
      .master("local")
      .getOrCreate()

    import spark.implicits._


    def convertArray(str: String) = {

      val arr: List[String] = (str.split(",")).toList
      arr
    }

    val convAnony: String => Seq[String] = (asst: String) => {
      convertArray(asst)
    }
    val convUDF = udf(convAnony)

    val customer = spark.read
      .option("header", "true")
      .option("sep", "|")
      .option("inferSchema", "true")
      .csv("/Users/himanshubhardwaj/customer.csv")

    println(customer.rdd.partitions.length)

    val x = customer.withColumn("asset", explode(split(col("asset"), ",")))
      .filter(col("asset") =!= "null")
      .select("id", "asset").groupBy("id").agg((collect_list("asset"))
      .alias("assets"))
    //  .withColumn("totalasset" , agg(collect_list("asset")))

    println(x.rdd.partitions.length)


   x.write.mode("append").saveAsTable("customer")


    /*
  def assetCount(asset: Array[String]): Int = {
    val arr = asset.toSeq
    var c = 0

    for (i <- arr) {
      if (i != "null") {
        c += 1
      }

    }
    c
  }

  val assetCountAnony: Array[String] => Int = (asst: Array[String]) => {
    assetCount(asst)
  }
  val assetCountUDF = udf(assetCountAnony)
  //println(assetCount(Array("shud","null","huhuh","null","ghdghg")))

  customer.withColumn("asset_count", assetCountUDF(col("asset"))).show(10)

*/
    spark.stop()
  }
}