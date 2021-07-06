package ScalaSparkCode

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col,split,explode,regexp_replace,regexp_extract}
import scala.util.matching.Regex

object StringEval {

  def main(args: Array[String]): Unit = {


    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder()
      .appName("stringCheck")
      .master("local")
      .getOrCreate()

    var lines = spark.read.text("/Users/himanshubhardwaj/Desktop/milk.txt").toDF("lines")

    import spark.implicits._

    lines = lines.withColumn("lines",
      split(col("lines"), "[\\s,-\\.]"))
        .withColumn("lines" , explode($"lines"))
        .withColumn("lines" ,col("lines").rlike("\\w+"))

      //  .withColumn("lines" , regexp_extract($"lines", "[.]?(\\w)[.]?", 1))
        .groupBy("lines").count().where($"count" === 1)
      lines.show(10)




    spark.stop()
  }
}
