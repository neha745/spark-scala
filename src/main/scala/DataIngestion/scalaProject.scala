package DataIngestion

import org.apache.spark.sql.SparkSession

object scalaProject {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("stringCheck")
      .master("local")
      .config("hive.exec.dynamic.partition", "true")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .getOrCreate()


    val df = spark.read
      .option("inferSchema" , "true")
      .option("header" , "true")
      .load("hdfs:///user/hadoop/data/*.csv")

    val results = df.groupBy("VendorID").count()

    results.write.partitionBy("VendorID").parquet("/user/hadoop/output/")


spark.stop()

  }

}
