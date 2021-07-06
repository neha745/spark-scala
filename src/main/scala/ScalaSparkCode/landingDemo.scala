package ScalaSparkCode

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession


object landingDemo {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Demo")
      .master("local")
      .getOrCreate()

  }

}

