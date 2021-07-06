package ScalaSparkCode

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import scala.util.control.Breaks

object stringCheck  {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder()
      .appName("stringCheck")
      .master("local")
      .getOrCreate()
    import spark.implicits._
    val loop = new Breaks;
    println("enter any String")
    val str = Console.readLine()
    var x:Char = '0'

    val charset = str.toSet.toList.sorted(Ordering.Char.reverse)

    loop.breakable {
      for (i <- charset) {
        if (str.contains(i.toUpper) && str.contains((i.toLower))) {

          if (i > (x)) {
            x = i.toUpper
            loop.break()
          }

        }

      }
    }

if(x != '0') {
  println("the highest character with both capital and small letter will be " +x)
}
else{
  println("NO")
}




  }



}
