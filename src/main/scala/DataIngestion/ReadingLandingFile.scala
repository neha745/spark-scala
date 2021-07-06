package DataIngestion

import java.io.{BufferedWriter, File, FileWriter}

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import java.util.regex.{Matcher, Pattern}

import scala.util.control.Breaks.{break, breakable}

object ReadingLandingFile {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder()
      .appName("ReadingLandingDir")
      .master("local")
      .getOrCreate()

    val props = ConfigFactory.load()
    val logger = LoggerFactory.getLogger(this.getClass)
    val Filepattern = props.getString("pattern")

    import spark.implicits._

    val hadoopConf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(hadoopConf)
    val landing_path = props.getString("landingpath")

    val files_status_list = fs.listFiles(new Path(landing_path), true)
    println(files_status_list.hasNext)

    /**
     * now we have to check wether the file is the genuine file and not any other useless file aso we have to
     * check the name of file whether it matches the given pattern or not or else it is some random file.
     * so in order to match we will have a regex pattern which will match the name but in order to do the match
     * we will use pattern method from regex class so it will simply ask for string and the expr and we give boolean
     * we have many regex func like findFirstIn so these are not valid in this scenario. one regex matcher we
     * used in matching the phone number string with given multiple format using match case which matched regex
     * expression with the string. here we dont have multiple case so we will simply use matcher.
     */


    val patt = Pattern.compile(Filepattern)
    logger.info(patt.toString)

    while (files_status_list.hasNext) {
      breakable {
        val landingFilePath = files_status_list.next().getPath
        var fileName = landingFilePath.getName
        logger.info(s"file name : $fileName")
        if (fileName.endsWith("control")) break()







      }
    }



    spark.stop()
  }

}
