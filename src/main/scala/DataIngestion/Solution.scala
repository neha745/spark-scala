package DataIngestion

import scala.io.Source
import org.apache.spark.SparkContext
object Solution {

  def main(args: Array[String]): Unit = {

    var arr = Array(0,0,0) // 51234
    var arr2 = new Array[Int](arr.length)
    val num = arr.length
    def x(a: Array[Int], k: Int): Array[Int] = {
      // write your code in Scala 2.12
      for(i <- 1 to k ){
        for(j <- 0 to num-1 by 1){
          if(j != (num-1) ) {

          arr2(j+1) = arr(j)

          }
          else

            arr2(0) = arr(j)

        }

        arr2.copyToArray(arr)



      }

      arr2
    }
    val y = x(arr,3)



  }
}
