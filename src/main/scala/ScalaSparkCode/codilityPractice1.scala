package ScalaSparkCode
import scala.collection.mutable.{ListBuffer, Set}

object codilityPractice1 {

  def main(args: Array[String]): Unit = {

    var arr:Array[Int] = Array.empty[Int]
    var arr2 = new Array[Int](9)
    var arr1 = Array(1,2,3)
    var k =0
    arr = Array.concat(arr,arr1)

    for (i <- (arr1.length -1) to 0 by -1  ){

      arr2(k) = arr1(i)
      k +=1
    }
    //arr2(9) =99  ARRAY IS IMMUTABLE WE CAN CHANGE THE ELEMENTS BUT CANNOT ADD ELEMENTS
    //arr2(10) = 89
    arr2.foreach(println)

    var l:List[Int] = List.empty[Int]
    var l2 = List(1,2,3)
    val x = List.concat(l,l2)
       l2:+=99 //we CANNOT add elements to list we can add or concat 1 list with another so listbuffer
l2.foreach(print)


val lb = new ListBuffer[Int]()
    lb += 1
    lb+= 2

var s = Set[Int]()
    s += 3
    s






  }


}
