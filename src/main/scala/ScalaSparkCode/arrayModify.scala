package ScalaSparkCode

object arrayModify {

  def main(args: Array[String]): Unit = {

    println("enter an array with space as delimiter")
    val arr = Console.readLine().split(" ").map(_.toInt)
    val list1 = List.empty[String] //create empty list

    var sum =0
    var list = scala.collection.mutable.ListBuffer[Int]()
    list ++= arr.toList

    for(i <- 0 to arr.length-1 by 1){
        sum +=arr(i)



    if( sum <0){
      sum -=arr(i)

      val elemnt = arr(i)
      if(list.contains(elemnt)){
        //list = list.filter(_!=elemnt)
        list -= elemnt
        list :+=elemnt


      }

    }

    }
    println(list)
  }

}


/*
      list ++= arr.slice(0,i).toList
      println(list)
      list ++= arr.slice(i+1, arr.length)
      list :+=elemnt
      println(list)
      list.drop(i)
      println(list)*/