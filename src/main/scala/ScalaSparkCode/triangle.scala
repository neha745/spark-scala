package ScalaSparkCode

object triangle {

  def main(args: Array[String]): Unit = {

  def solution(a: Array[Int]): Int = {

      val P = 0
      val Q = 2
      val R = 4
      var i: Int = 0

      if (0 <= P & P < Q & Q < R & R < a.length) {

        if ((a(P) + a(Q)) > a(R) & (a(Q) + a(R)) > a(P) & (a(R) + a(P)) > a(Q)) {
          i=1
        }
        else
          i=0
      }
      i
    }

  }

}
