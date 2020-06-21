package SparkBasics
import java.io.PrintWriter

import scala.io.StdIn._
import scala.io.Source

object Spark_FB {

  def main(args: Array[String]): Unit = {



    /**
    val a = Array(1, 3, 4, 23, 3)
    println(a.mkString(", "))
    a.foreach(println)
    a.map(_ * 2).foreach(println)
    a.map(i => i * 3).foreach(println)
    println("print %2")
    a.filter(_ % 2 == 0).foreach(println)
    println(a.forall(_ < 10))
    println(a.reduce(_ + _))

    val Array(dd, bb, dc) = "THIS IS TEST".split(" ")
    println(dd)

    val name = readLine()
    println("What is your name")

    val lst = buildList()
    //println(lst)
    // println(concatStrings(lst))
    println(concatstringsPat(lst))
      */
    val str = readfile()
    println("Retrun Value "+ str)

    grade(assignments =List(45,97),tests =List(343))

    val plus3 = add(3)_
    val eight = plus3(5)

    println(eight)

    println(threeTuple(math.random))

  }

  //def threeTuple(a:Double):(Double,Double,Double) =
  def threeTuple(a: => Double):(Double,Double,Double) =  // pass by name
{
  (a,a,a)
}
  def add(x:Int)(y:Int):Int = x+y
    // Method
    def buildList(): List[String] = {
      val input = readLine()
      if (input == "quit") Nil
      else input :: buildList() //:: cons
    }

    def concatStrings(words: List[String]): String = {
      if (words.isEmpty) ""
      else words.head + concatStrings((words.tail))
    }

    def concatstringsPat(words: List[String]): String = words match {
      case Nil => ""
      case h :: t => h + concatstringsPat(t)
            }

  def readfile():String = {

    val source = Source.fromFile("matrix.txt")
    val lines = source.getLines()
    val matrix = lines.map(_.split(" ").map(_.toDouble)).toArray
    source.close()
   println("Inside Loop " )
    val pw = new PrintWriter("rowSums.txt")
    matrix.foreach { row => pw.println(row.sum) }
    pw.close()
    "completed"
  }

  def grade(quizzes:List[Int] = Nil,assignments:List[Int] =  Nil,tests:List[Int] = Nil): Double ={
    0
  }


}
