package SparkBasics

object GradeBook {
  def main(args: Array[String]): Unit = {
    val students = List(new Student("jane","Doe"),new Student("john","doe"))
    for(s <- students)
      printStudent(s)
  }

  def printStudent(s:Student) : Unit = {
    println(s.firstName +" " +s.lastName)
    println("Grade =" + s.average)
    println(s.tests.mkString)
  }

}
