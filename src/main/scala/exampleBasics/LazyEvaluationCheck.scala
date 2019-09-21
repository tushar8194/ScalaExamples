package exampleBasics


object LazyEvaluationCheck {

  def main(args: Array[String]): Unit = {
    val ages = Seq(42, 75, 29, 64)
    //s is called interpolation string. Prepending s to any string literal allows the usage of variables directly in the string
    println(s"the oldest age is ${ages.max} ")

    lazy val x={
      println("lazy evaluation of x")
      20
    }

    val y={
      println("normal evaluation of y")
      3
    }

    println(s"${x}")



  }
}
