package exampleBasics

import org.apache.spark.{SparkConf, SparkContext}


object CountingGivenWord {
  def main(args: Array[String]): Unit = {

    val conf= new SparkConf().setAppName("Countisparkng no of given word").setMaster("local")
    val sc= new SparkContext(conf)

    val inpFile=sc.textFile(getClass.getResource("/sparkdoc.txt").getPath)

    println("Please enter the word you wish to count from the input file: ")
    val word=scala.io.StdIn.readLine()

    val cnt= inpFile.flatMap(lines => lines.split(" ")).filter(rec => rec.equals(word)).count()

    println( word + " exists in the input file " + cnt + " times.")

    sc.stop()

  }

}
