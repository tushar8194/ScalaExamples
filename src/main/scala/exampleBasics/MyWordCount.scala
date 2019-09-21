package exampleBasics

import org.apache.spark.{SparkConf,SparkContext}


object MyWordCount {
  def main(args: Array[String]): Unit = {

    //create a spark configuration parameter
    val conf= new SparkConf()
    conf.setAppName("Word Cound")
    conf.setMaster("local")

    //create a spark context object
    val sc = new SparkContext(conf)

    //load the text file
    val file=sc.textFile(getClass.getResource("/sparkdoc.txt").getPath)

    //counting the words in the text file
    val wordCount=file.flatMap(lines => lines.split(" ")).map(rec => (rec,1)).reduceByKey(_+_)
    wordCount.collect().foreach(println)

    sc.stop()
  }
}

