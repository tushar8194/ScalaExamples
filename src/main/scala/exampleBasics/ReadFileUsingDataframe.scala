package exampleBasics

import org.apache.spark.sql.SparkSession


object ReadFileUsingDataframe {
  def main(args: Array[String]): Unit = {

    //create SparkSession
val sprk= SparkSession.builder().appName("Reading File using Dataframe").master("local").getOrCreate()

    //read file
    val filePath=getClass.getResource("/FileForDF.txt").getPath


    //create dataframe
    val df= sprk.read.options(Map("inferSchema" -> "false", "sep" -> "|","header" -> "true")).csv(filePath)

df.show(true)


  }
}
