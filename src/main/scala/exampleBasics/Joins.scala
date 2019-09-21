package exampleBasics

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object Joins {
  def main(args: Array[String]): Unit = {

    val sprk= SparkSession.builder().appName("Reading File using Dataframe").master("local").getOrCreate()
    val conf= new SparkConf()
    conf.setAppName("Word Cound")
    conf.setMaster("local")

    // this is used to implicitly convert an RDD to a DataFrame.
    import sprk.sqlContext.implicits._


    val left = Seq((0, "zero","1234"), (1, "one","3456")).toDF("id", "left","col")
    val right = Seq((0, "zero","345"), (2, "two","894"), (3, "three","49270")).toDF("id", "right","col3")
    left.join(right, left("id") === right("id") && left("left") === right("right")).
      select(left("id"),left("left"),left("col"),right("col3")
    ).show

  }
}
