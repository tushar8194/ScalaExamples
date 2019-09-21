package exampleBasics

import org.apache.spark.{SparkConf, SparkContext}

object Test {
  def main(args: Array[String]): Unit = {

    val conf=new SparkConf().setMaster("local").setAppName("testing syntax")
    val sc= new SparkContext(conf)


    /*val rdd1 = sc.parallelize(Seq(("Spark",78),("Hive",95),("spark",15),("HBase",25),("spark",39),("BigData",78),("spark",49)))
    rdd1.countByKey*/

    val fl=sc.textFile(getClass.getResource("/transactions").getPath)
    val a= fl.map { l =>
       l.split(",")


    }

println(s"${a}")
  }
}