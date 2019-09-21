import java.io.FileWriter

import org.apache.spark.{SparkConf, SparkContext}


object LocationOnSale {
  def main(args: Array[String]) {


    val conf = new SparkConf().setAppName("SparkJoins").setMaster("local")
    val sc = new SparkContext(conf)

    val transactionsIn = sc.textFile("/Users/z002gh2/naina/GITREPO/WorkspaceLearning/LearningScala/src/main/resources/transactions")
    val usersIn = sc.textFile("/Users/z002gh2/naina/GITREPO/WorkspaceLearning/LearningScala/src/main/resources/Users")


    val newTransactionsPair = transactionsIn.map { t =>
      val p = t.split(",")
      (p(2), p(1))
    }
    newTransactionsPair.collect().foreach(println)

    val newUsersPair = usersIn.map { t =>
      val p = t.split(",")
      (p(0), p(3))
    }
    newUsersPair.collect().foreach(println)
    var jn = newTransactionsPair.leftOuterJoin(newUsersPair).values.distinct
    val result=jn.countByKey()
    val rslts=sc.parallelize(result.toSeq).map(t => (t._1.toString, t._2.toString))

    val file = rslts.
      map { case (key, value) => (key+","+value) }
      file.foreach(println)

    val headerRDD = sc.parallelize(Seq("Product name,no of locations"));
    val finalFile=headerRDD++(file)
    finalFile.foreach(println)

    val output = "/Users/z002gh2/naina/GITREPO/WorkspaceLearning/LearningScala/src/main/resources/output"
    finalFile.repartition(1). saveAsTextFile(output)

    sc.stop()
  }

}
