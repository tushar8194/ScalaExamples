package exampleBasics

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object ReadJson {
  def main(args: Array[String]): Unit = {
    val spark=  SparkSession.builder().master("local").getOrCreate()

    val path=getClass.getResource("/EmployeeDetails.json").getPath


    val empDf=spark.read.json(path)

    empDf.show()

    val newEmpDf= empDf.withColumn("hire_date", from_unixtime(unix_timestamp(col("hire_date"),"mm/dd/yyyy"),"yyyy-mm-dd"))
    newEmpDf.coalesce(1).select("deptno","hire_date","empno").write.option("header","true").csv("/Users/z002gh2/naina/GITREPO/WorkspaceLearning/LearningScala/src/main/resources/EmpDtls.csv")




  }
}
