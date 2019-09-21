package exampleBasics

import org.apache.spark.{SparkConf, SparkContext}


object UsingAccumulators {
  def main(args: Array[String]): Unit = {

    val conf= new SparkConf().setAppName("Log Analysis").setMaster("local")
    val sc = new SparkContext(conf)

    val badPkts= sc.accumulator(0,"Bad Requests")
    val zeroValueSales=sc.accumulator(0,"Zero Value Sales")
    val missingFields=sc.accumulator(0,"Missing fields")
    val blankLines=sc.accumulator(0,"Blank Lines")

    sc.textFile(getClass.getResource("/logFile.log").getPath,4)
      .foreach{line=>
                if (line.length()==0 ) blankLines+=1
                else if (line.contains("Bad data packet")) badPkts +=1
                else {

                      val fields=line.split("\t")
                      if (fields.length!=4)
                        {
                          println("Missing Field Line : " + line)
                          missingFields+=1
                        }
                      else if (fields(3).toFloat==0) zeroValueSales+=1
                     }

                 }


    println("Log analysis Counters:")
    println(s"\tBad Packets=${badPkts.value}")
    println(s"\tZero Value Sales=${zeroValueSales.value}")
    println(s"\tMissing Fields=${missingFields.value}")
    println(s"\tBlank Lines=${blankLines.value}")



  }

}
