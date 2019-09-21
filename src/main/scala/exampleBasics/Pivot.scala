package exampleBasics


import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame


object Pivot {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.set("spark.driver.host","localhost")
    val spark= SparkSession.builder().config(conf).appName("Pivot Dataframe").master("local").getOrCreate()

    val path = getClass.getResource("/country.csv").getPath
    val df = spark.read.option("delimiter",",").option("header","true").csv(path)

    import org.apache.spark.sql.functions._
    val countries = List("UK", "US", "Can")

    val countryValue = udf{(countryToCheck: String, countryInRow: String, value: Long) =>
      if(countryToCheck == countryInRow) value else 0
    }

    val countryFuncs = countries.map{country => (dataFrame: DataFrame) =>
     dataFrame.withColumn(country, countryValue(lit(country), df("tag"), df("value"))) }

    val dfWithCountry = Function.chain(countryFuncs)(df)

    dfWithCountry.show()
//    +---+---+-----+---+---+---+
//    | id|tag|value| US| UK|Can|
//    +---+---+-----+---+---+---+
//    |  1| US|   50| 50|  0|  0|
//    |  1| UK|  100|  0|100|  0|
//    |  1|Can|  125|  0|  0|125|
//    |  2| US|   75| 75|  0|  0|
//    |  2| UK|  150|  0|150|  0|
//    |  2|Can|  175|  0|  0|175|
//    +---+---+-----+---+---+---+


    val dfWithCountries = Function.chain(countryFuncs)(df).drop("tag").drop("value")

    dfWithCountries.show()
    //    +---+---+---+---+
    //    | id| US| UK|Can|
    //    +---+---+---+---+
    //    |  1| 50|  0|  0|
    //    |  1|  0|100|  0|
    //    |  1|  0|  0|125|
    //    |  2| 75|  0|  0|
    //    |  2|  0|150|  0|
    //    |  2|  0|  0|175|
    //    +---+---+---+---+

    dfWithCountries.groupBy("id").sum(countries: _*).show

//    +---+-------+-------+--------+
//    | id|sum(US)|sum(UK)|sum(Can)|
//    +---+-------+-------+--------+
//    |  1|     50|    100|     125|
//    |  2|     75|    150|     175|
//    +---+-------+-------+--------+


  }

}
