package io.surfkit.driver

import io.surfkit.am.IntTypeMapping
import io.surfkit.data.Data

import scala.Predef._

/**
 *
 * Created by Corey Auger
 */

object DiscoverStates extends App with SparkSetup{

  override def main(args: Array[String]) {

    import sqlContext.implicits._

    val p = new java.io.PrintWriter("./output/opento.json")

    val state = sqlContext.sql(
      """
        |SELECT state, city, country
        |FROM members
      """.stripMargin
    )

    //load the city and population data
    val worldCities = sc.textFile("../data/cities/worldcitiespop.txt")
      .map(_.split(","))
      .filter(_(0) != "Country")
      .filter(s => s(4) != "" && s(5) != "" && s(6) != "")
      .map(s => Data.City(s(0), s(1), s(2), s(3), s(4).toInt, s(5).toDouble, s(6).toDouble))
      .toDF()

    //worldCities.show(100)
    worldCities.registerTempTable("Cities")

    val join = sqlContext.sql(
      """
        |SELECT a.state, a.city, a.country, b.Country, b.Region, b.Latitude, b.Longitude, b.Population
        |FROM Men a JOIN Cities b
        |ON lower(a.city) = lower(b.City)
        |ORDER BY a.country
      """.stripMargin
    )

    // Group By Country-Region-City
    // Display some test output.
    join.distinct.show(100)
    //join.distinct.foreach(println)


    p.close()
    sc.stop()

  }



}
