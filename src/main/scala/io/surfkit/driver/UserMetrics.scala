package io.surfkit.driver

import io.surfkit.data.Data

import scala.Predef._

/**
 *
 * Created by Corey Auger
 */

object UserMetrics extends App with SparkSetup{

  override def main(args: Array[String]) {

    import sqlContext.implicits._

    val p = new java.io.PrintWriter("./output/usermetrics.json")

    val women = sqlContext.sql(
      """
        |SELECT city, state, country, gender, dob, profile_ethnicity, profile_weight, profile_height, profile_bodytype, profile_smoke, profile_drink, profile_relationship
        |WHERE gender = 1
      """.stripMargin
    )
    val men = sqlContext.sql(
      """
        |SELECT city, state, country, gender, dob, profile_ethnicity, profile_weight, profile_height, profile_bodytype, profile_smoke, profile_drink, profile_relationship
        |WHERE gender = 2
      """.stripMargin
    )

    women.cache()
    men.cache()


    Seq((men,"Men"),(women,"Women")).foreach{ case (rdd, sex) =>
      rdd.toDF().describe().show()      // print the mean, min, max and stddev
    }

    p.close()
    sc.stop()

  }



}
