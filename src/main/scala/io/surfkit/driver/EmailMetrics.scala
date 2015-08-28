package io.surfkit.driver


import io.surfkit.data.Data.{EmailCount, EmailStats}

import scala.Predef._

/**
 *
 * Created by Corey Auger
 */

object EmailMetrics extends App with SparkSetup{

  override def main(args: Array[String]) {

    sqlContext.load("jdbc", Map(
      "url" -> config.getString("database"),
      "dbtable" -> "aminno_member_email",
      "user" -> config.getString("dbuser"),
      "password" -> config.getString("password") ))
      .registerTempTable("email")

    val email = sqlContext.sql(
      """
        |SELECT email
        |FROM email
      """.stripMargin
    )
    //println(email.schema)
    //email.take(1).foreach(println)

    val validEmail = email.map(_.getString(0)).filter(_.contains("@"))
    validEmail.cache()
    val total = validEmail.count()
    println(s"Total email addresses ${total}")

    val topDomains =
      validEmail
        .map(s => (s.substring(s.indexOf('@')),1))  // get the domain..
        .reduceByKey((a,b) => a+b)

    val totalDomains = topDomains.count()
    println(s"NUmber of domains ${totalDomains}")

    val topCounts = topDomains.sortBy( _._2, false).take(250).map { d =>
      println(d)
      EmailCount(
        domain = d._1,
        count = d._2
      )
    }

    val json = EmailStats(
      total = total,
      totalDomains = totalDomains,
      counts = topCounts
    )

    // write data to json file
    val p = new java.io.PrintWriter("./output/email.json")
    p.write(upickle.default.write(json))
    p.close()

    sc.stop()

  }



}
