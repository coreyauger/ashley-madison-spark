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
        |WHERE gender = 2
      """.stripMargin
    ).cache()
    val men = sqlContext.sql(
      """
        |SELECT city, state, country, gender, dob, profile_ethnicity, profile_weight, profile_height, profile_bodytype, profile_smoke, profile_drink, profile_relationship
        |WHERE gender = 1
      """.stripMargin
    ).cache()

    val menN = men.count()
    val womenN = women.count()

    p.write(s"Num Women ${womenN}\n")
    p.write(s"Num Men ${menN} \n")
    p.write("\n\n")

    men.registerTempTable("Men")
    women.registerTempTable("Women")


    /*

    val menCityOpenTo2 = menCityOpenTo.map { r =>
      (r.getString(0), r.getString(1).split("\\|").filter(_ != "").map(s => IntTypeMapping.prefOpenTo.get(s.toInt)).filter(_ != None).map(_.get).toSet, r.getInt(2))
    }
    val womenCityOpenTo2 = womenCityOpenTo.map { r =>
      (r.getString(0), r.getString(1).split("\\|").filter(_ != "").map(s => IntTypeMapping.prefOpenTo.get(s.toInt)).filter(_ != None).map(_.get).toSet, r.getInt(2))
    }


    IntTypeMapping.prefOpenTo.values.take(5).map { opento =>
      p.write(s"Men Open to ${opento}\n")
      menCityOpenTo2.filter(_._2.contains(opento)).map(r => ((r._1,r._3), 1) ).reduceByKey((a,b) => a+b).map(s => (s._1._1,s._1._2.toDouble, s._2 )).sortBy( _._3, false).take(20).foreach(s => p.write(s.toString+ "\n"))
      //menCityOpenTo2.filter(_._2.contains(opento)).map(r => (r._1, 1) ).reduceByKey((a,b) => a+b).map(s => (s._1, s._2 )).sortBy( _._2, false).take(20).foreach(s => p.write(s.toString+ "\n"))
      p.write("\n")
      p.write(s"Women Open to ${opento}\n")
      womenCityOpenTo2.filter(_._2.contains(opento)).map(r => ((r._1,r._3), 1) ).reduceByKey((a,b) => a+b).map(s => (s._1._1,s._1._2.toDouble, s._2 )).sortBy( _._3, false).take(20).foreach(s => p.write(s.toString+ "\n"))
      p.write("\n\n")
    }
    */


    p.close()
    p.close()
    sc.stop()

  }



}
