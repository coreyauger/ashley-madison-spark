package io.surfkit.driver

import com.typesafe.config.ConfigFactory
import io.surfkit.am.IntTypeMapping
import io.surfkit.data.Data
import org.apache.spark.{SparkConf, SparkContext}

import scala.Predef._
import scala.io._
import org.apache.spark.sql._
import scala.util._
import scala.concurrent.Await
import scala.concurrent.duration._
/**
 *
 * Created by suroot
 */

object Main extends App{

  override def main(args: Array[String]) {
    val config = ConfigFactory.load()

    val p = new java.io.PrintWriter("opento.txt")
    //When you create the SparkContext you tell it which jars to copy to the executors. Include the connector jar.
    val classes = Seq(
      getClass,                   // To get the jar with our own code.
      classOf[com.mysql.jdbc.Driver]  // To get the connector.
    )
    val jars = classes.map(_.getProtectionDomain().getCodeSource().getLocation().getPath())


    val conf = new SparkConf()
      .setAppName("Ashley Madison")
      .setMaster("spark://192.168.200.237:7077")
      .setJars(jars :+ "./target/scala-2.10/ashley-madison-spark_2.10-1.0.jar")      // send workers the driver..

    println("loading spark conf")
    val sc = new SparkContext(conf)
    // Read the data from MySql (JDBC)
    // Load the driver
    Class.forName("com.mysql.jdbc.Driver")

    println("get sql context")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val df = sqlContext.load("jdbc", Map(
      "url" -> "jdbc:mysql://localhost:3306/am",
      //"dbtable" -> "am_am_member",
      "dbtable" -> "am_tmp",            // small subset (10,000) records.
      "user" -> "root",
      "password" -> config.getString("password") ))


    // now.. lets get down and dirty.
    val amDf = df.select(
      "id",
      "city",
      "zip",
      "state",
      "latitude",
      "longitude",
      "country",
      "gender",
      "dob",
      "profile_ethnicity",
      "profile_weight",
      "profile_height",
      "profile_bodytype",
      "profile_smoke",
      "profile_drink",
      "profile_relationship",
      "pref_opento",
      "pref_turnsmeon",
      "pref_lookingfor"
    )
    // print the schema ..............
    println(amDf.schema)
    println("doing query..")

    //load the city and population data
    val worldCities = sc.textFile("../data/cities/worldcitiespop.txt")
      .map(_.split(","))
      .filter(_(0) != "Country")
      .filter(s => s(4) != "" && s(5) != "" && s(6) != "")
      .map(s => Data.City(s(0), s(1), s(2), s(3), s(4).toInt, s(5).toDouble, s(6).toDouble))
      .toDF()

    //worldCities.show(100)
    worldCities.registerTempTable("Cities")

    val women = amDf.filter("gender = 1").cache()
    val men = amDf.filter("gender = 2").cache()

    val menN = men.count()
    val womenN = women.count()
    println(s"Num Women ${womenN}")
    println(s"Num Men ${menN}")

    p.write(s"Num Women ${womenN}\n")
    p.write(s"Num Men ${menN} \n")
    p.write("\n\n")

    men.registerTempTable("Men")
    women.registerTempTable("Women")


    // open to totals...
    /*
    val menOpenTo = men.select(df("pref_opento")).map { r =>
      (r.getString(0).split("\\|").filter(_ != "").map(s => IntTypeMapping.prefOpenTo.get(s.toInt)).filter(_ != None).map(_.get).toSet)
    }
    val womenOpenTo = women.select(df("pref_opento")).map { r =>
      (r.getString(0).split("\\|").filter(_ != "").map(s => IntTypeMapping.prefOpenTo.get(s.toInt)).filter(_ != None).map(_.get).toSet)
    }
    IntTypeMapping.prefOpenTo.values.map { opento =>
      p.write(s"Men ${opento} totals\n")
      val menx = menOpenTo.filter(_.contains(opento)).count
      p.write(s"${menx} / ${menN}   ${(menx.toDouble/menN.toDouble)}\n\n")

      p.write(s"Women ${opento} totals\n")
      val womenx = womenOpenTo.filter(_.contains(opento)).count
      p.write(s"${womenx} / ${womenN}   ${(womenx.toDouble/womenN.toDouble)}\n\n\n")

    }
    */


    // Discovered that Lat,Lng in a LOT of cases is messed up.. (sign is inverted)
    // eg: "Vancouver"
    // (distance, (city, population, openTo, userLat, userLng, cityLat, cityLng)
    // List((245.80350473473368,(Vancouver,157517,|7|,49.25,123.1__,45.6388889,-122.6602778)), (246.25003299999997,(Vancouver,1837970,|7|,49.25,123.1___,49.25,-123.133333)))
    /*
    val menCityOpenTo = sqlContext.sql(
      """
        |SELECT a.id, a.city, a.pref_opento, a.latitude, a.longitude, b.Population, b.Latitude, b.Longitude
        |FROM Men a JOIN Cities b
        |ON lower(a.city) = lower(b.City)
        |WHERE a.latitude > 0
      """.stripMargin
    )

    menCityOpenTo.show(40)

    menCityOpenTo.map{ r =>
      (s"${r.getString(1)}-${r.getInt(0)}", (r.getString(1), r.getInt(5), r.getString(2), r.getDouble(3), r.getDouble(4), r.getDouble(6), r.getDouble(7)))
    }.groupByKey().map{ r =>
      r._2.map{
        case (city, population, openTo, userLat, userLng, cityLat, cityLng) =>
          val dist = Math.sqrt( Math.pow(userLat-cityLat,2)+Math.pow(userLng-cityLng,2)  )
          (dist,(city,population,openTo, userLat, userLng, cityLat, cityLng))
      }.toList.sortBy(_._1)

    }.take(50).foreach(println)
    */


    val menCityOpenTo = sqlContext.sql(
      """
        |SELECT a.city, a.pref_opento, b.Population
        |FROM Men a JOIN Cities b
        |ON lower(a.city) = lower(b.City)
        |ORDER BY b.Population
      """.stripMargin
    )

    val womenCityOpenTo = sqlContext.sql(
      """
        |SELECT a.city, a.pref_opento, b.Population
        |FROM Women a JOIN Cities b
        |ON lower(a.city) = lower(b.City)
        |ORDER BY b.Population
      """.stripMargin
    )



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


    p.close()
    sc.stop()

  }



}
