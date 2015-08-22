package io.surfkit.driver

import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkConf, SparkContext}

import scala.Predef._
import scala.concurrent.Await
import scala.concurrent.duration._
/**
 *
 * Created by suroot
 */

object Main extends App{

  override def main(args: Array[String]) {
   val config = ConfigFactory.load()

    val conf = new SparkConf().setAppName("Ashley Madison").setMaster("local[4]").set("spark.executor.memory","8g")
    val sc = new SparkContext(conf)
    // Read the data from MySql (JDBC)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val df = sqlContext.load("jdbc", Map(
      "url" -> "jdbc:mysql://localhost:3306/am",
      "dbtable" -> "am_am_member",
      "user" -> "root",
      "password" -> config.getString("password") ))

    // print the schema ..............
    println(df.schema)

    // now.. lets get down and dirty.
    val amDf = df.select(
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
    println("doing query..")

    amDf.take(10).foreach(println)

    sc.stop()

  }



}
