package io.surfkit.driver

import com.typesafe.config.ConfigFactory
import io.surfkit.driver.Main._
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by suroot on 27/08/15.
 */
trait SparkSetup {

  val config = ConfigFactory.load()
  //When you create the SparkContext you tell it which jars to copy to the executors. Include the connector jar.
  val classes = Seq(
    getClass,                   // To get the jar with our own code.
    classOf[com.mysql.jdbc.Driver]  // To get the connector.
  )
  val jars = classes.map(_.getProtectionDomain().getCodeSource().getLocation().getPath())

  val conf = new SparkConf()
    .setAppName("Ashley Madison")
    .setMaster(config.getString("spark.master"))
    .set("spark.executor.memory", "8g")
    .setJars(jars ++
      Seq(
        "./target/scala-2.10/ashley-madison-spark_2.10-1.0.jar",
        "./lib/quasiquotes_2.10-2.0.0.jar",
        "./lib/upickle_2.10-0.3.6.jar"
      )
    )      // send workers the driver..


  println("loading spark conf")
  val sc = new SparkContext(conf)
  // Read the data from MySql (JDBC)
  // Load the driver
  Class.forName("com.mysql.jdbc.Driver")

  println("get sql context")
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)

  val df = sqlContext.load("jdbc", Map(
    "url" -> config.getString("database"),
    "dbtable" -> "am_am_member",
    //"dbtable" -> "am_tmp",            // small subset (10,000) records.
    "user" -> config.getString("dbuser"),
    "password" -> config.getString("password") ))
  .registerTempTable("members")

}
