package io.surfkit.driver

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

    val conf = new SparkConf().setAppName("Ashley Madison").setMaster("local[4]").set("spark.executor.memory","8g")
    val sc = new SparkContext(conf)
    // Read the data from MySql (JDBC)

    sc.stop()

  }



}
