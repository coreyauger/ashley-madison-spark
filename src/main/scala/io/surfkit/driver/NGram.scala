package io.surfkit.driver

import io.surfkit.data.Data

import scala.Predef._

/**
 *
 * Created by Corey Auger
 */

object NGram extends App with SparkSetup{

  override def main(args: Array[String]) {

    import sqlContext.implicits._
    val p = new java.io.PrintWriter("./output/ngram.json")

    val women = sqlContext.sql(
      """
        |SELECT profile_caption, pref_lookingfor_abstract, city, state, country, gender, dob, profile_ethnicity
        |WHERE gender = 2
      """.stripMargin
    ).cache()
    val men = sqlContext.sql(
      """
        |SELECT profile_caption, pref_lookingfor_abstract, city, state, country, gender, dob, profile_ethnicity
        |WHERE gender = 1
      """.stripMargin
    ).cache()

    // TODO: profile_caption
    // TODO: pref_lookingfor_abstract

    val menN = men.count()
    val womenN = women.count()
    println(s"Num Women ${womenN}")
    println(s"Num Men ${menN}")

    p.write(s"Num Women ${womenN}\n")
    p.write(s"Num Men ${menN} \n")
    p.write("\n\n")

    men.registerTempTable("Men")
    women.registerTempTable("Women")


    val menProfileCaption = sqlContext.sql(
      """
        |SELECT a.profile_caption
        |FROM Men a
      """.stripMargin
    )

    val NGramSize = 5

    // TODO: ngram by city..
    // TODO: ngram by country
    // TODO: ngram by ethnicity
    // TODO: ngram by age

    // TODO: most used words NGram=1 remove stop words..

    men
      // TODO: remove punctuation
      // TODO: remove stop words ?
      .map(r => r.getString(0).toLowerCase.split(" ") )   // lower case + split
      .filter(r => r.length >= NGramSize)     // filter our small profiles
      .flatMap(r => r.sliding(NGramSize) )
      .map(r => (r.mkString(" "), 1))
      .reduceByKey((a,b) => a+b)
      .sortBy( _._2, false)
      .take(25).foreach(println)


    women
      // TODO: remove punctuation
      // TODO: remove stop words ?
      .map(r => r.getString(0).toLowerCase.split(" ") )   // lower case + split
      .filter(r => r.length >= NGramSize)     // filter our small profiles
      .flatMap(r => r.sliding(NGramSize) )
      .map(r => (r.mkString(" "), 1))
      .reduceByKey((a,b) => a+b)
      .sortBy( _._2, false)
      .take(25).foreach(println)


    println("################################################################################")

    p.close()
    sc.stop()

  }



}
