package io.surfkit.driver

import java.io.InputStream

import io.surfkit.data.Data
import io.surfkit.data.Data.NGramStats
import org.apache.spark.rdd.RDD

import scala.Predef._
import io.surfkit.data._
/**
 *
 * Created by Corey Auger
 */

object NGram extends App with SparkSetup{

  override def main(args: Array[String]) {


    val p = new java.io.PrintWriter("./output/ngram.json")

    val women = sqlContext.sql(
      """
        |SELECT profile_caption , city, state, country, profile_ethnicity
        |FROM members
        |WHERE gender = 1
      """.stripMargin
    )
    val men = sqlContext.sql(
      """
        |SELECT profile_caption, city, state, country, profile_ethnicity
        |FROM members
        |WHERE gender = 2
      """.stripMargin
    )
    // TODO: pref_lookingfor_abstract


    val stream : InputStream = getClass.getResourceAsStream("/stopwords.txt")
    val stopWords = scala.io.Source.fromInputStream( stream )
      .getLines
      .map(_.trim.toLowerCase.replaceAll("[^\\w\\s]",""))      // remove punctuation
      .toSet

    import sqlContext.implicits._
    // TODO: ngram by city.. ??
    // TODO: ngram by age


    val menDf = men
      .map(r =>
        ( // tokenize, convert to lowercase, and remove stop words.
          ((if(r.isNullAt(0))"" else r.getString(0)).toLowerCase.replaceAll("[^\\w\\s]","").split(" "))
            .map(_.trim).filter(_ != "")
            .filter(w => !stopWords.contains(w)),      // profile_caption
         // ((if(r.isNullAt(1))"" else r.getString(1)).toLowerCase.replaceAll("[^\\w\\s]","").split(" "))
         //   .map(_.trim).filter(_ != "")
         //   .filter(w => !stopWords.contains(w)),      // pref_lookingfor_abstract
          if(r.isNullAt(1)) "" else r.getString(1),                             // city
          if(r.isNullAt(2)) 0 else r.getInt(2),                                // state
          if(r.isNullAt(3)) 0 else r.getInt(3),                                // country
         // r.getDate(5),                               // dob
          if(r.isNullAt(4)) 0 else r.getInt(4)                                 // profile_ethnicity
        )
      )

    val womenDf = women
      .map(r =>
      ( // tokenize, convert to lowercase, and remove stop words.
        ((if(r.isNullAt(0))"" else r.getString(0)).toLowerCase.replaceAll("[^\\w\\s]","").split(" "))
          .map(_.trim).filter(_ != "")
          .filter(w => !stopWords.contains(w)),      // profile_caption
       // ((if(r.isNullAt(1))"" else r.getString(1)).toLowerCase.replaceAll("[^\\w\\s]","").split(" "))
       //   .map(_.trim).filter(_ != "")
       //   .filter(w => !stopWords.contains(w)),      // pref_lookingfor_abstract
        if(r.isNullAt(1)) "" else r.getString(1),                             // city
        if(r.isNullAt(2)) 0 else r.getInt(2),                                // state
        if(r.isNullAt(3)) 0 else r.getInt(3),                                // country
      //  r.getDate(5),                               // dob
        if(r.isNullAt(4)) 0 else r.getInt(4)                                 // profile_ethnicity
        )
      )

    val MaxNGramSize = 4

//    val menN = menDf.count()
//    val womenN = womenDf.count()
//    println(s"Num Women ${womenN}")
//    println(s"Num Men ${menN}")

    Seq( (menDf, "Men"),(womenDf, "Women") ).foreach { case (rdd, sex) =>
      Seq(MaxNGramSize).foreach{ ngramLen =>
      //(1 to MaxNGramSize).foreach{ ngramLen =>
         println("********************************************************************************************************")
          println(s"CALC NGRAM $ngramLen for $sex")
          val profileCaption = rdd.filter(r => r._1.length >= ngramLen) // filter out less then ngrams
            .flatMap(r =>
            r._1.sliding(ngramLen).map { ngram =>
              (
                ngram.mkString(" "),
                (
                  r._2, // city
                  r._3, // state
                  r._4, // country
                  r._5 // profile_ethnicity
                  )
                )
            }
            )
          profileCaption.cache() // cache this RDD

          println("********************************************************************************************************")
          println(s"DO COUNTS $ngramLen for $sex")
          val counts = profileCaption
            .map(r => (r._1, 1))
            .reduceByKey((a, b) => a + b)
            .sortBy(_._2, false)
            .map(r =>
              Data.NGram(
                ngram = r._1,
                groupBy = "",
                groupByValue = "",
                count = r._2
              )
            ) .take(250)
          // write json file
          val total = profileCaption.count()
          val ngramStats = NGramStats(title = s"${sex} NGram-${ngramLen}", total = total, sex = sex, data = counts)
          val json = new java.io.PrintWriter(s"./output/ngram${sex}Total-${ngramLen}.json")
          json.write(upickle.default.write(ngramStats))
          json.close()

          // group by state...
          println("********************************************************************************************************")
          println(s"CALC STATE $ngramLen for $sex")
          val state = profileCaption
            .map(r => ((r._2._2, r._1), 1) )
            .reduceByKey((a, b) => a + b)
            .groupByKey()
            .map( r => (r._1, r._2.toList.sorted(Ordering[Int].reverse)) )
            .flatMap(r =>
              r._2.take(25).map{ count =>
                Data.NGram(
                  ngram = r._1._2,
                  groupBy = "State",
                  groupByValue = r._1._1.toString,
                  count = count
                )
              }
            ).take(150)
          // write json file
          val ngramStatsByState = NGramStats(title = s"${sex} NGram-${ngramLen} by State", total = total, sex = sex, data = state)
          val jsonState = new java.io.PrintWriter(s"./output/ngram${sex}State-${ngramLen}.json")
          jsonState.write(upickle.default.write(ngramStatsByState))
          jsonState.close()


          // group by ethnicity...
          println("********************************************************************************************************")
          println(s"CALC ETH $ngramLen for $sex")
          val ethnicity = profileCaption
            .map(r => ((r._2._4, r._1), 1) )
            .reduceByKey((a, b) => a + b)
            .groupByKey()
            .map( r => (r._1, r._2.toList.sorted(Ordering[Int].reverse)) )
            .flatMap(r =>
              r._2.take(25).map{ count =>
                Data.NGram(
                  ngram = r._1._2,
                  groupBy = "State",
                  groupByValue = r._1._1.toString,
                  count = count
              )
            }
            ).take(150)
          // write json file
          val ngramStatsByEthnicity = NGramStats(title = s"${sex} NGram-${ngramLen} by Ethnicity", total = total, sex = sex, data = ethnicity)
          val jsonEthnicity = new java.io.PrintWriter(s"./output/ngram${sex}Ethnicity-${ngramLen}.json")
          jsonEthnicity.write(upickle.default.write(ngramStatsByEthnicity))
          jsonEthnicity.close()

          println("********************************************************************************************************")
          println(s"UNCACHE $ngramLen for $sex")
          profileCaption.unpersist()    // uncache RDD
        }

    }


    p.close()
    sc.stop()

  }



}
