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

    import sqlContext.implicits._
    val p = new java.io.PrintWriter("./output/ngram.json")

    val women = sqlContext.sql(
      """
        |SELECT profile_caption, pref_lookingfor_abstract, city, state, country, dob, profile_ethnicity
        |FROM members
        |WHERE gender = 2
      """.stripMargin
    )
    val men = sqlContext.sql(
      """
        |SELECT profile_caption, pref_lookingfor_abstract, city, state, country, dob, profile_ethnicity
        |FROM members
        |WHERE gender = 1
      """.stripMargin
    )


    val stream : InputStream = getClass.getResourceAsStream("/stopwords.txt")
    val stopWords = scala.io.Source.fromInputStream( stream ).getLines.map(_.trim).toSet

    // TODO: ngram by city.. ??
    // TODO: ngram by age


    val menDf = men
      .map(r =>
        ( // tokenize, convert to lowercase, and remove stop words.
          (r.getString(0).toLowerCase.split(" ")).filter(w => !stopWords.contains(w)),      // profile_caption
          (r.getString(0).toLowerCase.split(" ")).filter(w => !stopWords.contains(w)),      // pref_lookingfor_abstract
          r.getString(2),                             // city
          r.getInt(3),                                // state
          r.getInt(4),                                // country
          r.getDate(5),                               // dob
          r.getInt(6)                                 // profile_ethnicity
        )
      )
    menDf.cache()     // cache in memory

    val womenDf = women
      .map(r =>
      ( // tokenize, convert to lowercase, and remove stop words.
        (r.getString(0).toLowerCase.split(" ")).filter(w => !stopWords.contains(w)),      // profile_caption
        (r.getString(0).toLowerCase.split(" ")).filter(w => !stopWords.contains(w)),      // pref_lookingfor_abstract
        r.getString(2),                             // city
        r.getInt(3),                                // state
        r.getInt(4),                                // country
        r.getDate(5),                               // dob
        r.getInt(6)                                 // profile_ethnicity
        )
      )

    womenDf.cache()     // cache in memory

    val MaxNGramSize = 6

    val menN = menDf.count()
    val womenN = womenDf.count()
    println(s"Num Women ${womenN}")
    println(s"Num Men ${menN}")

    (1 to MaxNGramSize).foreach{ ngramLen =>
      Seq( (menDf, "Men"), (womenDf, "Women") ).foreach { case (rdd, sex) =>
        val profileCaption = rdd.filter(r => r._1.length >= ngramLen) // filter out less then ngrams
          .flatMap(r =>
          r._1.sliding(ngramLen).map { ngram =>
            (
              ngram.mkString(" "),
              (
                r._3, // city
                r._4, // state
                r._5, // country
                r._6, // dob
                r._7 // profile_ethnicity
                )
              )
          }
          )
        profileCaption.cache() // cache this RDD

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

        // group by country...
        val country = profileCaption
          .map(r => ((r._2._3, r._1), 1) )
          .reduceByKey((a, b) => a + b)
          .sortBy(_._2, false)
          .map(r =>
            Data.NGram(
              ngram = r._1._2,
              groupBy = "Country",
              groupByValue = r._1._1.toString,
              count = r._2
            )
          ).take(250)
        // write json file
        val ngramStatsByCountry = NGramStats(title = s"${sex} NGram-${ngramLen} by Country", total = total, sex = sex, data = country)
        val jsonCountry = new java.io.PrintWriter(s"./output/ngram${sex}Country-${ngramLen}.json")
        jsonCountry.write(upickle.default.write(ngramStatsByCountry))
        jsonCountry.close()


        // group by state...
        val state = profileCaption
          .map(r => ((r._2._2, r._1), 1) )
          .reduceByKey((a, b) => a + b)
          .sortBy(_._2, false)

          .map(r =>
            Data.NGram(
              ngram = r._1._2,
              groupBy = "State",
              groupByValue = r._1._1.toString,
              count = r._2
            )
          ).take(250)
        // write json file
        val ngramStatsByState = NGramStats(title = s"${sex} NGram-${ngramLen} by State", total = total, sex = sex, data = state)
        val jsonState = new java.io.PrintWriter(s"./output/ngram${sex}State-${ngramLen}.json")
        jsonState.write(upickle.default.write(ngramStatsByState))
        jsonState.close()


        // group by ethnicity...
        val ethnicity = profileCaption
          .map(r => ((r._2._5, r._1), 1) )
          .reduceByKey((a, b) => a + b)
          .sortBy(_._2, false)
          .map(r =>
            Data.NGram(
              ngram = r._1._2,
              groupBy = "Ethnicity",
              groupByValue = r._1._1.toString,
              count = r._2
            )
          ).take(250)
        // write json file
        val ngramStatsByEthnicity = NGramStats(title = s"${sex} NGram-${ngramLen} by Ethnicity", total = total, sex = sex, data = ethnicity)
        val jsonEthnicity = new java.io.PrintWriter(s"./output/ngram${sex}Ethnicity-${ngramLen}.json")
        jsonEthnicity.write(upickle.default.write(ngramStatsByEthnicity))
        jsonEthnicity.close()

        profileCaption.unpersist()    // uncache RDD
      }

    }


    p.close()
    sc.stop()

  }



}
