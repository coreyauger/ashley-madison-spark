package io.surfkit.data

/**
 * Created by suroot on 23/08/15.
 */
object Data {
  case class City(Country:String,City:String,AccentCity:String,Region:String,Population:Int,Latitude:Double,Longitude:Double)

  case class EmailStats(total:Long, totalDomains:Long, counts:Seq[(String, Int)])

  case class NGram(ngram:String, groupBy:String, groupByValue:String, count:Long)
  case class NGramStats(title:String, total:Long, sex:String, data:Seq[NGram])
}
