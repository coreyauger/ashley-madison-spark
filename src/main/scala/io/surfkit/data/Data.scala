package io.surfkit.data

/**
 * Created by suroot on 23/08/15.
 */
object Data {
  case class City(Country:String,City:String,AccentCity:String,Region:String,Population:Int,Latitude:Double,Longitude:Double)


  case class EmailCount(domain:String, count:Long)
  case class EmailStats(total:Long, totalDomains:Long, counts:Seq[EmailCount])
}
