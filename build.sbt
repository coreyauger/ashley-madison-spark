import sbt.Keys._

name := "ashley-madison-spark"

version := "1.0"

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

libraryDependencies ++= deps

scalaVersion := "2.10.4"

lazy val deps = {
  val akkaV = "2.3.9"
  val akkaStreamV = "1.0-RC3"
  Seq(
    "mysql" % "mysql-connector-java" % "5.1.+" % "compile",
    "com.lihaoyi" %% "upickle" % "0.3.6",
    "org.scalamacros" %% s"quasiquotes" % "2.0.0" % "provided",
    "org.apache.spark" %% "spark-core" % "1.4.1",
    "org.apache.spark" %% "spark-sql" % "1.4.1"
  )
}

addCommandAlias("email",  "run-main io.surfkit.driver.EmailMetrics")

addCommandAlias("ngram",  "run-main io.surfkit.driver.NGram")

addCommandAlias("main",  "run-main io.surfkit.driver.Main")

addCommandAlias("states",  "run-main io.surfkit.driver.DiscoverState")



assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs @ _*)         => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
  case "application.conf"                            => MergeStrategy.concat
  case "unwanted.txt"                                => MergeStrategy.discard
  case x =>
    //val oldStrategy = (assemblyMergeStrategy in assembly).value
    //oldStrategy(x)
    MergeStrategy.first
}
