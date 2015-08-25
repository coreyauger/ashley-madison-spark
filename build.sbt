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
    "org.apache.spark" %% "spark-core" % "1.4.1",
    "org.apache.spark" %% "spark-sql" % "1.4.1"
  )
}

