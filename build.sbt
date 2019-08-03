import scala.io.Source
scalaVersion := "2.11.12"

name := "modelmonitor"
organization := "ml.dhs"

val lines=Source.fromFile("VERSION").getLines.toArray
version := lines(0)+"-SNAPSHOT"

fork in Test := true
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")
parallelExecution in Test := false

val sparkVersion = "2.4.0"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.scalatest" %% "scalatest" % "3.0.5" % "test",
  "com.holdenkarau" %% "spark-testing-base" % s"${sparkVersion}_0.12.0" % "test",
  "org.json4s" %% "json4s-native" % "3.6.7"
)

