// The simplest possible sbt build file is just one line:

scalaVersion := "2.11.12"


name := "ModelMonitoring"
organization := "ml.dhs"
version := "0.0.1"
val sparkVersion = "2.3.0"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.scalatest" %% "scalatest" % "3.0.5" % "test"
)

