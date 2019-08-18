import sbt._
import Keys._

name := "framework"

version := "1.1.0"

scalaVersion := "2.11.8"

val sparkVersion = "2.2.1"

resolvers += Resolver.mavenLocal

sources in(Compile, doc) ~= (_ filter (x => !x.getName.contains("Test")))

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)
libraryDependencies += "org.apache.commons" % "commons-collections4" % "4.0"
libraryDependencies += "org.apache.commons" % "commons-math3" % "3.6.1"
libraryDependencies += "org.ini4j" % "ini4j" % "0.5.4"
libraryDependencies += "com.thoughtworks.xstream" % "xstream" % "1.4.10"
libraryDependencies += "org.xes-standard" % "openxes" % "2.23"
libraryDependencies += "org.xes-standard" % "openxes-xstream" % "2.23"
libraryDependencies += "org.deckfour" % "Spex" % "1.0"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.4" % Test
libraryDependencies += "com.opencsv" % "opencsv" % "4.1"
libraryDependencies += "org.jfree" % "jfreechart" % "1.0.17"
libraryDependencies += "com.google.guava" % "guava" % "15.0"
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.7.2"





//retrieveManaged := true

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case x => MergeStrategy.last
}

mainClass in assembly := Some("org.processmining.scala.viewers.spectrum.view.PerformanceSpectrum")
