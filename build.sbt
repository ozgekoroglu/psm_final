import sbt._
import Keys._

name := "psm"
version := "1.1.0"
scalaVersion := "2.11.8"
val sparkVersion = "2.2.1"
resolvers += Resolver.mavenLocal
sources in(Compile, doc) ~= (_ filter (x => !x.getName.contains("Test")))

lazy val commonSettings = Seq(
  organization := "org.processmining",
  version := s"$version-SNAPSHOT",
  scalaVersion := "2.11.8"
)

lazy val perf_spec = project
  .settings(
    commonSettings
  )

lazy val classifiers_outlier = project
  .dependsOn(perf_spec)
  .settings(
    commonSettings
  )

/*
lazy val classifiers = project
  .dependsOn(perf_spec)
  .settings(
    commonSettings
  )
*/

lazy val ppm = project
  .dependsOn(perf_spec)
  .settings(
    commonSettings,
    libraryDependencies ++= Seq(
      "org.apache.commons" % "commons-collections4" % "4.0",
      "org.apache.commons" % "commons-math3" % "3.6.1",
      "org.ini4j" % "ini4j" % "0.5.4",
      "com.thoughtworks.xstream" % "xstream" % "1.4.10",
      "org.xes-standard" % "openxes" % "2.23",
      "org.xes-standard" % "openxes-xstream" % "2.23",
      "org.deckfour" % "Spex" % "1.0",
      "org.scalatest" %% "scalatest" % "3.0.4" % Test,
      "com.opencsv" % "opencsv" % "4.1",
      "org.jfree" % "jfreechart" % "1.0.17"
    )
  )


lazy val framework = project
  .dependsOn(perf_spec)
  .settings(
    commonSettings,
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion,
      "org.apache.commons" % "commons-collections4" % "4.0",
      "org.apache.commons" % "commons-math3" % "3.6.1",
      "org.ini4j" % "ini4j" % "0.5.4",
      "com.thoughtworks.xstream" % "xstream" % "1.4.10",
      "org.xes-standard" % "openxes" % "2.23",
      "org.xes-standard" % "openxes-xstream" % "2.23",
      "org.deckfour" % "Spex" % "1.0",
      "org.scalatest" %% "scalatest" % "3.0.4" % Test,
      "com.opencsv" % "opencsv" % "4.1",
      "org.jfree" % "jfreechart" % "1.0.17"
    )
  )

lazy val sim_ein = project
  .dependsOn(framework, ppm)
  .settings(
    commonSettings,
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion,
      "org.apache.commons" % "commons-collections4" % "4.1",
      "org.apache.commons" % "commons-math3" % "3.6.1",
      "org.ini4j" % "ini4j" % "0.5.4",
      "com.thoughtworks.xstream" % "xstream" % "1.4.10",
      "org.xes-standard" % "openxes" % "2.23",
      "org.xes-standard" % "openxes-xstream" % "2.23",
      "org.deckfour" % "Spex" % "1.0",
      "org.scalatest" %% "scalatest" % "3.0.4" % Test,
      "com.opencsv" % "opencsv" % "4.1",
      "org.jfree" % "jfreechart" % "1.0.17"
    )
  )


assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case x => MergeStrategy.last
}