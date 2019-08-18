import sbt._
import Keys._

name := "classifier_blockage"

version := "1.1.0"

scalaVersion := "2.11.8"


resolvers += Resolver.mavenLocal




//retrieveManaged := true
/*
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.last
}
*/
//mainClass in assembly := Some("org.processmining.scala.viewers.spectrum.view.PerformanceSpectrum")
