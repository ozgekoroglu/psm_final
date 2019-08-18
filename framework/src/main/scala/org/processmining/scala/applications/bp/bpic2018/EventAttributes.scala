package org.processmining.scala.applications.bp.bpic2018

object EventAttributes {
  val Doctype = "doctype"
  val Subprocess = "subprocess"
  val Activity = "activity"

  val Timestamp = "time:timestamp"

  val Resource = "org:resource"
  val Success = "success"
  val Note = "note"
  val ConceptName = "concept:name"

  val SetOfMainAttributes = Set(Doctype, Subprocess, Activity, Timestamp, Resource, Success, Note)
  val SetOfMinAttributes = Set(Doctype, Subprocess, Activity, Timestamp)


}
