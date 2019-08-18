package org.processmining.scala.applications.mhs.bhs.bpi

import org.processmining.scala.applications.mhs.bhs.t3.eventsources.BpiCsvImportHelper
import org.processmining.scala.log.common.types.{Id, Timestamp}
import org.processmining.scala.log.common.utils.common.types.Event

@deprecated
private[bhs] trait Lpc {
  val lpc: String
}

@deprecated
private[bhs] trait Surname {
  val surname: String
}

@deprecated
private[bhs] object PackageInfoEvent {
  def apply(bpiCsvHelper: BpiCsvImportHelper, idIndex: Int, eventTsIndex: Int, attributeIndex: Int)(a: Array[String]): Event = {
    Event(a(idIndex),
      if (a.length <= attributeIndex) "" else a(attributeIndex),
      bpiCsvHelper.extractOracleTimestampMs(a(eventTsIndex)))
  }
}

// Represents a package info record with LPC
@deprecated
private[bhs] case class PackageInfoLpcEntry(id: String, lpc: String, timestamp: Long)
  extends Serializable with Id with Lpc with Timestamp

@deprecated
private[bhs] object PackageInfoLpcEntry{
  def apply(bpiCsvHelper: BpiCsvImportHelper, idIndex: Int, lpcIndex: Int, eventTsIndex: Int)(a: Array[String]): PackageInfoLpcEntry =
    PackageInfoLpcEntry(a(idIndex), a(lpcIndex), bpiCsvHelper.extractOracleTimestampMs(a(eventTsIndex)))
}

// Represents a package info record with 2 attributes (besides pid and timestamp)
@deprecated
private[bhs] case class PackageInfoWithLpcWithProcessDefinitionName(id: String, timestamp: Long, lpc: Option[String], process: Option[String]) extends Serializable {
  override def toString: String = String.format(
    """%s "%s": "%s" "%s" """,
    timestamp.toString, id, if (lpc == None) lpc else lpc.get, if (process == None) process else process.get)
}
