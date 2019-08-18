package org.processmining.scala.applications.mhs.acp.example

case class Entry1(val PalOrderId: String, val TrayTsuId: String, val PalTimeStamp: Long)

case class Entry2(val TrayTsuId: String, val EtrTime: Long)

case class Entry3(val OrderId: String, val TrayTsuId: String, val FirstEntryAfterPal: Long, val ContentType: String)

case class Entry4(val LastAsrExitTime: Long, val AsrOrderId: String, val TrayTsuId: String)

object Entry1 {
  def apply(tsConverter: String => Long, row: Array[String]) = new Entry1(row(0), row(1), tsConverter(row(2)))
}

object Entry2 {
  def apply(tsConverter: String => Long, row: Array[String]) = new Entry2(row(0), tsConverter(row(1)))
}

object Entry3 {
  def apply(tsConverter: String => Long, row: Array[String]) = new Entry3(row(0), row(1), tsConverter(row(2)), if(row.length==4) row(3) else "")
}

object Entry4 {
  def apply(tsConverter: String => Long, row: Array[String]) = new Entry4(tsConverter(row(0)), row(1), row(2))
}
