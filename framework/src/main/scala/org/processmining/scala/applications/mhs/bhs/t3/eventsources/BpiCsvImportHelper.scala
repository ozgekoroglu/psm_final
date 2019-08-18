package org.processmining.scala.applications.mhs.bhs.t3.eventsources

import java.time.LocalDateTime

import org.processmining.scala.log.utils.common.csv.common.CsvImportHelper

class BpiCsvImportHelper(timestampFormatterPattern: String,
                         zoneIdString: String
                  ) extends CsvImportHelper(timestampFormatterPattern, zoneIdString) {

  /**
    * Parses ISO-8601-incompatible Oracle timestamps into Epoch milliseconds
    */
  def extractOracleTimestampMs(str: String): Long = {
    val ldtTimestamp = LocalDateTime.parse(
      str
        .replaceFirst("JAN", "Jan")
        .replaceFirst("FEB", "Feb")
        .replaceFirst("MAR", "Mar")
        .replaceFirst("APR", "Apr")
        .replaceFirst("MAY", "May")
        .replaceFirst("JUN", "Jun")
        .replaceFirst("JUL", "Jul")
        .replaceFirst("AUG", "Aug")
        .replaceFirst("SEP", "Sep")
        .replaceFirst("OCT", "Oct")
        .replaceFirst("NOV", "Nov")
        .replaceFirst("DEC", "Dec"),
      timestampFormatter)
    ldtTimestamp.atZone(zoneId).toInstant().toEpochMilli()
  }
}

object BpiCsvImportHelper{
   //val DefaultTimestampPattern = "dd-MMM-yy HH.mm.ss.SSSSSS"
   val DefaultTimestampPattern = "yyyy/MM/dd HH:mm:ss.SSS"
}