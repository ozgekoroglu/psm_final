package org.processmining.scala.applications.mhs.acp.example

import org.processmining.scala.log.utils.common.csv.common.CsvImportHelper

class SplunkCsvImportHelper(timestampFormatterPattern: String, zoneIdString: String) extends CsvImportHelper(timestampFormatterPattern, zoneIdString) {

  override def extractTimestamp(str: String): Long = {
    try {
      val part = (str.split(" ")) (0)
      if (part.isEmpty) 0
      else {
        val cleanString = part
          .replace("T", " ")
          .replace("Z", "")
        val normalString = if (!cleanString.endsWith(".")) {
          val firstFraction = cleanString.lastIndexOf('.') + 1
          val ns = cleanString.substring(firstFraction)
          val ns9Digits = ns + (0 until 9 - ns.length).map(_ => "0").mkString("")
          cleanString.substring(0, firstFraction) + ns9Digits
        } else {
          cleanString + "000000000"
        }
        super.extractTimestamp(normalString)
      }
    } catch {
      case _ : Throwable => throw new IllegalArgumentException(str)
    }
  }
}
