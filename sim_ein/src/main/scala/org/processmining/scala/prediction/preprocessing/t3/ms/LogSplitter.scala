package org.processmining.scala.prediction.preprocessing.t3.ms

import java.time.{Instant, ZoneId, ZonedDateTime}
import java.util.Date

import org.processmining.scala.applications.mhs.bhs.t3.scenarios.spark.T3Session
import org.processmining.scala.log.common.unified.log.spark.UnifiedEventLog
import org.processmining.scala.log.utils.common.csv.common.CsvExportHelper

private object LogSplitterFileNameSettings {
  val inDir = "C://data//full_converted//test" // initial logs
  val outDir = "C:/ms/logs/test" // separated logs

  //Provide here an interval of days
  val from = "20190531"
  val until = "20190601"

  val outFileName = s"$outDir/$from.csv"

  def predicate(s: String): Boolean = {
    s.substring(0, 8) >= from && s.substring(0, 8) < until
  }

}

private object LogSplitter extends T3Session(
  LogSplitterFileNameSettings.inDir,
  LogSplitterFileNameSettings.outDir,
  "27-09-2017 00:00:00", //does not matter here
  "01-04-2018 00:00:00", //does not matter here
  900000, //time window //does not matter here
  LogSplitterFileNameSettings.predicate
) {


  def main(args: Array[String]): Unit = {
    val log = logMovementsWoTimeFrameTrim
    export(log, LogSplitterFileNameSettings.outFileName)
    println(s"""Pre-processing took ${(new Date().getTime - appStartTime.getTime) / 1000} seconds.""")
  }
}

private object LogSplitterLpcAsCaseId extends T3Session(
  LogSplitterFileNameSettings.inDir,
  LogSplitterFileNameSettings.outDir,
  "27-09-2017 00:00:00", //does not matter here
  "01-04-2018 00:00:00", //does not matter here
  900000, //time window //does not matter here
  LogSplitterFileNameSettings.predicate
) {

  private def timestamp2DayOfYear(timestamp: Long): Int =
    ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.of(CsvExportHelper.AmsterdamTimeZone)).getDayOfYear


  def main(args: Array[String]): Unit = {
    val log = logMovementsWoTimeFrameTrim
    val pidTolpcDictionary = bpiImporter.loadLpcFromCsv(spark).collect().toMap
    val lpcEventPairs = log
      .events()
      .map(x => {
        val lpc = pidTolpcDictionary.get(x._1.id)
        val event = x._2
        val caseId = if (lpc.isDefined) s"D${timestamp2DayOfYear(event.timestamp)}_${lpc.get}" else s"PID_${x._1.id}"
        (caseId, event)
      })
    export(UnifiedEventLog.create(lpcEventPairs), LogSplitterFileNameSettings.outFileName)
    println(s"""Pre-processing took ${(new Date().getTime - appStartTime.getTime) / 1000} seconds.""")
  }
}


private object Example1 extends T3Session(
  LogSplitterFileNameSettings.inDir,
  LogSplitterFileNameSettings.outDir,
  "27-09-2017 00:00:00", //does not matter here
  "01-04-2018 00:00:00", //does not matter here
  900000, //time window //does not matter here
  LogSplitterFileNameSettings.predicate
) {

  private def timestamp2DayOfYear(timestamp: Long): Int =
    ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.of(CsvExportHelper.AmsterdamTimeZone)).getDayOfYear


  def main(args: Array[String]): Unit = {
    val log = logMovementsWoTimeFrameTrim
    val pidTolpcDictionary = bpiImporter.loadLpcFromCsv(spark).collect().toMap
    val lpcEventPairs = log
      .events()
      .map(x => {
        val lpc = pidTolpcDictionary.get(x._1.id)
        val event = x._2
        val caseId = if (lpc.isDefined) s"D${timestamp2DayOfYear(event.timestamp)}_${lpc.get}" else s"PID_${x._1.id}"
        (caseId, event)
      })
    export(UnifiedEventLog.create(lpcEventPairs), LogSplitterFileNameSettings.outFileName)
    println(s"""Pre-processing took ${(new Date().getTime - appStartTime.getTime) / 1000} seconds.""")
  }
}

