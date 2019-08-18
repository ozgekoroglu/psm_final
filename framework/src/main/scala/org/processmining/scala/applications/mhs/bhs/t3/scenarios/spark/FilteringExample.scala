package org.processmining.scala.applications.mhs.bhs.t3.scenarios.spark

import java.util.Date

import org.apache.spark.storage.StorageLevel
import org.processmining.scala.applications.mhs.bhs.t3.eventsources.{BpiImporter, TaskReportSchema}
import org.processmining.scala.log.common.csv.spark.CsvWriter
import org.processmining.scala.log.common.enhancment.segments.spark.DurationSegmentProcessor
import org.processmining.scala.log.common.filtering.expressions.events.regex.impl.EventEx
import org.processmining.scala.log.common.filtering.expressions.traces._
import org.processmining.scala.log.common.filtering.expressions.traces.impl.TraceExpression


private object FilteringExample extends T3Session(
  "d:/logs/20160719",
  "d:/tmp/july_19_v.Sept26_15m_filtering/",
  "19-07-2016 12:00:00",
  "20-07-2016 22:00:00",
  900000 //time window
) {

  def main(args: Array[String]): Unit = {

    logSegmentsWithArtificialStartsStops.persist(StorageLevel.MEMORY_AND_DISK)
    val (segmentDf, statDf, durationProcessor) = DurationSegmentProcessor(processorConfig,
      config.percentile)

    //val classifiedDurationEvents = durationProcessor.getClassifiedSegments()

    val classifiedDurationLog = durationProcessor.getClassifiedSegmentLog()

    val log = withArtificialStartsStops(logMovements fullOuterJoin logTasks fullOuterJoin classifiedDurationLog)


    /*
    Problem: extract a log of bag traces that on 19.07.16 between 17:00 and 20:00 GMT had task "RouteToMC"
    on the pre-sorter 50. Sub-traces should be extracted in the following way: from entering the presorter 50 till exiting
    (but including Scanners, MCs and Level4 Exit)
     */

    val incidenTimeStart = dateHelper.extractTimestamp("19-07-2016 17:00:00")
    val incidenTimeEnd = dateHelper.extractTimestamp("19-07-2016 20:00:00")
    val preSorter50 = EventEx("7750\\.\\d+\\.\\d+")
    //    val mc50 = TrackingReport("7731\\.\\d+\\.\\d+")
    //    val scanners = TrackingReport("773[5-8]\\.\\d+\\.\\d+")
    //    val start = TrackingReport("Start")
    //    val end = TrackingReport("End")
    val taskRouteToMCIncidentTime = EventEx("RouteToMC")
      .during(incidenTimeStart, incidenTimeEnd)

    val preSorter50IncidentTime = preSorter50
      .during(incidenTimeStart, incidenTimeEnd)

    val matchingFilter =
      (preSorter50IncidentTime >-> taskRouteToMCIncidentTime) | (taskRouteToMCIncidentTime >-> preSorter50IncidentTime)

    val subtraceFilter = preSorter50 >-> taskRouteToMCIncidentTime >-> preSorter50
    //
    //    println(matchingFilter)
    //    println(subtraceFilter)
    //
    //    val filteredLog = log
    //      .maxEventsPerTrace(300)
    //      .matchTraces(matchingFilter)
    //      .subtraces(subtraceFilter)
    //      .select(TrackingReportEvent.Type)

    val t = TraceExpression()

    val filteredLog = log // could be Spark or single-machine impl. (parallel collections)
      .map(t trimToTimeframe(from, until))
      .filter((t length(0, 300)) and (t contains matchingFilter))
      .map(t subtrace subtraceFilter)

    CsvWriter.logToCsvLocalFilesystem(filteredLog.filterByAttributeNames(TaskReportSchema.AttrNameTaskClass),
      s"${config.outDir}/filtered_log_q6.csv",
      csvExportHelper.timestamp2String,
      TaskReportSchema.Schema.toArray: _*)

    println(s"""Pre-processing took ${(new Date().getTime - appStartTime.getTime) / 1000} seconds.""")
  }
}
