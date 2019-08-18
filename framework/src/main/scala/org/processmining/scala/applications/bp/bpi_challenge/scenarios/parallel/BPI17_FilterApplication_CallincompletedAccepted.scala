package org.processmining.scala.applications.bp.bpi_challenge.scenarios.parallel

import java.time.Duration
import java.util.Date

import org.processmining.scala.applications.bp.bpi_challenge.types.ApplicationEvent
import org.processmining.scala.log.common.csv.parallel.CsvWriter
import org.processmining.scala.log.common.enhancment.segments.parallel.{DurationSegmentProcessor, SegmentProcessorConfig}
import org.processmining.scala.log.common.filtering.expressions.events.regex.impl.EventEx
import org.processmining.scala.log.common.filtering.expressions.traces._
import org.processmining.scala.log.common.unified.event.{CommonAttributeSchemas, UnifiedEvent}
import org.processmining.scala.log.common.unified.log.parallel.UnifiedEventLog
import org.processmining.scala.log.common.unified.log.common.UnifiedEventLog.UnifiedTrace
import org.processmining.scala.log.common.unified.trace.UnifiedTraceId

import scala.collection.parallel.ParSeq

private object BPI17_FilterApplication_CallincompletedAccepted extends TemplateForBpi2017CaseStudyApplication {
  def main(args: Array[String]): Unit = {
    /*
    Question: find all traces that were active during the phase of "shortened completion" happening
    */

    val processorConfigApplications = SegmentProcessorConfig(logApplicationsDfrEventsWithArtificialStartsStops,
      caseStudyConfig.timestamp1Ms,
      caseStudyConfig.timestamp2Ms,
      caseStudyConfig.twSize)
    val (segments, stat, durationProcessor) = DurationSegmentProcessor(processorConfigApplications)
    val logSegmentsWithClassifiedDurations =  durationProcessor.getClassifiedSegmentLog()

    val logFull = logApplicationsWithArtificialStartsStops fullOuterJoin logSegmentsWithClassifiedDurations
    println(s"Originally ${logFull.traces().size} traces")

    val parsingDone = (new Date()).getTime

    val evxFastSegment =
      EventEx("W_Call incomplete files:O_Accepted")
        .withRange(CommonAttributeSchemas.AttrNameDuration, Duration.ofDays(0).toMillis, Duration.ofDays(4).toMillis-1)
    val evxSlowSegment =
      EventEx("W_Call incomplete files:O_Accepted")
        .withRange(CommonAttributeSchemas.AttrNameDuration, Duration.ofDays(4).toMillis, Duration.ofDays(30).toMillis)

    val logFastSegment = logFull.filter((t contains evxFastSegment))
    val logSlowSegment = logFull.filter((t contains evxSlowSegment))

    val queryingDone = (new Date()).getTime

    println(s"Found ${logFastSegment.traces().size} traces with filter.")
    println(s"Found ${logSlowSegment.traces().size} traces with filter.")
//    println(s"Found ${logOfferOnceAccept.traces().size} traces with filter.")
//    println(s"Found ${logOfferOnceOther.traces().size} traces with filter.")
//    println(s"Found ${logProjected.traces().size} traces with projection.")

    CsvWriter.logToCsvLocalFilesystem(
      logFastSegment.filterByAttributeNames(ApplicationEvent.schema: _*),
      s"${caseStudyConfig.outDir}/BPI_challenge_incomplete_accept_fast_segment_log_${csvExportHelper.timestamp2String((new Date()).getTime).replace(':','.')}.csv",
      csvExportHelper.timestamp2String,
      ApplicationEvent.schema: _*)

    CsvWriter.logToCsvLocalFilesystem(
      logSlowSegment.filterByAttributeNames(ApplicationEvent.schema: _*),
      s"${caseStudyConfig.outDir}/BPI_challenge_incomplete_accept_slow_segment_log_${csvExportHelper.timestamp2String((new Date()).getTime).replace(':','.')}.csv",
      csvExportHelper.timestamp2String,
      ApplicationEvent.schema: _*)

    val writingDone = (new Date()).getTime

    println(s"Reading took ${(parsingDone - appStartTime.getTime) / 1000} seconds.")
    println(s"Querying took ${(queryingDone - parsingDone) / 1000} seconds.")
    println(s"Writing took ${(writingDone - queryingDone) / 1000} seconds.")
  }
}
