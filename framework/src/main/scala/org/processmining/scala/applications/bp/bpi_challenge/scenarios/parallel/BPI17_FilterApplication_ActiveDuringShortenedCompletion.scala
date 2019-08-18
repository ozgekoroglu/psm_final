package org.processmining.scala.applications.bp.bpi_challenge.scenarios.parallel

import java.util.Date

import org.processmining.scala.applications.bp.bpi_challenge.scenarios.parallel.BPI17_FilterApplication_AllVsMultipleOffers_ApplicationOnly.{caseStudyConfig, csvExportHelper}
import org.processmining.scala.applications.bp.bpi_challenge.types.ApplicationEvent
import org.processmining.scala.log.common.csv.parallel.CsvWriter
import org.processmining.scala.log.common.filtering.expressions.events.regex.impl.EventEx
import org.processmining.scala.log.common.filtering.expressions.traces._
import org.processmining.scala.log.common.unified.event.UnifiedEvent
import org.processmining.scala.log.common.unified.log.parallel.UnifiedEventLog
import org.processmining.scala.log.common.unified.log.common.UnifiedEventLog.UnifiedTrace
import org.processmining.scala.log.common.unified.trace.UnifiedTraceId

import scala.collection.parallel.ParSeq

private object BPI17_FilterApplication_ActiveDuringShortenedCompletion extends TemplateForBpi2017CaseStudyApplication {
  def main(args: Array[String]): Unit = {
    /*
    Question: find all traces that were active during the phase of "shortened completion" happening
    */

    val logFull = logApplicationsWithArtificialStartsStops // fullOuterJoin logSegmentsWithClassifiedDurations
    println(s"Originally ${logFull.traces().size} traces")

    val parsingDone = (new Date()).getTime

    val logShortenedCompletion = logFull
      .project(EventEx("W_Shortened completion "))

    println(s"Found ${logShortenedCompletion.traces().size} traces matching filter")

    val intervalsShortenedCompletionActive = logShortenedCompletion.traces().map(
      tr => { tr match {
        case (id,events:List[UnifiedEvent]) => {
          val startTime = events.head.timestamp
          val endTime = events.last.timestamp
          (startTime,endTime)
        }
      }}
    )

    def inIntervals(ts: Long, intervals: ParSeq[(Long,Long)] ): Boolean = {
      for (interval <- intervals) if (interval._1 <= ts && ts <= interval._2) return true
      return false
    }

    def activeDuringIntervals(id: UnifiedTraceId, events: List[UnifiedEvent], intervals: ParSeq[(Long,Long)]): Boolean = {
      for (event <- events) if (inIntervals(event.timestamp, intervals)) return true
      return false
    }

    def activeDuringShortenedCompletion(trace: UnifiedTrace): Boolean = {
      activeDuringIntervals(trace._1, trace._2, intervalsShortenedCompletionActive)
    }

    val logActiveDuringShortened = UnifiedEventLog.fromTraces(
      logFull.traces().filter(activeDuringShortenedCompletion)
    )

    val queryingDone = (new Date()).getTime

    println(s"Found ${logActiveDuringShortened.traces().size} traces with filter.")
//    println(s"Found ${logOfferRepeatedOther.traces().size} traces with filter.")
//    println(s"Found ${logOfferOnceAccept.traces().size} traces with filter.")
//    println(s"Found ${logOfferOnceOther.traces().size} traces with filter.")
//    println(s"Found ${logProjected.traces().size} traces with projection.")

    CsvWriter.logToCsvLocalFilesystem(
      logActiveDuringShortened.filterByAttributeNames(ApplicationEvent.schema: _*),
      s"${caseStudyConfig.outDir}/BPI_challenge_application_activeDuringShortenedCompletion_log_${csvExportHelper.timestamp2String((new Date()).getTime).replace(':','.')}.csv",
      csvExportHelper.timestamp2String,
      ApplicationEvent.schema: _*)

    val writingDone = (new Date()).getTime

    println(s"Reading took ${(parsingDone - appStartTime.getTime) / 1000} seconds.")
    println(s"Querying took ${(queryingDone - parsingDone) / 1000} seconds.")
    println(s"Writing took ${(writingDone - queryingDone) / 1000} seconds.")
  }
}