package org.processmining.scala.applications.bp.bpi_challenge.scenarios.parallel

import java.time.Duration
import java.util.Date

import org.processmining.scala.applications.bp.bpi_challenge.types.OfferEvent
import org.processmining.scala.log.common.csv.parallel.CsvWriter
import org.processmining.scala.log.common.filtering.expressions.events.regex.impl.EventEx
import org.processmining.scala.log.common.filtering.expressions.traces._

private object BPI17_EnrichLogWithAmountClass extends TemplateForBpi2017CaseStudy {
  def main(args: Array[String]): Unit = {

    val logSegmentsWithClassifiedDurations =  durationProcessor.getClassifiedSegmentLog()
    val logEnrichedWithStartStop = logOffersWithArtificialStartsStops fullOuterJoin logSegmentsWithClassifiedDurations
    println(s"Originally ${logEnrichedWithStartStop.traces().size} traces")

    CsvWriter.logToCsvLocalFilesystem(
      logEnrichedWithStartStop.filterByAttributeNames("offeredAmount", "offeredAmountClass"),
      s"${caseStudyConfig.outDir}/BPI_challenge_enriched_log_${csvExportHelper.timestamp2String((new Date()).getTime).replace(':','.')}.csv",
      csvExportHelper.timestamp2String,
      "offeredAmount", "offeredAmountClass")

    println(s"Pre-processing took ${(new Date().getTime - appStartTime.getTime) / 1000} seconds.")
  }
}
