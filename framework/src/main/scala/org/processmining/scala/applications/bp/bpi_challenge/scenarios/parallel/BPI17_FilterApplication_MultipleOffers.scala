package org.processmining.scala.applications.bp.bpi_challenge.scenarios.parallel

import java.util.Date

import org.processmining.scala.applications.bp.bpi_challenge.types.ApplicationEvent
import org.processmining.scala.log.common.csv.parallel.CsvWriter
import org.processmining.scala.log.common.filtering.expressions.events.regex.impl.EventEx
import org.processmining.scala.log.common.filtering.expressions.traces._

private object BPI17_FilterApplication_MultipleOffers extends TemplateForBpi2017CaseStudyApplication {
  def main(args: Array[String]): Unit = {

    /*
    Question: find all traces where "O_Create Offer" occurred twice, the offer was
    finally accepted, and the application was validated successfully
    */

    // any application with two subsequent offers that was accepted, then validated with "completed", then ends with complete
    val subtracexOfferRepeatedValidateCompletedEnd = EventEx("O_Create Offer") >->  EventEx("O_Create Offer") >-> EventEx("O_Accepted") >-> EventEx("W_Validate application").withValue("lifecycle:transition", "COMPLETE") >>  EventEx("End")

    val logFull = logApplicationsWithArtificialStartsStops // fullOuterJoin logSegmentsWithClassifiedDurations
    println(s"Originally ${logFull.traces().size} traces")

    val logFiltered = logFull
      .filter((t contains subtracexOfferRepeatedValidateCompletedEnd))


    println(s"Found ${logFiltered.traces().size} traces.")

    CsvWriter.logToCsvLocalFilesystem(
      logFiltered.filterByAttributeNames(ApplicationEvent.schema: _*),
      s"${caseStudyConfig.outDir}/BPI_challenge_all_filtered(multiple_offers)_log_${csvExportHelper.timestamp2String((new Date()).getTime).replace(':','.')}.csv",
      csvExportHelper.timestamp2String,
      ApplicationEvent.schema: _*)

    println(s"Pre-processing took ${(new Date().getTime - appStartTime.getTime) / 1000} seconds.")
  }
}
