package org.processmining.scala.applications.bp.bpi_challenge.scenarios.parallel

import java.util.Date

import org.processmining.scala.applications.bp.bpi_challenge.scenarios.parallel.BPI17_FilterApplication_CreateValidateFourEyes.appStartTime
import org.processmining.scala.applications.bp.bpi_challenge.types.ApplicationEvent
import org.processmining.scala.log.common.csv.parallel.CsvWriter
import org.processmining.scala.log.common.filtering.expressions.events.regex.impl.EventEx
import org.processmining.scala.log.common.filtering.expressions.traces._
import org.processmining.scala.log.common.unified.log.parallel.UnifiedEventLog

private object BPI17_FilterApplication_AllVsMultipleOffers_ApplicationOnly extends TemplateForBpi2017CaseStudyApplication {
  def main(args: Array[String]): Unit = {
    /*
    Question: find all traces where "O_Create Offer" occurred twice, the offer was
    finally accepted, and the application was validated successfully
    */

    // any application with two subsequent offers that was accepted, then validated with "completed", then ends with complete
    val subtracexOfferRepeated = EventEx("O_Create Offer") >->  EventEx("O_Create Offer")
    val subtracexOfferOnce = EventEx("O_Create Offer")
    val subtracexAcceptValidateCompletedEnd = EventEx("O_Accepted") >-> EventEx("W_Validate application").withValue("lifecycle:transition", "COMPLETE") >>  EventEx("End")

    val logFull = logApplicationsWithArtificialStartsStops // fullOuterJoin logSegmentsWithClassifiedDurations
    println(s"Originally ${logFull.traces().size} traces")

    val parsingDone = (new Date()).getTime

    // filter for repeated offers
    val logOfferRepeated = logFull.filter((t contains subtracexOfferRepeated))
    // filter for single offers
    val logOfferOnce = UnifiedEventLog
      .fromTraces(logFull.traces
          .filter( tr => ((t contains subtracexOfferOnce).evaluate(tr) && !(t contains subtracexOfferRepeated).evaluate(tr)))
      )

    // repeated offers that were accepted and validated
    val logOfferRepeatedAccept =
      logOfferRepeated
      .filter((t contains subtracexAcceptValidateCompletedEnd))
      .project(EventEx("A_.*"))

    // repeated offers that were not accepted and validated
    val logOfferRepeatedOther = UnifiedEventLog
      .fromTraces(logOfferRepeated.traces
        .filter( tr => !(t contains subtracexAcceptValidateCompletedEnd).evaluate(tr)))
      .project(EventEx("A_.*"))

    // single offers that were accepted and validated
    val logOfferOnceAccept =
      logOfferOnce
      .filter((t contains subtracexAcceptValidateCompletedEnd))
      .project(EventEx("A_.*"))

    // single offers that were not accepted and validated
    val logOfferOnceOther = UnifiedEventLog
      .fromTraces(logOfferOnce.traces
        .filter( tr => !(t contains subtracexAcceptValidateCompletedEnd).evaluate(tr)))
      .project(EventEx("A_.*"))

    val logProjected = logFull
        .project(EventEx("A_.*"))   // project to "A_*" events only

    val queryingDone = (new Date()).getTime

    println(s"Found ${logOfferRepeatedAccept.traces().size} traces with filter.")
    println(s"Found ${logOfferRepeatedOther.traces().size} traces with filter.")
    println(s"Found ${logOfferOnceAccept.traces().size} traces with filter.")
    println(s"Found ${logOfferOnceOther.traces().size} traces with filter.")
    println(s"Found ${logProjected.traces().size} traces with projection.")

    CsvWriter.logToCsvLocalFilesystem(
      logOfferRepeatedAccept.filterByAttributeNames(ApplicationEvent.schema: _*),
      s"${caseStudyConfig.outDir}/BPI_challenge_application_offers_multiple_accept_log_${csvExportHelper.timestamp2String((new Date()).getTime).replace(':','.')}.csv",
      csvExportHelper.timestamp2String,
      ApplicationEvent.schema: _*)

    CsvWriter.logToCsvLocalFilesystem(
      logOfferRepeatedOther.filterByAttributeNames(ApplicationEvent.schema: _*),
      s"${caseStudyConfig.outDir}/BPI_challenge_application_offers_multiple_other_log_${csvExportHelper.timestamp2String((new Date()).getTime).replace(':','.')}.csv",
      csvExportHelper.timestamp2String,
      ApplicationEvent.schema: _*)

    CsvWriter.logToCsvLocalFilesystem(
      logOfferOnceAccept.filterByAttributeNames(ApplicationEvent.schema: _*),
      s"${caseStudyConfig.outDir}/BPI_challenge_application_offers_one_accept_log_${csvExportHelper.timestamp2String((new Date()).getTime).replace(':','.')}.csv",
      csvExportHelper.timestamp2String,
      ApplicationEvent.schema: _*)

    CsvWriter.logToCsvLocalFilesystem(
      logOfferOnceOther.filterByAttributeNames(ApplicationEvent.schema: _*),
      s"${caseStudyConfig.outDir}/BPI_challenge_application_offers_one_other_log_${csvExportHelper.timestamp2String((new Date()).getTime).replace(':','.')}.csv",
      csvExportHelper.timestamp2String,
      ApplicationEvent.schema: _*)

    CsvWriter.logToCsvLocalFilesystem(
      logProjected.filterByAttributeNames(ApplicationEvent.schema: _*),
      s"${caseStudyConfig.outDir}/BPI_challenge_application_full_log_${csvExportHelper.timestamp2String((new Date()).getTime).replace(':','.')}.csv",
      csvExportHelper.timestamp2String,
      ApplicationEvent.schema: _*)

    val writingDone = (new Date()).getTime

    println(s"Reading took ${(parsingDone - appStartTime.getTime) / 1000} seconds.")
    println(s"Querying took ${(queryingDone - parsingDone) / 1000} seconds.")
    println(s"Writing took ${(writingDone - queryingDone) / 1000} seconds.")
  }
}
