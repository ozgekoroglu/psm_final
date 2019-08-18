package org.processmining.scala.applications.bp.bpi_challenge.scenarios.parallel

import java.util.Date

import org.processmining.scala.applications.bp.bpi_challenge.types.ApplicationEvent
import org.processmining.scala.log.common.csv.parallel.CsvWriter
import org.processmining.scala.log.common.filtering.expressions.events.regex.impl.EventEx
import org.processmining.scala.log.common.filtering.expressions.traces._
import org.processmining.scala.log.common.unified.log.parallel.UnifiedEventLog

private object BPI17_FilterApplication_AllVsMultipleOffers_WorkflowOnly extends TemplateForBpi2017CaseStudyApplication {
  def main(args: Array[String]): Unit = {
    /*
    Question: find all traces where "O_Create Offer" occurred twice, the offer was
    finally accepted, and the application was validated successfully
    */

    // any application with two subsequent offers that was accepted, then validated with "completed", then ends with complete
    val subtracexOfferRepeated = EventEx("O_Create Offer") >->  EventEx("O_Create Offer")
    val subtracexOfferOnce = EventEx("O_Create Offer")
    val subtracexAcceptValidateCompletedEnd = EventEx("O_Accepted") >-> EventEx("W_Validate application").withValue("lifecycle:transition", "COMPLETE") >>  EventEx("End")
    val subtracexPendingCompleted = EventEx("A_Pending").withValue("lifecycle:transition", "COMPLETE")

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
      .filter((t contains subtracexAcceptValidateCompletedEnd) and (t contains subtracexPendingCompleted))
      //.project(EventEx("W_.*"),EventEx("A_Pending"),EventEx("A_Denied"),EventEx("A_Cancelled"))

    // repeated offers that were accepted but not validated
    val logOfferRepeatedOtherAccept = UnifiedEventLog
      .fromTraces(logOfferRepeated.traces
        .filter( tr => !(t contains subtracexAcceptValidateCompletedEnd).evaluate(tr) && (t contains subtracexPendingCompleted).evaluate(tr)))
      //.project(EventEx("W_.*"),EventEx("A_Pending"),EventEx("A_Denied"),EventEx("A_Cancelled"))
    // repeated offers that were not accepted and validated
    val logOfferRepeatedOtherReject = UnifiedEventLog
      .fromTraces(logOfferRepeated.traces
        .filter( tr => !(t contains subtracexAcceptValidateCompletedEnd).evaluate(tr) && !(t contains subtracexPendingCompleted).evaluate(tr)))
      //.project(EventEx("W_.*"),EventEx("A_Pending"),EventEx("A_Denied"),EventEx("A_Cancelled"))

    val logOfferOnceAccept =
      logOfferOnce
        .filter((t contains subtracexAcceptValidateCompletedEnd) and (t contains subtracexPendingCompleted))
        //.project(EventEx("W_.*"),EventEx("A_Pending"),EventEx("A_Denied"),EventEx("A_Cancelled"))

    // repeated offers that were accepted but not validated
    val logOfferOnceOtherAccept = UnifiedEventLog
      .fromTraces(logOfferOnce.traces
        .filter( tr => !(t contains subtracexAcceptValidateCompletedEnd).evaluate(tr) && (t contains subtracexPendingCompleted).evaluate(tr)))
      //.project(EventEx("W_.*"),EventEx("A_Pending"),EventEx("A_Denied"),EventEx("A_Cancelled"))
    // repeated offers that were not accepted and validated
    val logOfferOnceOtherReject = UnifiedEventLog
      .fromTraces(logOfferOnce.traces
        .filter( tr => !(t contains subtracexAcceptValidateCompletedEnd).evaluate(tr) && !(t contains subtracexPendingCompleted).evaluate(tr)))
      //.project(EventEx("W_.*"),EventEx("A_Pending"),EventEx("A_Denied"),EventEx("A_Cancelled"))

    val queryingDone = (new Date()).getTime

    println(s"Found ${logOfferRepeatedAccept.traces().size} traces with filter.")
    println(s"Found ${logOfferRepeatedOtherAccept.traces().size} traces with filter.")
    println(s"Found ${logOfferRepeatedOtherReject.traces().size} traces with filter.")
    println(s"Found ${logOfferOnceAccept.traces().size} traces with filter.")
    println(s"Found ${logOfferOnceOtherAccept.traces().size} traces with filter.")
    println(s"Found ${logOfferOnceOtherReject.traces().size} traces with filter.")

    CsvWriter.logToCsvLocalFilesystem(
      logOfferRepeatedAccept.filterByAttributeNames(ApplicationEvent.schema: _*),
      s"${caseStudyConfig.outDir}/BPI_challenge_workflow_offers_multiple_validate_accept_log_${csvExportHelper.timestamp2String((new Date()).getTime).replace(':','.')}.csv",
      csvExportHelper.timestamp2String,
      ApplicationEvent.schema: _*)

    CsvWriter.logToCsvLocalFilesystem(
      logOfferRepeatedOtherAccept.filterByAttributeNames(ApplicationEvent.schema: _*),
      s"${caseStudyConfig.outDir}/BPI_challenge_workflow_offers_multiple_accept_log_${csvExportHelper.timestamp2String((new Date()).getTime).replace(':','.')}.csv",
      csvExportHelper.timestamp2String,
      ApplicationEvent.schema: _*)

    CsvWriter.logToCsvLocalFilesystem(
      logOfferRepeatedOtherReject.filterByAttributeNames(ApplicationEvent.schema: _*),
      s"${caseStudyConfig.outDir}/BPI_challenge_workflow_offers_multiple_reject_log_${csvExportHelper.timestamp2String((new Date()).getTime).replace(':','.')}.csv",
      csvExportHelper.timestamp2String,
      ApplicationEvent.schema: _*)

    CsvWriter.logToCsvLocalFilesystem(
      logOfferOnceAccept.filterByAttributeNames(ApplicationEvent.schema: _*),
      s"${caseStudyConfig.outDir}/BPI_challenge_workflow_offers_one_validate_accept_log_${csvExportHelper.timestamp2String((new Date()).getTime).replace(':','.')}.csv",
      csvExportHelper.timestamp2String,
      ApplicationEvent.schema: _*)

    CsvWriter.logToCsvLocalFilesystem(
      logOfferOnceOtherAccept.filterByAttributeNames(ApplicationEvent.schema: _*),
      s"${caseStudyConfig.outDir}/BPI_challenge_workflow_offers_one_accept_log_${csvExportHelper.timestamp2String((new Date()).getTime).replace(':','.')}.csv",
      csvExportHelper.timestamp2String,
      ApplicationEvent.schema: _*)

    CsvWriter.logToCsvLocalFilesystem(
      logOfferOnceOtherReject.filterByAttributeNames(ApplicationEvent.schema: _*),
      s"${caseStudyConfig.outDir}/BPI_challenge_workflow_offers_one_reject_log_${csvExportHelper.timestamp2String((new Date()).getTime).replace(':','.')}.csv",
      csvExportHelper.timestamp2String,
      ApplicationEvent.schema: _*)

    val writingDone = (new Date()).getTime

    println(s"Reading took ${(parsingDone - appStartTime.getTime) / 1000} seconds.")
    println(s"Querying took ${(queryingDone - parsingDone) / 1000} seconds.")
    println(s"Writing took ${(writingDone - queryingDone) / 1000} seconds.")
  }
}
