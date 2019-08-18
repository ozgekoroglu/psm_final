package org.processmining.scala.applications.bp.bpi_challenge.scenarios.parallel

import java.util.Date

import org.processmining.scala.applications.bp.bpi_challenge.types.ApplicationEvent
import org.processmining.scala.log.common.csv.parallel.CsvWriter
import org.processmining.scala.log.common.filtering.expressions.events.regex.impl.EventEx
import org.processmining.scala.log.common.filtering.expressions.events.variables.EventVar
import org.processmining.scala.log.common.filtering.expressions.traces._
import org.processmining.scala.log.common.unified.log.parallel.UnifiedEventLog

private object BPI17_FilterApplication_MultipleOffers_FallingVsGrowingAmounts extends TemplateForBpi2017CaseStudyApplication {

  // check if argument can be cast into a Double value
  def isDouble(x: Any): Boolean = {
    val d = (x match {
      case Some(y:Double) => true
      case y:Double => true
      case _ => false})
    return d;
  }

  // cast into a Double value, assuming isDouble(x) holds
  def getAsDouble(x: Any): Double = {
    assert(isDouble(x))
    val d = (x match {
      case Some(y:Double) => y
      case y:Double => y})
    return d;
  }

  def main(args: Array[String]): Unit = {

    /* Question: Find all traces where the amount offered to the custerom falls/grows and create a log
     * of cases with falling/growing/non-changing offered amounts */

    val attrOfferedAmount = ApplicationEvent.ApplicationAttributesSchema("offeredAmount")

    val evvCreate1 = EventVar("O_Create Offer")
      .defineTimestamp("tsCreate1") // a variable with this name will be created in the map of timestamps (if possible)
      .defineVar("amountCreate1", "offeredAmount") // a variable with this name will be created in the map of variables (if possible)

    val evvCreate2 = EventVar("O_Create Offer")
      .defineTimestamp("tsCreate2") // a variable with this name will be created in the map of timestamps (if possible)
      .defineVar("amountCreate2", "offeredAmount") // a variable with this name will be created in the map of variables (if possible)


    val tracexCreateOfferTwice = (evvCreate1 >|> evvCreate2)
    val tracexOfferGrows = tracexCreateOfferTwice.where((vars, timestamps, _) => {
      val off1 = vars.get("amountCreate1");
      val off2 = vars.get("amountCreate2");
      if (isDouble(off1) && isDouble(off2)) getAsDouble(off1) < getAsDouble(off2)
      else false
    })

    val tracexOfferFalls = tracexCreateOfferTwice.where((vars, timestamps, _) => {
      val off1 = vars.get("amountCreate1");
      val off2 = vars.get("amountCreate2");
      if (isDouble(off1) && isDouble(off2)) getAsDouble(off1) > getAsDouble(off2)
      else false
    })


    val logFull = logApplicationsWithArtificialStartsStops // fullOuterJoin logSegmentsWithClassifiedDurations
    println(s"Originally ${logFull.traces().size} traces")

    val subtracexAcceptValidateCompletedEnd = EventEx("O_Accepted") >-> EventEx("W_Validate application").withValue("lifecycle:transition", "COMPLETE") >>  EventEx("End")
    val subtracexAcceptedPendingCompleted = EventEx("O_Accepted") >-> EventEx("A_Pending").withValue("lifecycle:transition", "COMPLETE")

    val logFilteredGrowing = logFull.filter(tracexOfferGrows)
    println(s"Found ${logFilteredGrowing.traces().size} traces.")
    val logFilteredGrowingAccept = logFilteredGrowing.filter((t contains subtracexAcceptedPendingCompleted))
    println(s"Found ${logFilteredGrowingAccept.traces().size} traces.")

    val logFilteredFalling = logFull.filter(tracexOfferFalls)
    println(s"Found ${logFilteredFalling.traces().size} traces.")
    val logFilteredFallingAccept = logFilteredFalling.filter((t contains subtracexAcceptedPendingCompleted))
    println(s"Found ${logFilteredFallingAccept.traces().size} traces.")

    val logFilteredNotGrowingFalling = UnifiedEventLog.fromTraces(logFull.traces.filter(tr => !(tracexOfferGrows.evaluate(tr)) && !(tracexOfferFalls.evaluate(tr))))
    val logFilteredNotGrowingFallingMultipleOffers = logFilteredNotGrowingFalling.filter((t contains (EventEx("O_Create Offer") >-> EventEx("O_Create Offer"))))
    println(s"Found ${logFilteredNotGrowingFallingMultipleOffers.traces().size} traces.")
    val logFilteredNotGrowingFallingMultipleOffersAccept = logFilteredNotGrowingFallingMultipleOffers.filter((t contains subtracexAcceptedPendingCompleted))
    println(s"Found ${logFilteredNotGrowingFallingMultipleOffersAccept.traces().size} traces.")

    // collect traceId of each trace in the log (traceTuple = (traceId, evens)
//    val allTraceIds = logFull.traces().map( traceTuple => traceTuple._1 )
//    val growingTraceIds = logFilteredGrowing.traces().map( traceTuple => traceTuple._1)

    val schema = Array("requestedAmount", "requestedAmountClass", "offeredAmount", "offeredAmountClass", "lifecycle:transition", "org:resource")

    CsvWriter.logToCsvLocalFilesystem(
      logFilteredGrowing,
      s"${caseStudyConfig.outDir}/BPI_challenge_application_filtered(multiple_offers,growing)_log_${csvExportHelper.timestamp2String((new Date()).getTime).replace(':','.')}.csv",
      csvExportHelper.timestamp2String,
      schema: _*)

    CsvWriter.logToCsvLocalFilesystem(
      logFilteredGrowingAccept,
      s"${caseStudyConfig.outDir}/BPI_challenge_application_filtered(multiple_offers,growing,accept)_log_${csvExportHelper.timestamp2String((new Date()).getTime).replace(':','.')}.csv",
      csvExportHelper.timestamp2String,
      schema: _*)

    CsvWriter.logToCsvLocalFilesystem(
      logFilteredFalling,
      s"${caseStudyConfig.outDir}/BPI_challenge_application_filtered(multiple_offers,falling)_log_${csvExportHelper.timestamp2String((new Date()).getTime).replace(':','.')}.csv",
      csvExportHelper.timestamp2String,
      schema: _*)

    CsvWriter.logToCsvLocalFilesystem(
      logFilteredFallingAccept,
      s"${caseStudyConfig.outDir}/BPI_challenge_application_filtered(multiple_offers,falling,accept)_log_${csvExportHelper.timestamp2String((new Date()).getTime).replace(':','.')}.csv",
      csvExportHelper.timestamp2String,
      schema: _*)

    CsvWriter.logToCsvLocalFilesystem(
      logFilteredNotGrowingFallingMultipleOffers,
      s"${caseStudyConfig.outDir}/BPI_challenge_application_filtered(multiple_offers,unchanged)_log_${csvExportHelper.timestamp2String((new Date()).getTime).replace(':','.')}.csv",
      csvExportHelper.timestamp2String,
      schema: _*)

    CsvWriter.logToCsvLocalFilesystem(
      logFilteredNotGrowingFallingMultipleOffersAccept,
      s"${caseStudyConfig.outDir}/BPI_challenge_application_filtered(multiple_offers,unchanged,accept)_log_${csvExportHelper.timestamp2String((new Date()).getTime).replace(':','.')}.csv",
      csvExportHelper.timestamp2String,
      schema: _*)

    println(s"Pre-processing took ${(new Date().getTime - appStartTime.getTime) / 1000} seconds.")
  }
}
