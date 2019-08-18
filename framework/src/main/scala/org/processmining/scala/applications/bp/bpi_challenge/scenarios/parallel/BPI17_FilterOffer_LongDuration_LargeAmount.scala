package org.processmining.scala.applications.bp.bpi_challenge.scenarios.parallel

import java.time.Duration
import java.util.Date

import org.processmining.scala.applications.bp.bpi_challenge.types.OfferEvent
import org.processmining.scala.log.common.csv.parallel.CsvWriter
import org.processmining.scala.log.common.filtering.expressions.events.regex.impl.EventEx
import org.processmining.scala.log.common.filtering.expressions.traces._
import org.processmining.scala.log.common.unified.event.CommonAttributeSchemas

private object BPI17_FilterOffer_LongDuration_LargeAmount extends TemplateForBpi2017CaseStudy {
  def main(args: Array[String]): Unit = {
    /*
    Question: find all traces where "O_Create Offer" with a high offer amount (greater than 30000)
    eventually followed by "O_Accepted", and at least one transition
    between 2 directly connected events took from 1 to 2 months
    */
    val evxCreatedOfferWihtHugeAmount =
      EventEx("O_Create Offer")
        // 7 and 8 are amount classes, which defined in event sources (classes 7-9 is for amounts from 30 000 and higher)
        .withRange[Byte]("offeredAmountClass", 7, 9)
    val evxAccepted = EventEx("O_Accepted")
    val subtracexEventuallyAcceptedHugeAmount = evxCreatedOfferWihtHugeAmount >-> evxAccepted
    val evxSomeSlowSegmentsWithDurationBetween1and2months =
      EventEx()
        .withRange(CommonAttributeSchemas.AttrNameDuration, Duration.ofDays(30).toMillis, Duration.ofDays(60).toMillis)

    println(s"""evxCreatedOfferWihtHugeAmount is translated into "${subtracexEventuallyAcceptedHugeAmount.translate()}"""")
    println(s"""evxSomeSlowSegmentsWithDurationBetween1and2months is translated into "${evxSomeSlowSegmentsWithDurationBetween1and2months.translate()}"""")

    val logSegmentsWithClassifiedDurations =  durationProcessor.getClassifiedSegmentLog()
    val logFullwSegments = logOffersWithArtificialStartsStops fullOuterJoin logSegmentsWithClassifiedDurations
    println(s"Originally ${logFullwSegments.traces().size} traces")

    val logFilteredSlow = logFullwSegments
      .filter((t contains subtracexEventuallyAcceptedHugeAmount) and (t contains evxSomeSlowSegmentsWithDurationBetween1and2months))
    println(s"Found ${logFilteredSlow.traces().size} traces")

    CsvWriter.logToCsvLocalFilesystem(
      logFilteredSlow.filterByAttributeNames("offeredAmount", "offeredAmountClass"),
      s"${caseStudyConfig.outDir}/BPI_challenge_offer_filtered(long,huge-amount)_log_${csvExportHelper.timestamp2String((new Date()).getTime).replace(':','.')}.csv",
      csvExportHelper.timestamp2String,
      "offeredAmount", "offeredAmountClass")

    println(s"Pre-processing took ${(new Date().getTime - appStartTime.getTime) / 1000} seconds.")
  }
}
