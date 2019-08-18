package org.processmining.scala.applications.bp.bpi_challenge.scenarios.spark.dev.billion

import java.time.Duration
import java.util.Date

import org.processmining.scala.log.common.filtering.expressions.events.regex.impl.EventEx
import org.processmining.scala.log.common.filtering.expressions.traces._
import org.processmining.scala.log.common.unified.event.CommonAttributeSchemas

private object FilteringExampleBillion extends TemplateForBpi2017CaseStudy {
  def main(args: Array[String]): Unit = {

    val (logOffers, startDate) = LogCloner.createLog(config.scale, config.partitions)
    /*
    Question: find all traces where "O_Create Offer" with a high offer amount (greater than 30000)
    eventually followed by "O_Accepted", and at least one transition
    between 2 directly connected events took from 1 to 2 months
    */
    val createdOfferWihtHugeAmountEx =
    EventEx(LogCloner.activityMap("O_Create Offer").toString)
      // 7 and 8 are amount classes, which defined in event sources (classes 7-9 is for amounts from 30 000 and higher)
      .withRange[Byte]("offeredAmountClass", 7, 9)
    val acceptedEx = EventEx(LogCloner.activityMap("O_Accepted").toString)

    val eventuallyAcceptedHugeAmountEx = createdOfferWihtHugeAmountEx >-> acceptedEx

    val someSlowSegmentsWithDurationBetween1and2monthsEx =
      EventEx()
        .withRange(CommonAttributeSchemas.AttrNameDuration, Duration.ofDays(30).toMillis, Duration.ofDays(60).toMillis)

    logger.info(s"""eventuallyAcceptedHugeAmountEx is translated into "${eventuallyAcceptedHugeAmountEx.translate()}"""")
    logger.info(s"""someSlowSegmentsWithDurationBetween1and2monthsEx is translated into "${someSlowSegmentsWithDurationBetween1and2monthsEx.translate()}"""")

    //println(s"Originally ${logMixed.traces().count()} traces")

    val logFiltered = logOffers
      .filter((t contains eventuallyAcceptedHugeAmountEx) and (t contains someSlowSegmentsWithDurationBetween1and2monthsEx))
    //.persist()

    val found = logFiltered.traces().count
    val msg = s"Found $found traces"
    logger.info(msg)
    //    CsvWriter.logToCsvLocalFilesystem(
    //      logFiltered,
    //      s"${caseStudyConfig.outDir}/BPI_challenge_Spark_filtered_log_${csvExportHelper.timestamp2String((new Date()).getTime)}.csv",
    //      csvExportHelper.timestamp2String,
    //      OfferEvent.OfferAttributesSchema)

    val totalTime = new Date(); // for time measuring
    logger.info(s"Pre-processing took ${(totalTime.getTime - startDate.getTime) / 1000} seconds.")
  }
}
