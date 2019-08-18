package org.processmining.scala.applications.bp.bpi_challenge.scenarios.spark

import java.time.Duration
import java.util.Date

import org.processmining.scala.applications.bp.bpi_challenge.types.OfferEvent
import org.processmining.scala.log.common.csv.spark.CsvWriter
import org.processmining.scala.log.common.filtering.expressions.events.regex.impl.EventEx
import org.processmining.scala.log.common.filtering.expressions.traces._
import org.processmining.scala.log.common.unified.event.CommonAttributeSchemas

private object FilteringExample extends TemplateForBpi2017CaseStudy {
  def main(args: Array[String]): Unit = {
    /*
    Question: find all traces where "O_Create Offer" with a high offer amount (greater than 30000)
    eventually followed by "O_Accepted", and at least one transition
    between 2 directly connected events took from 1 to 2 months
    */
    /*
    Question: find all traces where "O_Create Offer" with a high offer amount (greater than 30000)
    eventually followed by "O_Accepted", and at least one transition
    between 2 directly connected events took from 1 to 2 months
    */
    val createdOfferWihtHugeAmountEx =
    EventEx("O_Create Offer")
      // 7 and 8 are amount classes, which defined in event sources (classes 7-9 is for amounts from 30 000 and higher)
      .withRange[Byte]("offeredAmountClass",  7, 9)
    val acceptedEx = EventEx("O_Accepted")

    val eventuallyAcceptedHugeAmountEx = createdOfferWihtHugeAmountEx >-> acceptedEx

    val someSlowSegmentsWithDurationBetween1and2monthsEx =
      EventEx()
        .withRange(CommonAttributeSchemas.AttrNameDuration, Duration.ofDays(30).toMillis, Duration.ofDays(60).toMillis)


    println(s"""eventuallyAcceptedHugeAmountEx is translated into "${eventuallyAcceptedHugeAmountEx.translate()}"""")
    println(s"""someSlowSegmentsWithDurationBetween1and2monthsEx is translated into "${someSlowSegmentsWithDurationBetween1and2monthsEx.translate()}"""")



    //println(s"Originally ${logMixed.traces().count()} traces")

    val logFiltered = (logMixed fullOuterJoin durationSegmentLog)
      .filter( (t contains eventuallyAcceptedHugeAmountEx) and (t contains someSlowSegmentsWithDurationBetween1and2monthsEx))
      .persist()

    println(s"Found ${logFiltered.traces().count} traces")

    CsvWriter.logToCsvLocalFilesystem(
      logFiltered,
      s"${caseStudyConfig.outDir}/BPI_challenge_Spark_filtered_log_${csvExportHelper.timestamp2String((new Date()).getTime)}.csv",
      csvExportHelper.timestamp2String,
      "offeredAmount", "offeredAmountClass")

    val totalTime = new Date(); // for time measuring
    println(s"Pre-processing took ${(totalTime.getTime - appStartTime.getTime) / 1000} seconds.")
  }
}
