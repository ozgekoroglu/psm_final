package org.processmining.scala.applications.bp.bpi_challenge.scenarios.parallel

import java.time.Duration
import java.util.Date

import org.processmining.scala.applications.bp.bpi_challenge.types.OfferEvent
import org.processmining.scala.log.common.csv.parallel.CsvWriter
import org.processmining.scala.log.common.filtering.expressions.events.regex.impl.EventEx
import org.processmining.scala.log.common.filtering.expressions.events.variables.EventVar
import org.processmining.scala.log.common.filtering.expressions.traces._

private object FilteringExampleWithVars extends TemplateForBpi2017CaseStudy {
  def main(args: Array[String]): Unit = {
    /*
    Question: find all traces where "O_Create Offer" with a high offer amount (greater than 30000)
    eventually followed by "O_Accepted", and with a time interval between them greater than 30 days and
    equal amounts of those events
    */
    val evxCreatedOfferWihtHugeAmount =
      EventEx("O_Create Offer")
        // 7 and 8 are amount classes, which defined in event sources (classes 7-9 is for amounts from 30 000 and higher)
        .withRange[Byte]("offeredAmountClass", 7, 9)
    val evxAccepted = EventEx("O_Accepted")
    val subtracexEventuallyAcceptedHugeAmount = evxCreatedOfferWihtHugeAmount >-> evxAccepted
    println(s"""evxCreatedOfferWihtHugeAmount is translated into "${subtracexEventuallyAcceptedHugeAmount.translate()}"""")

    val attrOfferedAmount = OfferEvent.OfferAttributesSchema("offeredAmount") // to provide a type of attribute

    val evvCreate = EventVar("O_Create Offer")
      .defineTimestamp("tsCreate") // a variable with this name will be created in the map of timestamps (if possible)
      .defineVar("amountCreate", "offeredAmount") // a variable with this name will be created in the map of variables (if possible)
    //.defineVar("amountClassCreate", OfferEvent.OfferAttributesSchema("offeredAmountClass")) // example how to use offeredAmountClass

    val evvAccepted = EventVar("O_Accepted")
      .defineTimestamp("tsAccepted") // a variable with this name will be created in the map of timestamps (if possible)
      .defineVar("amountAccepted", "offeredAmount") // a variable with this name will be created in the map of variables (if possible)

    val evvxDurationBetweenCreatedAndAcceptedGreaterThan30DaysAndAmountsAreEqual =
      (evvCreate >|> evvAccepted).where( // a predicate as a user-defined function
        (vars, timestamps, _) => {
          val tsCreate = timestamps.get("tsCreate") // timestamp of "O_Create Offer" as Option[Long]
          val tsAccepted = timestamps.get("tsAccepted") // timestamp of "O_Accepted" as Option[Long]
          val amountCreate = vars.get(("amountCreate")) // amount of "O_Create Offer" as Option[String]
          val amountAccepted = vars.get(("amountAccepted")) // amount of "O_Accepted" as Option[String]

          //At first we must check that all variables (that we are using) are defined
          tsCreate.isDefined && tsAccepted.isDefined && amountCreate.isDefined && amountAccepted.isDefined &&
            //then we can define any predicate:
            (tsAccepted.get - tsCreate.get > Duration.ofDays(30).toMillis) && // Time interval is greater than 30 days
            (amountCreate.get.asInstanceOf[String] == amountAccepted.get.asInstanceOf[String]) // In the log they are always equal (?)
        }
      )

    val logMixedToBeFiltered = logOffersWithArtificialStartsStops
    println(s"Originally ${logMixedToBeFiltered.traces().size} traces")

    val logFiltered = logMixedToBeFiltered
      .filter((t contains subtracexEventuallyAcceptedHugeAmount) and evvxDurationBetweenCreatedAndAcceptedGreaterThan30DaysAndAmountsAreEqual)
    println(s"Found ${logFiltered.traces().size} traces")

    val filename = s"${caseStudyConfig.outDir}/BPI_challenge_Parallel_filtered_log_vars_${csvExportHelper.timestamp2String((new Date()).getTime)}.csv"
    CsvWriter.logToCsvLocalFilesystem(
      logFiltered,
      filename,
      csvExportHelper.timestamp2String,
      "offeredAmount", "offeredAmountClass")

    println(s"Pre-processing took ${(new Date().getTime - appStartTime.getTime) / 1000} seconds.")
    println(s"Output: $filename")
  }
}
