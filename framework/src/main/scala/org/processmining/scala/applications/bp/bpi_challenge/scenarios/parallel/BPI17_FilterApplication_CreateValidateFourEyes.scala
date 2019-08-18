package org.processmining.scala.applications.bp.bpi_challenge.scenarios.parallel

import java.util.Date

import org.processmining.scala.applications.bp.bpi_challenge.scenarios.parallel.BPI17_FilterApplication_MultipleOffers_FallingVsGrowingAmounts.{getAsDouble, isDouble}
import org.processmining.scala.applications.bp.bpi_challenge.types.ApplicationEvent
import org.processmining.scala.log.common.csv.parallel.CsvWriter
import org.processmining.scala.log.common.filtering.expressions.events.regex.impl.EventEx
import org.processmining.scala.log.common.filtering.expressions.events.variables.EventVar
import org.processmining.scala.log.common.filtering.expressions.traces._

private object BPI17_FilterApplication_CreateValidateFourEyes extends TemplateForBpi2017CaseStudyApplication {
  def main(args: Array[String]): Unit = {

    /*
    Question: find all traces where "A_Create Application" is done by a different/the same user as "W_Validate application"
    */


    val attrOrgResource = ApplicationEvent.ApplicationAttributesSchema("org:resource")

    val evvCreate = EventVar("A_Create Application")
      .defineVar("res1", "org:resource") // a variable with this name will be created in the map of variables (if possible)

    val evvValidate = EventVar("W_Validate application")
      .defineVar("res2", "org:resource") // a variable with this name will be created in the map of variables (if possible)

    val tracexFourEyes = (evvCreate >|> evvValidate).where((vars, timestamps, activities) => {
      val res1 = vars.get("res1");
      val res2 = vars.get("res2");
      res1.isDefined && res2.isDefined && (!res1.get.asInstanceOf[String].equals(res2.get.asInstanceOf[String]))
    })

    val tracexSameEyes = (evvCreate >|> evvValidate).where((vars, timestamps, activities) => {
      val res1 = vars.get("res1");
      val res2 = vars.get("res2");
      res1.isDefined && res2.isDefined && (res1.get.asInstanceOf[String].equals(res2.get.asInstanceOf[String]))
    })
    val logFull = logApplicationsWithArtificialStartsStops // fullOuterJoin logSegmentsWithClassifiedDurations
    println(s"Originally ${logFull.traces().size} traces")

    val parsingDone = (new Date()).getTime

    val logFilteredFourEyes = logFull.filter(tracexFourEyes)
    println(s"Found ${logFilteredFourEyes.traces().size} traces.")

    val logFilteredSameEyes = logFull.filter(tracexSameEyes)
    println(s"Found ${logFilteredSameEyes.traces().size} traces.")

    val logFilteredSameEyesViolation = logFilteredSameEyes.project(EventEx("A_Create Application"), EventEx("W_Validate application"))
    println(s"Found ${logFilteredSameEyes.traces().size} traces.")

    val queryingDone = (new Date()).getTime
    //val schema = Array("requestedAmount", "requestedAmountClass", "offeredAmount", "offeredAmountClass", "lifecycle:transition", "org:resource")

    CsvWriter.logToCsvLocalFilesystem(
      logFilteredFourEyes,
      s"${caseStudyConfig.outDir}/BPI_challenge_all_filtered(create_validate_4eyes)_log_${csvExportHelper.timestamp2String((new Date()).getTime).replace(':','.')}.csv",
      csvExportHelper.timestamp2String,
      ApplicationEvent.schema: _*)

    CsvWriter.logToCsvLocalFilesystem(
      logFilteredSameEyes,
      s"${caseStudyConfig.outDir}/BPI_challenge_all_filtered(create_validate_2eyes)_log_${csvExportHelper.timestamp2String((new Date()).getTime).replace(':','.')}.csv",
      csvExportHelper.timestamp2String,
      ApplicationEvent.schema: _*)

    CsvWriter.logToCsvLocalFilesystem(
      logFilteredSameEyesViolation,
      s"${caseStudyConfig.outDir}/BPI_challenge_all_filtered(create_validate_2eyes)_violations_log_${csvExportHelper.timestamp2String((new Date()).getTime).replace(':','.')}.csv",
      csvExportHelper.timestamp2String,
      ApplicationEvent.schema: _*)

    val writingDone = (new Date()).getTime

    println(s"Reading took ${(parsingDone - appStartTime.getTime) / 1000} seconds.")
    println(s"Querying took ${(queryingDone - parsingDone) / 1000} seconds.")
    println(s"Writing took ${(writingDone - queryingDone) / 1000} seconds.")
  }
}
