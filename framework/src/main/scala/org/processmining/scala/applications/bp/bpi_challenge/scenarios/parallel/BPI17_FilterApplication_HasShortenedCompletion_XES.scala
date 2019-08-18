//package org.processmining.scala.applications.bp.bpi_challenge.scenarios.parallel
//
//import java.util.Date
//
//import org.processmining.scala.applications.bp.bpi_challenge.types.ApplicationEvent
//import org.processmining.scala.log.common.csv.parallel.CsvWriter
//import org.processmining.scala.log.common.filtering.expressions.events.regex.EventEx
//import org.processmining.scala.log.common.filtering.expressions.traces._
//import org.processmining.scala.log.common.xes.parallel.XesWriter
//
//private object BPI17_FilterApplication_HasShortenedCompletion_XES extends TemplateForXES {
//  def main(args: Array[String]): Unit = {
//    /*
//    Question: find all traces that were active during the phase of "shortened completion" happening
//    */
//
//    val logFull = log // fullOuterJoin logSegmentsWithClassifiedDurations
//    println(s"Originally ${logFull.traces().size} traces")
//
//    val parsingDone = (new Date()).getTime
//
//    val logShortenedCompletion = logFull.filter((t contains EventEx("W_Shortened completion ")))
//
//    val queryingDone = (new Date()).getTime
//
//    println(s"Found ${logShortenedCompletion.traces().size} traces with filter.")
////    println(s"Found ${logOfferRepeatedOther.traces().size} traces with filter.")
////    println(s"Found ${logOfferOnceAccept.traces().size} traces with filter.")
////    println(s"Found ${logOfferOnceOther.traces().size} traces with filter.")
////    println(s"Found ${logProjected.traces().size} traces with projection.")
//
//    // TODO: no writing yet, check types
////    CsvWriter.logToCsvLocalFilesystem(
////      logShortenedCompletion,
////      s"${caseStudyConfig.outDir}/BPI_challenge_application_shortenedCompletion_log_${csvExportHelper.timestamp2String((new Date()).getTime).replace(':','.')}.csv",
////      csvExportHelper.timestamp2String,
////      ApplicationEvent.ApplicationAttributesSchema)
//
//    XesWriter.write(logShortenedCompletion,
//      s"${caseStudyConfig.outDir}/BPI_challenge_application_shortenedCompletion_log_${csvExportHelper.timestamp2String((new Date()).getTime).replace(':','.')}.xes.gz"
//    )
//    val writingDone = (new Date()).getTime
//
//    println(s"Reading took ${(parsingDone - appStartTime.getTime) / 1000} seconds.")
//    println(s"Querying took ${(queryingDone - parsingDone) / 1000} seconds.")
//    println(s"Writing took ${(writingDone - queryingDone) / 1000} seconds.")
//  }
//}
