//package org.processmining.scala.applications.bp.bpi_challenge.scenarios.parallel
//
//import java.io.File
//import java.time.Duration
//import java.util.Date
//
//import org.processmining.scala.applications.bp.bpi_challenge.types.ApplicationEvent
//import org.processmining.scala.log.common.csv.common.{CsvExportHelper, CsvImportHelper}
//import org.processmining.scala.log.common.csv.parallel.CsvReader
//import org.processmining.scala.log.common.enhancment.segments.parallel.{ClazzBasedSegmentProcessor, DurationSegmentProcessor, SegmentProcessorConfig}
//import org.processmining.scala.log.common.enhancment.segments.spark.SegmentUtils
//import org.processmining.scala.log.common.filtering.expressions.traces.TraceExpression
//import org.processmining.scala.log.common.unified.event.UnifiedEvent
//import org.processmining.scala.log.common.unified.log.parallel.UnifiedEventLog
//import org.processmining.scala.log.common.unified.log.spark.UnifiedEventLog.UnifiedTrace
//import org.processmining.scala.log.common.unified.trace.UnifiedTraceId
//import org.processmining.scala.log.common.xes.parallel.XesReader
//
//import scala.collection.parallel.ParSeq
//
//// Internal app config
//protected case class CaseStudyConfigXES(
//                                      inDir: String,
//                                      outDir: String,
//                                      timestamp1Ms: Long, // start of a time interval for analysis (ms)
//                                      timestamp2Ms: Long, // end of a time interval for analysis (ms)
//                                      twSize: Long, // time window size (ms)
//                                      percentile: Double // for the duration processor
//                                    )
//
//
//protected class TemplateForXES(
//                                   inFile: String =  "D:\\LinuxShared\\Logs\\BPIC17\\BPI_Challenge_2017.xes.gz",
//                                   outDir: String = "D:\\LinuxShared\\Logs\\BPIC17\\bpi2017_filtering_xes",
//                                   fromString: String = "", //"02-01-2016 00:00:00",
//                                   untilString: String = "", //"02-02-2017 00:00:00",
//                                   timeWindow: Long = Duration.ofDays(1).toMillis){
//
//  def getMinimumTimeStamp(events: List[UnifiedEvent]): Long = {
//    // map all events to their timestamps and reduce the list of timestamps by the minimum function
//    val minTime = events.map( e => e.timestamp ).reduceLeft(_ min _)
//    minTime
//  }
//
//  def getMinimumTimeStamp(traces: ParSeq[(UnifiedTraceId, List[UnifiedEvent])]): Long = {
//    val minTime = traces.map( tr => getMinimumTimeStamp(tr._2)).reduceLeft(_ min _)
//    minTime
//  }
//
//  def getMaximumTimeStamp(events: List[UnifiedEvent]): Long = {
//    // map all events to their timestamps and reduce the list of timestamps by the maximum function
//    val maxTime = events.map( e => e.timestamp ).reduceLeft(_ max _)
//    maxTime
//  }
//
//  def getMaximumTimeStamp(traces: ParSeq[(UnifiedTraceId, List[UnifiedEvent])]): Long = {
//    val maxTime = traces.map( tr => getMaximumTimeStamp(tr._2)).reduceLeft(_ max _)
//    maxTime
//  }
//
//  val appStartTime = new Date(); // for time measuring
//
//  val logFile = new File(inFile)
//
//  // load log from XES source
//  val importedLog = XesReader.read(logFile)(0)
//
//  val dateHelper = new CsvImportHelper("dd-MM-yyyy HH:mm:ss", CsvExportHelper.AmsterdamTimeZone) //to provide time intervals in code
//  val csvExportHelper = new CsvExportHelper("dd-MM-yyyy HH:mm:ss", CsvExportHelper.AmsterdamTimeZone, ";") //for logs export
//  val from =  if (!fromString.isEmpty) dateHelper.extractTimestamp(fromString) else getMinimumTimeStamp(importedLog.traces())
//  val until = if (!untilString.isEmpty) dateHelper.extractTimestamp(untilString) else getMaximumTimeStamp(importedLog.traces())
//
//  val log = if (!fromString.isEmpty && !toString.isEmpty) {
//    val from = dateHelper.extractTimestamp(fromString)
//    val until = dateHelper.extractTimestamp(untilString)
//    importedLog.map(TraceExpression().trimToTimeframe(from, until))
//  } else {
//    importedLog
//  }
//
//  val caseStudyConfig = CaseStudyConfigXES(inFile, outDir, from, until,  timeWindow, 0.5) // percentile
//  private val _ = new File(caseStudyConfig.outDir).mkdirs() // create out dir
//
//  val logWithArtificialStartsStops =
//    log
//      .map{TraceExpression().withArtificialStartsStops(
//        ApplicationEvent.createArtificialEvent("Start", -caseStudyConfig.twSize, _: UnifiedTraceId, _: UnifiedEvent),
//        ApplicationEvent.createArtificialEvent("End", caseStudyConfig.twSize, _: UnifiedTraceId, _: UnifiedEvent)
//      )}
//
//  val logApplicationsDfrEventsWithArtificialStartsStops =
//    logWithArtificialStartsStops
//      .map(SegmentUtils.convertToSegments(SegmentUtils.DefaultSeparator, _ : UnifiedTrace))
//
//  val t = TraceExpression()
//
//
//}
