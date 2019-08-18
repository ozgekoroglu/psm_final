package org.processmining.scala.applications.bp.bpi_challenge.scenarios.parallel

import java.io.File
import java.time.Duration
import java.util.Date

import org.processmining.scala.applications.bp.bpi_challenge.types.ApplicationEvent
import org.processmining.scala.log.common.csv.parallel.CsvReader
import org.processmining.scala.log.common.enhancment.segments.common.SegmentUtils
import org.processmining.scala.log.common.filtering.expressions.traces.impl.TraceExpression
import org.processmining.scala.log.common.unified.event.UnifiedEvent
import org.processmining.scala.log.common.unified.log.common.UnifiedEventLog.UnifiedTrace
import org.processmining.scala.log.common.unified.log.parallel.UnifiedEventLog
import org.processmining.scala.log.common.unified.trace.UnifiedTraceId
import org.processmining.scala.log.utils.common.csv.common.{CsvExportHelper, CsvImportHelper}

import scala.collection.parallel.ParSeq

// Internal app config
protected case class CaseStudyConfigBPI17Application(
                                      inDir: String,
                                      outDir: String,
                                      timestamp1Ms: Long, // start of a time interval for analysis (ms)
                                      timestamp2Ms: Long, // end of a time interval for analysis (ms)
                                      twSize: Long, // time window size (ms)
                                      percentile: Double // for the duration processor
                                    )


protected class TemplateForBpi2017CaseStudyApplication(
                                   inDir: String =  "D:\\LinuxShared\\Logs\\BPIC17",
                                   outDir: String = "D:\\LinuxShared\\Logs\\BPIC17\\/bpi2017_filtering_12",
                                   fromString: String = "", //"02-01-2016 00:00:00",
                                   untilString: String = "", //"02-02-2017 00:00:00",
                                   timeWindow: Long = Duration.ofDays(1).toMillis){

  def getMinimumTimeStamp(events: ParSeq[(String, UnifiedEvent)]): Long = {
    // map all events to their timestamps and reduce the list of timestamps by the minimum function
    val minTime = events.map( e => e._2.timestamp ).reduceLeft(_ min _)
    minTime
  }

  def getMaximumTimeStamp(events: ParSeq[(String, UnifiedEvent)]): Long = {
    // map all events to their timestamps and reduce the list of timestamps by the maximum function
    val maxTime = events.map( e => e._2.timestamp ).reduceLeft(_ max _)
    maxTime
  }

  val appStartTime = new Date(); // for time measuring

  // load all event types from their sources
  val eventsApplication = ApplicationEvent.loadEventsFromCsv(new CsvReader(), inDir)
    // UserGuide: add additional event types and sources here if necessary

  // setup analysis
  val dateHelper = new CsvImportHelper("dd-MM-yyyy HH:mm:ss", CsvExportHelper.AmsterdamTimeZone) //to provide time intervals in code
  val csvExportHelper = new CsvExportHelper("dd-MM-yyyy HH:mm:ss", CsvExportHelper.AmsterdamTimeZone, ";") //for logs export
  val from =  if (!fromString.isEmpty) dateHelper.extractTimestamp(fromString) else getMinimumTimeStamp(eventsApplication)
  val until = if (!untilString.isEmpty) dateHelper.extractTimestamp(untilString) else getMaximumTimeStamp(eventsApplication)

  val caseStudyConfig = CaseStudyConfigBPI17Application(inDir, outDir, from, until,  timeWindow, 0.5) // percentile
  private val _ = new File(caseStudyConfig.outDir).mkdirs() // create out dir

  val logApplications = UnifiedEventLog.create(eventsApplication)
    .map(TraceExpression()
      .trimToTimeframe(from, until))

  val logApplicationsWithArtificialStartsStops =
    logApplications
      .map{TraceExpression().withArtificialStartsStops(
        ApplicationEvent.createArtificialEvent("Start", -caseStudyConfig.twSize, _: UnifiedTraceId, _: UnifiedEvent),
        ApplicationEvent.createArtificialEvent("End", caseStudyConfig.twSize, _: UnifiedTraceId, _: UnifiedEvent)
      )}

  val logApplicationsDfrEventsWithArtificialStartsStops =
    logApplicationsWithArtificialStartsStops
      .map(SegmentUtils.convertToSegments(SegmentUtils.DefaultSeparator, _ : UnifiedTrace))

  val t = TraceExpression()


}
