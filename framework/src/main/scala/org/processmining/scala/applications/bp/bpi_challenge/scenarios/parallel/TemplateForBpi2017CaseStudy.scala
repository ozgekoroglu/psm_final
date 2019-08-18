package org.processmining.scala.applications.bp.bpi_challenge.scenarios.parallel

import java.io.File
import java.time.Duration
import java.util.Date

import org.processmining.scala.applications.bp.bpi_challenge.types.OfferEvent
import org.processmining.scala.log.common.csv.parallel.CsvReader
import org.processmining.scala.log.common.enhancment.segments.common.SegmentUtils
import org.processmining.scala.log.common.enhancment.segments.parallel.{ClazzBasedSegmentProcessor, DurationSegmentProcessor, SegmentProcessorConfig}
import org.processmining.scala.log.common.filtering.expressions.traces.impl.TraceExpression
import org.processmining.scala.log.common.unified.event.UnifiedEvent
import org.processmining.scala.log.common.unified.log.common.UnifiedEventLog.UnifiedTrace
import org.processmining.scala.log.common.unified.log.parallel.UnifiedEventLog
import org.processmining.scala.log.common.unified.trace.UnifiedTraceId
import org.processmining.scala.log.utils.common.csv.common.{CsvExportHelper, CsvImportHelper}


// Internal app config
private case class CaseStudyConfigBPI17Offer(
                                      inDir: String,
                                      outDir: String,
                                      timestamp1Ms: Long, // start of a time interval for analysis (ms)
                                      timestamp2Ms: Long, // end of a time interval for analysis (ms)
                                      twSize: Long, // time window size (ms)
                                      percentile: Double // for the duration processor
                                    )


protected class TemplateForBpi2017CaseStudy(
                                   inDir: String =  "D:\\LinuxShared\\Logs\\BPIC17",
                                   outDir: String = "D:\\LinuxShared\\Logs\\BPIC17\\/bpi2017_filtering_12",
                                   fromString: String = "02-01-2016 00:00:00",
                                   untilString: String = "02-02-2017 00:00:00",
                                   timeWindow: Long = Duration.ofDays(1).toMillis){

  val appStartTime = new Date(); // for time measuring
  val dateHelper = new CsvImportHelper("dd-MM-yyyy HH:mm:ss", CsvExportHelper.AmsterdamTimeZone) //to provide time intervals in code
  val csvExportHelper = new CsvExportHelper("dd-MM-yyyy HH:mm:ss", CsvExportHelper.AmsterdamTimeZone, ";") //for logs export
  val from = dateHelper.extractTimestamp(fromString)
  val until = dateHelper.extractTimestamp(untilString)
  val caseStudyConfig = CaseStudyConfigBPI17Application(inDir, outDir, from, until,  timeWindow, 0.5) // percentile
  private val _ = new File(caseStudyConfig.outDir).mkdirs() // create out dir

  // provides events, lazy load
  val eventsOffers = OfferEvent.loadFromCsv(new CsvReader(), caseStudyConfig.inDir)
  val logOffers = UnifiedEventLog.create(eventsOffers)
    .map(TraceExpression()
      .trimToTimeframe(from, until))

  val logOffersWithArtificialStartsStops =
    logOffers
      .map{TraceExpression().withArtificialStartsStops(
        OfferEvent.createArtificialEvent("Start", -caseStudyConfig.twSize, _: UnifiedTraceId, _: UnifiedEvent),
        OfferEvent.createArtificialEvent("End", caseStudyConfig.twSize, _: UnifiedTraceId, _: UnifiedEvent)
      )}

  val logOfDfrEventsWithArtificialStartsStops =
    logOffersWithArtificialStartsStops
      .map(SegmentUtils.convertToSegments(SegmentUtils.DefaultSeparator, _ : UnifiedTrace))

  val t = TraceExpression()

  // default config of the Segment Processors
  val processorConfig = SegmentProcessorConfig(logOfDfrEventsWithArtificialStartsStops,
    caseStudyConfig.timestamp1Ms,
    caseStudyConfig.timestamp2Ms,
    caseStudyConfig.twSize)

  //Map offered amounts to DFR events
  val offeredAmountClazzBasedSegmentProcessor = new ClazzBasedSegmentProcessor[(String, UnifiedEvent)](
    processorConfig,
    OfferEvent.ClazzCount,
    eventsOffers
      .filter(_._2.getAs[Byte]("offeredAmountClass") != 0)
      .distinct
      .toList,
    _._1,
    _._2.timestamp,
    _._2.getAs[Byte]("offeredAmountClass")
  )
  val (segments, stat, durationProcessor) = DurationSegmentProcessor(processorConfig)
}
