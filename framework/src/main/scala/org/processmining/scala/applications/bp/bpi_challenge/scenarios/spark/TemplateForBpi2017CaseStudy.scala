package org.processmining.scala.applications.bp.bpi_challenge.scenarios.spark

import java.io.File
import java.time.Duration
import java.util.Date

import org.processmining.scala.applications.bp.bpi_challenge.types.OfferEvent
import org.processmining.scala.log.common.csv.spark.CsvReader
import org.processmining.scala.log.common.enhancment.segments.common.SegmentUtils
import org.processmining.scala.log.common.enhancment.segments.spark.{ClazzBasedSegmentProcessor, DurationSegmentProcessor, SegmentProcessorConfig}
import org.processmining.scala.log.common.filtering.expressions.traces.impl.TraceExpression
import org.processmining.scala.log.common.unified.event.UnifiedEvent
import org.processmining.scala.log.common.unified.log.common.UnifiedEventLog.UnifiedTrace
import org.processmining.scala.log.common.unified.log.spark.UnifiedEventLog
import org.processmining.scala.log.common.unified.trace.UnifiedTraceId
import org.processmining.scala.log.common.utils.spark.AbstractSession
import org.processmining.scala.log.utils.common.csv.common.{CsvExportHelper, CsvImportHelper}

// Internal app config
private case class CaseStudyConfig(
                                      inDir: String,
                                      outDir: String,
                                      timestamp1Ms: Long, // start of a time interval for analysis (ms)
                                      timestamp2Ms: Long, // end of a time interval for analysis (ms)
                                      twSize: Long, // time window size (ms)
                                      percentile: Double // for the duration processor
                                    )


private class TemplateForBpi2017CaseStudy(
                                   inDir: String = "D:/logs/bpi_challenge_2017",
                                   outDir: String = "D:/logs/bpi_challenge_2017/out",
                                   fromString: String = "02-01-2016 00:00:00",
                                   untilString: String = "02-02-2017 00:00:00",
                                   timeWindow: Long = Duration.ofDays(1).toMillis)
  extends AbstractSession("BPI 2017") {

  val appStartTime = new Date(); // for time measuring
  val dateHelper = new CsvImportHelper("dd-MM-yyyy HH:mm:ss", CsvExportHelper.AmsterdamTimeZone) //to provide time intervals in code
  val csvExportHelper = new CsvExportHelper("dd-MM-yy HH.mm.ss.SSS", CsvExportHelper.AmsterdamTimeZone, ";") //for logs export
  val from = dateHelper.extractTimestamp(fromString)
  val until = dateHelper.extractTimestamp(untilString)
  val caseStudyConfig = CaseStudyConfig(inDir, outDir, from, until, timeWindow, 0.5) // percentile

  private val _ = new File(caseStudyConfig.outDir).mkdirs() // create out dir

  // provides events, lazy load
  val eventsOffers = OfferEvent.loadFromCsv(spark, new CsvReader(), caseStudyConfig.inDir)

  private val logOffers = UnifiedEventLog.create(eventsOffers)
    .map(TraceExpression().trimToTimeframe(from, until))

  val logOffersWithArtificialStartsStops =
    logOffers
      .map {
        TraceExpression().withArtificialStartsStops(
          OfferEvent.createArtificialEvent("Start", -caseStudyConfig.twSize, _: UnifiedTraceId, _: UnifiedEvent),
          OfferEvent.createArtificialEvent("End", caseStudyConfig.twSize, _: UnifiedTraceId, _: UnifiedEvent)
        )
      }


  val logSegmentsWithArtificialStartsStops =
    logOffersWithArtificialStartsStops
      .map(SegmentUtils.convertToSegments(SegmentUtils.DefaultSeparator, _: UnifiedTrace))


  /** *
    * Dataframe "segments" should be created before
    *
    * @return
    */
  //  def getStat() = {
  //    // Calculating segments descriptive statistics with the given percentile value
  //    // TODO: rename median into percentile in the resultset
  //    val stat = getDescriptiveStatistics("segments", caseStudyConfig.percentile, spark)
  //    stat.createOrReplaceTempView("stat") // registering stat 'table'
  //    stat
  //  }

  // default config of the Segment Processors
  val processorConfig = SegmentProcessorConfig(spark,
    logSegmentsWithArtificialStartsStops,
    caseStudyConfig.timestamp1Ms,
    caseStudyConfig.timestamp2Ms,
    caseStudyConfig.twSize)

  //Map offered amounts to segments
  val offeredAmountClazzBasedSegmentProcessor = new ClazzBasedSegmentProcessor[(String, UnifiedEvent)](
    processorConfig,
    OfferEvent.ClazzCount,
    eventsOffers
      .filter(_._2.getAs[Byte]("offeredAmountClass") != 0)
      .distinct,
    _._1,
    _._2.timestamp,
    _._2.getAs[Byte]("offeredAmountClass")
  )


  val logClassifiedOfferAmounts = offeredAmountClazzBasedSegmentProcessor.getClassifiedSegmentLog()

  val (segmentDf, statDf, durationProcessor) =
    DurationSegmentProcessor(processorConfig, caseStudyConfig.percentile)

  val durationSegmentLog = durationProcessor.getClassifiedSegmentLog()


  val logMixed = logOffersWithArtificialStartsStops fullOuterJoin logClassifiedOfferAmounts

  val t = TraceExpression()

}
