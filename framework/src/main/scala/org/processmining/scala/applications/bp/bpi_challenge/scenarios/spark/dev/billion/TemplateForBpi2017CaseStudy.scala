package org.processmining.scala.applications.bp.bpi_challenge.scenarios.spark.dev.billion

import java.time.Duration
import java.util.Date

import org.apache.log4j.PropertyConfigurator
import org.processmining.scala.applications.bp.bpi_challenge.types.OfferEvent
import org.processmining.scala.log.common.csv.parallel.CsvReader
import org.processmining.scala.log.common.filtering.expressions.traces.impl.TraceExpression
import org.processmining.scala.log.common.unified.log.parallel.UnifiedEventLog
import org.processmining.scala.log.common.utils.spark.AbstractSession
import org.processmining.scala.log.utils.common.csv.common.{CsvExportHelper, CsvImportHelper}
import org.slf4j.LoggerFactory

// Internal app config
//protected case class CaseStudyConfig(
//                                      inDir: String,
//                                      outDir: String,
//                                      timestamp1Ms: Long, // start of a time interval for analysis (ms)
//                                      timestamp2Ms: Long, // end of a time interval for analysis (ms)
//                                      twSize: Long, // time window size (ms)
//                                      percentile: Double // for the duration processor
//                                    )
//

protected class TemplateForBpi2017CaseStudy(
                                   //inDir: String = "D:\\logs\\bpi_challenge_2017",
                                   //outDir: String = "D:\\logs\\bpi_challenge_2017\\out",
                                   fromString: String = "02-12-2015 00:00:00",
                                   untilString: String = "02-03-2017 00:00:00",
                                   timeWindow: Long = Duration.ofDays(1).toMillis)
  extends AbstractSession("BPI 2017") {
  PropertyConfigurator.configure("./res/log4j.properties")
  val logger = LoggerFactory.getLogger(classOf[TemplateForBpi2017CaseStudy].getName)
  logger.info("App started")
  val appStartTime = new Date(); // for time measuring
  Config.toDisk(Config(-1, 0, "inDir"), "config_example.xml")
  val config = Config("config.xml")
  logger.info(config.toXml())
  val inDir = config.inDir


  val dateHelper = new CsvImportHelper("dd-MM-yyyy HH:mm:ss", CsvExportHelper.AmsterdamTimeZone) //to provide time intervals in code
  val csvExportHelper = new CsvExportHelper("dd-MM-yy HH.mm.ss.SSS", CsvExportHelper.AmsterdamTimeZone, ";") //for logs export
  val from = dateHelper.extractTimestamp(fromString)
  val until = dateHelper.extractTimestamp(untilString)
  //val caseStudyConfig = CaseStudyConfig(inDir, outDir, from, until, timeWindow, 0.5) // percentile

  //private val _ = new File(caseStudyConfig.outDir).mkdirs() // create out dir

  // provides events, lazy load
  val eventsOffers = OfferEvent.loadFromCsv(new CsvReader(), inDir)

  val srcLogOffers = UnifiedEventLog.create(eventsOffers)
    .map(TraceExpression().trimToTimeframe(from, until))
    //.persist(StorageLevel.MEMORY_ONLY)

//  val logOffersWithArtificialStartsStops =
//    logOffers
//      .map {
//        TraceExpression().withArtificialStartsStops(
//          OfferEvent.createArtificialEvent("Start", -caseStudyConfig.twSize, _: UnifiedTraceId, _: UnifiedEvent),
//          OfferEvent.createArtificialEvent("End", caseStudyConfig.twSize, _: UnifiedTraceId, _: UnifiedEvent)
//        )
//      }
//
//
//  val logSegmentsWithArtificialStartsStops =
//    logOffersWithArtificialStartsStops
//      .map(SegmentUtils.convertToSegments(SegmentUtils.DefaultSeparator, _: UnifiedTrace))
//

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
//  val processorConfig = SegmentProcessorConfig(spark,
//    logSegmentsWithArtificialStartsStops,
//    caseStudyConfig.timestamp1Ms,
//    caseStudyConfig.timestamp2Ms,
//    caseStudyConfig.twSize)

  //Map offered amounts to segments
//  val offeredAmountClazzBasedSegmentProcessor = new ClazzBasedSegmentProcessor[(String, UnifiedEvent)](
//    processorConfig,
//    OfferEvent.ClazzCount,
//    eventsOffers
//      .filter(_._2.attrs.getAs[Byte]("offeredAmountClass") != 0)
//      .distinct,
//    _._1,
//    _._2.timestamp,
//    _._2.attrs.getAs[Byte]("offeredAmountClass")
//  )
//

  //val logClassifiedOfferAmounts = offeredAmountClazzBasedSegmentProcessor.getClassifiedSegmentLog()

  //  val (segmentDf, statDf, durationProcessor) =
  //    DurationSegmentProcessor(processorConfig, caseStudyConfig.percentile)
  //
  //  val durationSegmentLog = durationProcessor.getClassifiedSegmentLog()
  //
  //
  //  val logMixed = logOffersWithArtificialStartsStops fullOuterJoin logClassifiedOfferAmounts

  val t = TraceExpression()

}
