package org.processmining.scala.applications.mhs.bhs.t3.scenarios.spark

import java.io.File
import java.util.Date

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.processmining.scala.applications.common.spark.EvaluationHelper
import org.processmining.scala.applications.mhs.bhs.bpi.{FailedDirectionError, TaskReportEvent, TrackingReportEvent}
import org.processmining.scala.applications.mhs.bhs.t3.eventsources._
import org.processmining.scala.applications.mhs.bhs.t3.utils.RecirculationDetector
import org.processmining.scala.log.common.csv.spark.{CsvReader, CsvWriter}
import org.processmining.scala.log.common.enhancment.segments.common.{AbstractDurationClassifier, FasterNormal23VerySlowDurationClassifier, InventoryAggregation}
import org.processmining.scala.log.common.enhancment.segments.spark._
import org.processmining.scala.log.common.filtering.expressions.traces.impl.TraceExpression
import org.processmining.scala.log.common.types.Segment
import org.processmining.scala.log.common.unified.event.{CommonAttributeSchemas, UnifiedEvent}
import org.processmining.scala.log.common.unified.log.spark.UnifiedEventLog
import org.processmining.scala.log.common.unified.trace.UnifiedTraceId
import org.processmining.scala.log.common.utils.common.types.EventWithClazz

import scala.collection.immutable.SortedMap

protected case class Config(
                             inDir: String,
                             outDir: String,
                             timestamp1Ms: Long, // start of a time interval for analysis (ms)
                             timestamp2Ms: Long, // end of a time interval for analysis (ms)
                             twSize: Long, // time window size (ms)
                             percentile: Double, // for the duration processor
                             useArtificialStart: Boolean,
                             useArtificialEnd: Boolean
                           )

/**
  * Represents a session for the T3 LHR dataset
  *
  * @param inDir       path to input files
  * @param outDir      path to output files
  * @param fromString  time interval (from)
  * @param untilString time interval (until)
  * @param timeWindow  time window (ms)
  */
class T3Session(inDir: String, outDir: String, fromString: String, untilString: String, timeWindow: Long, additionalFilter: String => Boolean = _ => true) extends EvaluationHelper("T3") { // For implicit conversions like converting RDDs to DataFrames


  protected val bpiCsvHelper = new BpiCsvImportHelper(BpiCsvImportHelper.DefaultTimestampPattern, timeZone) //to import CSV exported from Oracle DB

  /** time interval (from) (ms) */
  val from = dateHelper.extractTimestamp(fromString)

  /** time interval (until) (ms) */
  val until = dateHelper.extractTimestamp(untilString)

  require(from < until, "'from' date must be earlier than 'until'")

  protected val config = Config(
    inDir,
    outDir,
    from,
    until,
    timeWindow,
    0.5, // percentile
    true,
    true
  )

  protected val bpiImporter = new BpiImporter(
    BpiImporterConfig(
      config.inDir,
      bpiCsvHelper,
      new CsvReader(), // helper
      config.twSize,
      additionalFilter))

  /** T3-specific mapping for availability of MC and scanners */
  val mcAndScannersAvailability = bpiImporter.readAvailability(spark)
    .map { x =>
      if (!x._1.isEmpty) // destination
        (x._1 match {
          case "51" => "7731.2.2_av"
          case "52" => "7731.5.2_av"
          case "53" => "7731.8.2_av"
          case "54" => "7732.2.2_av"
          case "55" => "7732.5.2_av"
          case "56" => "7732.8.2_av"
          case _ => ""
        }, x._2, x._3, x._4)
      else (s"${x._5}.${x._6}.${x._7}" match {
        case "7735.4.99" => "7735.4.99_av"
        case "7735.13.99" => "7735.13.99_av"
        case "7736.4.99" => "7736.4.99_av"
        case "7736.13.99" => "7736.13.99_av"
        case "7737.4.99" => "7737.4.99_av"
        case "7737.10.99" => "7737.10.99_av"
        case "7738.4.99" => "7738.4.99_av"
        case "7738.10.99" => "7738.10.99_av"
        case _ => ""
      }, x._2, x._3, x._4)

    }
    .filter(!_._1.isEmpty)
    .persist()

  private val _ = new File(config.outDir).mkdirs() // create out dir

  protected val eventsTrackingReport = bpiImporter.loadTrackingReportFromCsv(spark)

  protected val eventsPackageReport = bpiImporter.loadPackageReportFromCsv(spark)

  protected val eventsPackageReportWithEmptyFailedDirections = bpiImporter.loadPackageReportFromCsvWithEmptyFailedDirection(spark)

  protected val eventsTaskReport = bpiImporter.loadTaskType(spark)

  //  val logMovements = UnifiedEventLog.create(eventsTrackingReport union eventsPackageReport)
  //    .map(TraceExpression().trimToTimeframe(from, until))

  /** Event log of bags movement (WC_TRACKINGREPORT & WC_PACKAGEREPORT) */
  val logMovements = UnifiedEventLog.create(eventsTrackingReport union eventsPackageReportWithEmptyFailedDirections)
    .map(TraceExpression().trimToTimeframe(from, until))

  /** Event log of bags movement (WC_TRACKINGREPORT & WC_PACKAGEREPORT) */
  val logMovementsWoTimeFrameTrim = UnifiedEventLog.create(eventsTrackingReport union eventsPackageReportWithEmptyFailedDirections)



  /** Adds artificial starts and ends events to traces of a log of movements */
  def withArtificialStartsStops(log: UnifiedEventLog): UnifiedEventLog =
    log
      .map {
        TraceExpression().withArtificialStartsStops(
          TrackingReportEvent.createArtificialEvent("Start", -config.twSize, _: UnifiedTraceId, _: UnifiedEvent),
          TrackingReportEvent.createArtificialEvent("End", config.twSize, _: UnifiedTraceId, _: UnifiedEvent)
        )
      }

  /** Event log of bags movement (WC_TRACKINGREPORT & WC_PACKAGEREPORT) with artificial starts and ends */
  val logMovementsWithArtificialStartsStops = withArtificialStartsStops(logMovements)

  /** Event log of bags tasks (WC_TASKREPORT) */
  val logTasks = UnifiedEventLog.create(eventsTaskReport)
    .map(TraceExpression().trimToTimeframe(from, until))

  /** Log of segments */
  val logSegments = logMovements.map(org.processmining.scala.log.common.enhancment.segments.common.SegmentUtils.convertToSegments(SegmentUtils.DefaultSeparator, _: (UnifiedTraceId, List[UnifiedEvent])))

  /** Log of segments with artificial starts and ends */
  val logSegmentsWithArtificialStartsStops = logMovementsWithArtificialStartsStops
    .map(org.processmining.scala.log.common.enhancment.segments.common.SegmentUtils.convertToSegments(SegmentUtils.DefaultSeparator, _: (UnifiedTraceId, List[UnifiedEvent])))

  /** predefined config of segment processors */
  val processorConfig = SegmentProcessorConfig(spark,
    logSegmentsWithArtificialStartsStops,
    config.timestamp1Ms, config.timestamp2Ms,
    config.twSize)

  //def movements() = logMovements.persist()

  //def tasks() = logTasks.persist()


  /** creates DurationProcessor */
  def createDurationProcessor() = DurationSegmentProcessor(processorConfig, config.percentile)

  protected def helper(segments: RDD[Segment]): UnifiedEventLog =
    UnifiedEventLog.create(
      bpiImporter.loadAvailabilitySegments(mcAndScannersAvailability)
        .map(x => (x.id,
          UnifiedEvent(
            x.timestamp,
            x.key,
            SortedMap(CommonAttributeSchemas.AttrNameDuration -> x.duration),
            None
          )
        ))
    )


  /** export 6 views for PErformance Spectrum Miner (see the tutorial) */
  def report(adc: AbstractDurationClassifier = new FasterNormal23VerySlowDurationClassifier()): Unit = {

    val startTime = new Date(); // for time measuring

    logSegmentsWithArtificialStartsStops.persist(StorageLevel.MEMORY_AND_DISK)

    // Writing the complete event log to disk
    //TODO: move to HDFS
    //    CsvWriter.logToCsvLocalFilesystem(logMovementsWithArtificialStartsStops,
    //      config.outDir + "complete_log.csv",
    //      csvExportHelper.timestamp2String)

    val (segmentsDataframe, statDataframe, durationProcessor) = DurationSegmentProcessor(processorConfig, config.percentile, adc)
    //segmentDf.cache()
    //statDf.cache()

    //  Writing the complete segment log to disk
    //TODO: move to HDFS
    //CsvWriter.dataframeToLocalCsv(segmentDf, config.outDir + "segments.csv")


    //Joining descriptive stat with capacity (here we just add a dummy field, to provide compatibility with older versions)
    CsvWriter.dataframeToLocalCsv(spark
      .sql("""SELECT key, median, mean, std, 0 as capacity FROM stat""")
      .toDF("key", "median", "mean", "std", "capacity"),
      config.outDir + "/segmentsStatisticsCapacity.csv")


    //Use of various segment processors

    SegmentProcessor.toCsvV2(
      durationProcessor, config.outDir + "/duration/", durationProcessor.adc.legend)

    //logMovementsWithArtificialStartsStops.persist()

    SegmentProcessor.toCsvV2(
      new ClazzBasedSegmentProcessor[EventWithClazz](processorConfig, ParameterClassifiers.RecirculationClassifierClazzCount,
        RecirculationDetector.loadSorterRecirculationClazz(logMovementsWithArtificialStartsStops, "77[45]0.2.98")
          .map { x => EventWithClazz(x.id, x.timestamp, ParameterClassifiers.recirculationClassifier(x.clazz)) },
        _.id, _.timestamp, _.clazz
      ), config.outDir + "/pre_recirculations/",
      "RECIRCULATIONS ON PRESORTERS%0%1-3%4-6%7-9%10-12%13-15%16-18%19-21%22+")
    //.unpersist()

    eventsTrackingReport.persist(StorageLevel.MEMORY_AND_DISK)

    //    SegmentProcessor.toCsvV2(
    //      new NodesProcessor[(String, UnifiedEvent)](
    //        processorConfig.copy(segments = helper(bpiImporter.loadNodeSegments(spark))),
    //        FailedDirectionError.Last.id,
    //        eventsTrackingReport,
    //        _._1, _._2.timestamp, _._2.attrs.getAs[Byte]("failedDirectionClass")
    //      ), config.outDir + "/nodes/",
    //      "FAILED DIRECTION%OK%NOT ALLOWED - FLOW CONTROL%NO DIRECTION - LM ROUTING%NOT AVAILABLE OR FULL%NOT ALLOWED - BLOCKED BY LM%NOT ALLOWED - SECURITY%CAPACITY TOO HIGH%TECHNICAL FAILURE%NOT ALLOWED - FORCED DIR.%NOT ALLOWED - DIMENSIONS")
    //    //.unpersist()


    SegmentProcessor.toCsvV2(
      new NodesProcessor[(String, UnifiedEvent)](processorConfig,
        FailedDirectionError.Last.id,
        eventsTrackingReport,
        _._1, _._2.timestamp, _._2.getAs[Byte](TrackingReportSchema.AttrNameFailedDirectionClass)
      ), config.outDir + "/failed_directions/", "FAILED DIRECTION%OK%NOT ALLOWED - FLOW CONTROL%NO DIRECTION - LM ROUTING%NOT AVAILABLE OR FULL%NOT ALLOWED - BLOCKED BY LM%NOT ALLOWED - SECURITY%CAPACITY TOO HIGH%TECHNICAL FAILURE%NOT ALLOWED - FORCED DIR.%NOT ALLOWED - DIMENSIONS")
    //.unpersist()

    eventsTrackingReport.unpersist()

    val taskProcessor =
      new ClazzBasedSegmentProcessor[(String, UnifiedEvent)](processorConfig, TaskReportEvent.ClazzCount,
        eventsTaskReport,
        _._1, _._2.timestamp, _._2.getAs[Byte](TaskReportSchema.AttrNameTaskClass))
    SegmentProcessor.toCsvV2(taskProcessor, config.outDir + "/tasktype/",
      "TASK TYPES%n/a%ScreenL1L2%Store%RouteToMC%AutoScan%VolumeScan%ProblemBag/Store%ManualScan%Dump%Batch%DataCapture%Deregistration%LinkToFlight%LinkToHandler%ManualDeliver%ManualScreen%ManualStore%OperationalControl%Reclaim%Registration%Release%RequestAirportTag%RequestTask%Retag%RouteToCache%RouteToOutputPoint%ScreenL2%ScreenL4%SpecialDestination%Others")
    //.unpersist()

    SegmentProcessor.toCsvV2(
      new NodesProcessor[EventWithClazz](
        processorConfig.copy(segments = helper(bpiImporter.loadAvailabilitySegments(mcAndScannersAvailability))),
        BpiImporter.AvailabilityClazzCount,
        bpiImporter.loadAvailability(mcAndScannersAvailability),
        _.id, _.timestamp, _.clazz
      ), config.outDir + "/availability/", "AVAILABILITY%n/a%NOT-AVAILABLE%LOGGED_OFF%LOGGED_ON")

    import spark.implicits._
    SegmentProcessor.toCsvV2(
      new IdBasedSegmentProcessor(
        processorConfig.copy(segments = helper(bpiImporter.loadAvailabilitySegments(mcAndScannersAvailability))),
        bpiImporter.loadLateBagIdsFromCsv(spark).toDF("id")),
      config.outDir + "/late_bags/", "LATE BAGS%Normal bags%Late bags")

    println(s"""Pre-processing took ${(new Date().getTime - startTime.getTime) / 1000} seconds.""")


    //        SegmentProcessor.toCsv(
    //          new ClazzBasedSegmentProcessor[EventWithClazz](processorConfig.copy(outDir = config.outDir + "/fs_recirculations/",
    //            legend = "RECIRCULATIONS ON FINAL SORTERS%0%1-3%4-6%7-9%10-12%13-15%16-18%19-21%22+"),
    //            RecirculationDetector.loadSorterRecirculationClazz2[TrackingReportEvent](tracesWithArtificialStartsStops, _.pid, _.timestamp, _.location, "77[67]0.2.98")
    //              .map { x => EventWithClazz(x.id, x.timestamp, ParameterClassifiers.recirculationClassifier(x.clazz)) },
    //            _.id, _.timestamp, _.clazz
    //          ))
    //          .unpersist()
    //
    //        SegmentProcessor.toCsv(
    //          new ClazzBasedSegmentProcessor[EventWithClazz](processorConfig.copy(
    //            outDir = config.outDir + "/length/",
    //            legend = ""),
    //            bpiImporter.loadPackageInfoClazz(spark, "LENGTH", ParameterClassifiers.lengthClassifier),
    //            _.id, _.timestamp, _.clazz))
    //          .unpersist()
    //
    //        SegmentProcessor.toCsv(
    //          new ClazzBasedSegmentProcessor[EventWithClazz](processorConfig.copy(
    //            outDir = config.outDir + "/weight/",
    //            legend = ""),
    //            bpiImporter.loadPackageInfoClazz(spark, "CHECKEDWEIGHT", ParameterClassifiers.weightClassifier),
    //            _.id, _.timestamp, _.clazz))
    //          .unpersist()
    //
    //        SegmentProcessor.toCsv(
    //          new ClazzBasedSegmentProcessor[EventWithClazz](processorConfig.copy(
    //            outDir = config.outDir + "/screening_level/",
    //            legend = "SCREENING LEVELS%n/a%ScreeningLevel=1,CLEARED%ScreeningLevel=1,NODECISION%ScreeningLevel=1,UNCLEARED%ScreeningLevel=2,CLEARED%ScreeningLevel=2,UNCLEARED%Everything else"),
    //            bpiImporter.loadPackageInfoClazz(spark, "L_SCREENINGRESULT", ParameterClassifiers.screeningClassifier),
    //            _.id, _.timestamp, _.clazz))
    //          .unpersist()
    //
    //
    //
    //        SegmentProcessor.toCsv(
    //          new IdBasedSegmentProcessor(processorConfig.copy(
    //            outDir = config.outDir + "/late_bags/",
    //            legend = "LATE BAGS%Normal bags%Late bags"),
    //            bpiImporter.loadLateBagIdsFromCsv(spark).toDF("id")))
    //          .unpersist()
  }

}