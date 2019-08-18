package org.processmining.scala.applications.mhs.bhs.sat.scenarios.spark

import java.util.Date

import org.processmining.scala.applications.mhs.bhs.sat.eventsources.{DestinationStateLookup, ExceptionIdLookup, ExitStateLookup, FileImporter}
import org.processmining.scala.log.common.csv.spark.CsvWriter
import org.processmining.scala.log.common.enhancment.segments.common.SegmentUtils
import org.processmining.scala.log.common.enhancment.segments.spark.{DurationSegmentProcessor, NodesProcessor, SegmentProcessor, SegmentProcessorConfig}
import org.processmining.scala.log.common.unified.event.UnifiedEvent
import org.processmining.scala.log.common.unified.log.spark.UnifiedEventLog
import org.processmining.scala.log.common.unified.trace.UnifiedTraceId
import org.processmining.scala.log.common.utils.spark.AbstractSession
import org.processmining.scala.log.utils.common.csv.common.{CsvExportHelper, CsvImportHelper}
import org.processmining.scala.log.utils.common.errorhandling.JvmParams
import org.slf4j.LoggerFactory


class SATSessionTemplate(inDir: String, outDir: String, twSize: Long) extends AbstractSession("SAT") {
  @transient
  lazy val logger = LoggerFactory.getLogger(classOf[SATSessionTemplate].getName)

  {
    spark.sparkContext.setLogLevel("INFO")
    println("Spark logging level is set to 'INFO'")
    JvmParams.reportToLog(logger, "SAT Session started")
  }

  protected val appStartTime = new Date(); // for time measuring

  // Internal app config
  protected val timeZone = CsvExportHelper.UtcTimeZone

  protected val csvExportHelper = new CsvExportHelper(CsvExportHelper.DefaultTimestampPattern,
    timeZone,
    CsvExportHelper.DefaultSeparator) //to export logs

  /** helper to provide time intervals in code */
  val dateHelper = new CsvImportHelper("dd-MM-yyyy HH:mm:ss", timeZone)

  val fileImporter = new FileImporter(inDir)
  val sortReport = fileImporter.loadSortReport(spark)
  val destinationReply = fileImporter.loadDestinationReply(spark)
  val destinationRequest = fileImporter.loadDestinationRequest(spark)

  val logSortReport = UnifiedEventLog.create(sortReport.map(x => (x.getAs[String]("LPC"), x)))
  val logDestinationReply = UnifiedEventLog.create(destinationReply.map(x => (x.getAs[String]("LPC"), x)))
  val logDestinationRequest = UnifiedEventLog.create(destinationRequest.map(x => (x.getAs[String]("LPC"), x)))

  val logSegmentsSortReport = logSortReport
    .map(SegmentUtils.convertToSegments(SegmentUtils.DefaultSeparator, _: (UnifiedTraceId, List[UnifiedEvent])))

  val logSegmentsdestinationReply = logDestinationReply
    .map(SegmentUtils.convertToSegments(SegmentUtils.DefaultSeparator, _: (UnifiedTraceId, List[UnifiedEvent])))

  val logSegmentsSDestinationRequest = logDestinationRequest
    .map(SegmentUtils.convertToSegments(SegmentUtils.DefaultSeparator, _: (UnifiedTraceId, List[UnifiedEvent])))

  val log = logSortReport fullOuterJoin logDestinationReply fullOuterJoin logDestinationRequest

  val logSegments = log
    .map(SegmentUtils.convertToSegments(SegmentUtils.DefaultSeparator, _: (UnifiedTraceId, List[UnifiedEvent])))

  def export(unifiedEventLog: UnifiedEventLog, filename: String, attrs: String*): Unit = {
    CsvWriter.logToCsvLocalFilesystem(unifiedEventLog,
      filename,
      csvExportHelper.timestamp2String,
      attrs: _*)
  }


  /** export 6 views for PErformance Spectrum Miner (see the tutorial) */
  def report(): Unit = {

    val startTime = new Date(); // for time measuring

    val (timestamp1Ms, timestamp2Ms) = logSegments.minMaxTimestamp()



    // Writing the complete event log to disk
    //TODO: move to HDFS
    //    CsvWriter.logToCsvLocalFilesystem(logMovementsWithArtificialStartsStops,
    //      config.outDir + "complete_log.csv",
    //      csvExportHelper.timestamp2String)

    val (segmentsDataframe, statDataframe, durationProcessor) = DurationSegmentProcessor(SegmentProcessorConfig(spark,
      logSegments,
      timestamp1Ms, timestamp2Ms,
      twSize), 0.5)
    //segmentDf.cache()
    //statDf.cache()

    //  Writing the complete segment log to disk
    //TODO: move to HDFS
    //CsvWriter.dataframeToLocalCsv(segmentDf, config.outDir + "segments.csv")


    //Joining descriptive stat with capacity (here we just add a dummy field, to provide compatibility with older versions)
    CsvWriter.dataframeToLocalCsv(spark
      .sql("""SELECT stat.*, 0 as capacity FROM stat""")
      .toDF("key", "median", "mean", "std", "capacity"),
      outDir + "/segmentsStatisticsCapacity.csv")


    //Use of various segment processors

    SegmentProcessor.toCsvV2(
      durationProcessor, outDir + "/duration/", durationProcessor.adc.legend)


    SegmentProcessor.toCsvV2(
      new NodesProcessor[(String, UnifiedEvent)](SegmentProcessorConfig(spark,
        logSegmentsSortReport,
        timestamp1Ms, timestamp2Ms,
        twSize),
        DestinationStateLookup.Last,
        logSortReport.events().map(x => (x._1.id, x._2)),
        _._1, _._2.timestamp, _._2.getAs[Byte](DestinationStateLookup.DestinationStateClass)
      ), outDir + "/sort_report/", "Destination State%OK%Unknown reason for not sorting%Sorted to / arrived at destination%Destination not valid (unreachable)%Destination full%Failed to divert%No destination%Destination not available%Gap too small%Item too long%Capacity limitation%Unknown")


    SegmentProcessor.toCsvV2(
      new NodesProcessor[(String, UnifiedEvent)](SegmentProcessorConfig(spark,
        logSegmentsdestinationReply,
        timestamp1Ms, timestamp2Ms,
        twSize),
        ExceptionIdLookup.Last,
        logDestinationReply.events().map(x => (x._1.id, x._2)),
        _._1, _._2.timestamp, _._2.getAs[Byte](ExceptionIdLookup.ExceptionIdClass)
      ), outDir + "/reply_exceptions/", "Exception descriptions%No Exception, Normal Sort%No BSM%Unkown Flight%No Chute%No Airline%No Sortation Mode%Inbound BSM%Too Early%To Late%HOT%Early%Invalid Chute%No Read%Invalid Code%Multi Read%Different Code Read%Fall Back Tag%Max Recirculation%Destination Disabled%MCS Destination%Unhandled Exception%Unknown")

    SegmentProcessor.toCsvV2(
      new NodesProcessor[(String, UnifiedEvent)](SegmentProcessorConfig(spark,
        logSegmentsSortReport,
        timestamp1Ms, timestamp2Ms,
        twSize),
        DestinationStateLookup.Last,
        logSortReport.events().map(x => (x._1.id, x._2)),
        _._1, _._2.timestamp, _._2.getAs[Byte](ExitStateLookup.ExitStateClass)
      ), outDir + "/exit_state/", "Exit State%Arrived at location (Sorted)%Lost in tracking%No exit%Unknown")

    println(s"""Pre-processing took ${(new Date().getTime - startTime.getTime) / 1000} seconds.""")

  }


}
