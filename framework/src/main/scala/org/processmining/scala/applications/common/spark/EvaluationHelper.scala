package org.processmining.scala.applications.common.spark

import java.io.File
import java.time.ZoneId
import java.time.temporal.ChronoUnit
import java.util.Date

import org.processmining.scala.log.common.csv.spark.CsvWriter
import org.processmining.scala.log.common.enhancment.segments.common._
import org.processmining.scala.log.common.enhancment.segments.spark._
import org.processmining.scala.log.common.unified.event.{CommonAttributeSchemas, UnifiedEvent}
import org.processmining.scala.log.common.unified.log.spark.UnifiedEventLog
import org.processmining.scala.log.common.unified.trace.UnifiedTraceId
import org.processmining.scala.log.common.utils.common.DateRounding
import org.processmining.scala.log.common.utils.spark.AbstractSession
import org.processmining.scala.log.common.xes.parallel.{XesReader, XesWriter}
import org.processmining.scala.log.utils.common.csv.common.{CsvExportHelper, CsvImportHelper}
import org.processmining.scala.log.utils.common.errorhandling.JvmParams
import org.slf4j.LoggerFactory

class EvaluationHelper(name: String) extends AbstractSession(name) {

  @transient
  lazy val logger = LoggerFactory.getLogger(classOf[EvaluationHelper].getClass)

  {
    spark.sparkContext.setLogLevel("INFO")
    println("Spark logging level is set to 'INFO'")
    JvmParams.reportToLog(logger, s"$name session is started")
  }

  protected val appStartTime = new Date(); // for time measuring

  // Internal app config
  protected val timeZone = CsvExportHelper.UtcTimeZone

  /** helper to provide time intervals in code */
  val dateHelper = new CsvImportHelper("dd-MM-yyyy HH:mm:ss", timeZone)

  protected val csvExportHelper = new CsvExportHelper(CsvExportHelper.DefaultTimestampPattern,
    timeZone,
    CsvExportHelper.DefaultSeparator) //to export logs


  /** imports a log from a XES file */
  def loadXesLog(logFilename: String): UnifiedEventLog = loadXesLog(logFilename, UnifiedEventLog.DefaultNumSlices, " ")

  /**
    * imports a log from a XES file
    *
    * @param logFilename        filename
    * @param numSlices          see SparkContext.parallelize
    * @param sep                separator between parts of activity names
    * @param activityClassifier activity classifier: a list of mandatory attributes from which activity names should be created
    * @return event log
    */
  def loadXesLog(logFilename: String, numSlices: Int, sep: String, activityClassifier: String*): UnifiedEventLog = {
    val logs = XesReader.read(new File(logFilename), None, None, XesReader.DefaultTraceAttrNamePrefix, sep, activityClassifier: _*)
    if (logs.nonEmpty) convertLog(logs.head, numSlices)
    else {
      logger.warn(s"Log $logFilename is empty")
      UnifiedEventLog.createEmpty(spark)
    }
  }

  private def convertLog(originalLog: org.processmining.scala.log.common.unified.log.parallel.UnifiedEventLog, numSlices: Int): UnifiedEventLog =
    UnifiedEventLog.fromTraces(originalLog.traces(), spark, numSlices)

  /**
    * exports a log of events into a CSV file
    *
    * @param unifiedEventLog log
    * @param filename        name of file
    * @param attrs           names of event attributes to be exported
    */
  def export(unifiedEventLog: UnifiedEventLog, filename: String, attrs: Set[String]): Unit =
    export(unifiedEventLog, filename, attrs.toArray: _*)

  /**
    * exports a log of events into a CSV file
    *
    * @param unifiedEventLog log
    * @param filename        name of file
    * @param attrs           names of event attributes to be exported
    */
  def export(unifiedEventLog: UnifiedEventLog, filename: String, attrs: String*): Unit = {
    CsvWriter.logToCsvLocalFilesystem(unifiedEventLog,
      filename,
      csvExportHelper.timestamp2String,
      attrs: _*)
  }

  /** exports a log of aggregated events into a CSV file */
  def exportAggregated(unifiedEventLog: UnifiedEventLog, filename: String) = export(unifiedEventLog, filename, CommonAttributeSchemas.AttrNameDuration, CommonAttributeSchemas.AttrNameSize)

  def reportDurationsForWeeks(log: UnifiedEventLog, rootDir: String, twSize: Long, efr: Boolean, zoneId: ZoneId = ZoneId.of(CsvExportHelper.AmsterdamTimeZone), adc: AbstractDurationClassifier = new Q4DurationClassifier): Unit =
    reportDurations(log, rootDir, twSize, DateRounding.truncatedToWeeks(zoneId, _), adc, efr)

  def reportDurationsForDays(log: UnifiedEventLog, rootDir: String, twSize: Long, efr: Boolean, zoneId: ZoneId = ZoneId.of(CsvExportHelper.AmsterdamTimeZone), adc: AbstractDurationClassifier = new Q4DurationClassifier): Unit =
    reportDurations(log, rootDir, twSize, DateRounding.truncatedTo(zoneId, ChronoUnit.DAYS,  _), adc, efr)

  def reportDurationsForHours(log: UnifiedEventLog, rootDir: String, twSize: Long, efr: Boolean, zoneId: ZoneId = ZoneId.of(CsvExportHelper.AmsterdamTimeZone), adc: AbstractDurationClassifier = new Q4DurationClassifier): Unit =
    reportDurations(log, rootDir, twSize, DateRounding.truncatedTo(zoneId, ChronoUnit.HOURS,  _), adc, efr)

  def reportDurationsForMonths(log: UnifiedEventLog, rootDir: String, twSize: Long, efr: Boolean, zoneId: ZoneId = ZoneId.of(CsvExportHelper.AmsterdamTimeZone), adc: AbstractDurationClassifier = new Q4DurationClassifier): Unit =
    reportDurations(log, rootDir, twSize, DateRounding.truncatedTo(zoneId, ChronoUnit.MONTHS,  _), adc, efr)

//  def reportAttributeForDays(log: UnifiedEventLog, rootDir: String, twSize: Long, attrName: String, legend: String, zoneId: ZoneId = ZoneId.of(CsvExportHelper.AmsterdamTimeZone), adc: AbstractDurationClassifier = new Q4DurationClassifier): Unit =
//    reportAttribute(log, rootDir, twSize, attrName, legend), adc) /// VD доделай!!!!!!!!!!

  /**
    * Calculate duration-based view for PSM
    *
    * @param log     log
    * @param rootDir output directory
    * @param twSize  size of time windows (ms)
    * @param adc classifier
    */
  @deprecated
  def reportDurations(log: UnifiedEventLog, rootDir: String, twSize: Long, adc: AbstractDurationClassifier = new Q4DurationClassifier): Unit =
    reportDurations(log, rootDir, twSize, x => x, adc, false)


  def reportDurations(log: UnifiedEventLog, rootDir: String, twSize: Long, left: Long => Long, adc: AbstractDurationClassifier, efr: Boolean): Unit =
    reportDurations(log, rootDir, twSize, left, adc, efr, InventoryAggregation)


  def reportDurations(log: UnifiedEventLog, rootDir: String, twSize: Long, left: Long => Long, adc: AbstractDurationClassifier, efr: Boolean, aaf: AbstractAggregationFunction): Unit = {
    val startTime = new Date(); // for time measuring
    log.persist()
    val initialBoundaries = log.minMaxTimestamp()
    val (timestamp1Ms, timestamp2Ms) = (left(initialBoundaries._1), initialBoundaries._2)
    val dir = s"$rootDir/duration/"
    logger.info(s"Log for $dir has timestamps from ${new Date(timestamp1Ms)} to ${new Date(timestamp2Ms)}")
    val segments = log.map(
      if(efr) org.processmining.scala.log.common.enhancment.segments.spark.SegmentUtils.convertToEFSegments(org.processmining.scala.log.common.enhancment.segments.common.SegmentUtils.DefaultSeparator, _: (UnifiedTraceId, List[UnifiedEvent]))
      else org.processmining.scala.log.common.enhancment.segments.common.SegmentUtils.convertToSegments(org.processmining.scala.log.common.enhancment.segments.common.SegmentUtils.DefaultSeparator, _: (UnifiedTraceId, List[UnifiedEvent]))
    )
    log.unpersist()
    val processorConfig = SegmentProcessorConfig(spark,
      segments,
      timestamp1Ms, timestamp2Ms,
      twSize,
      aaf)
    val (_, _, durationProcessor) = DurationSegmentProcessor(processorConfig, 0.5, adc)
    (new File(dir)).mkdirs()
    SegmentProcessor.toCsvV2(durationProcessor, dir, durationProcessor.adc.legend)
    println(s"""Pre-processing took ${(new Date().getTime - startTime.getTime) / 1000} seconds.""")
  }



  //TODO: use attrName!
  def reportAttribute(log: UnifiedEventLog, rootDir: String, twSize: Long, attrName: String, legend: String) = {
    val startTime = new Date(); // for time measuring
    log.persist()
    val (timestamp1Ms, timestamp2Ms) = log.minMaxTimestamp()
    val dir = s"$rootDir/${attrName.replace(':', '_')}/"
    (new File(dir)).mkdirs()
    logger.info(s"Log for $dir has timestamps from ${new Date(timestamp1Ms)} to ${new Date(timestamp2Ms)}")
    val segments = log.map(org.processmining.scala.log.common.enhancment.segments.spark.SegmentUtils.convertToOneActivitySegments(org.processmining.scala.log.common.enhancment.segments.common.SegmentUtils.DefaultSeparator, _: (UnifiedTraceId, List[UnifiedEvent])))

    val processorConfig = SegmentProcessorConfig(spark,
      segments,
      timestamp1Ms, timestamp2Ms,
      twSize)

    SegmentProcessor.toCsvV2(
      new NodesProcessor[(UnifiedTraceId, UnifiedEvent)](
        processorConfig,
        5,
        log.events(),
        _._1.id, _._2.timestamp, (_) => 0
      ), dir,
      legend)

    log.unpersist()
    println(s"""Pre-processing took ${(new Date().getTime - startTime.getTime) / 1000} seconds.""")
  }
  def exportXes(log: org.processmining.scala.log.common.unified.log.parallel.UnifiedEventLog, filename: String): Unit = {
    logger.info(s"Exporting $filename")
    XesWriter.write(log, filename, XesReader.DefaultTraceAttrNamePrefix)
  }

  def exportXes(log: UnifiedEventLog, filename: String): Unit =
    exportXes(org.processmining.scala.log.common.unified.log.spark.UnifiedEventLog.create(log), filename)


}
