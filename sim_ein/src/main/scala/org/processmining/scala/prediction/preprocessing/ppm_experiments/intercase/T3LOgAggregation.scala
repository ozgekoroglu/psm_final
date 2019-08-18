package org.processmining.scala.prediction.preprocessing.ppm_experiments.intercase

import java.io.File
import java.util.concurrent.{Callable, Executors, TimeUnit}

import org.apache.log4j.PropertyConfigurator
import org.processmining.scala.applications.mhs.bhs.t3.eventsources.{BpiCsvImportHelper, BpiImporter, BpiImporterConfig}
import org.processmining.scala.log.common.csv.common.CsvReader
import org.processmining.scala.log.common.csv.parallel.CsvWriter
import org.processmining.scala.log.common.filtering.expressions.events.regex.impl.EventEx
import org.processmining.scala.log.common.filtering.expressions.traces.impl.TraceExpression
import org.processmining.scala.log.common.unified.event.UnifiedEvent
import org.processmining.scala.log.common.unified.log.parallel.UnifiedEventLog
import org.processmining.scala.log.common.unified.trace.UnifiedEventAggregator
import org.processmining.scala.log.common.utils.common.EventAggregatorImpl
import org.processmining.scala.log.utils.common.csv.common.CsvExportHelper
import org.processmining.scala.log.utils.common.errorhandling.{EH, JvmParams}
import org.slf4j.LoggerFactory

class T3LogAggregation(dir: String, additionalFilter: String => Boolean, aggregationIniFilename: String, outputFilename: String) extends Callable[String] {

  private val logger = LoggerFactory.getLogger(classOf[T3LogAggregation])
  private val t = TraceExpression()
  protected val bpiCsvHelper = new BpiCsvImportHelper(BpiCsvImportHelper.DefaultTimestampPattern, CsvExportHelper.LondonTimeZone) //to import CSV exported from Oracle DB
  private val bpiImporter = new BpiImporter(
    BpiImporterConfig(
      dir,
      bpiCsvHelper,
      new org.processmining.scala.log.common.csv.spark.CsvReader(), // helper
      60000L, // does not matter here
      additionalFilter))


  def trackingAndPackageReportFactory(wcPackageReportHeader: Array[String]): Array[String] => (String, UnifiedEvent) = {
    bpiImporter.trackingReportEntryFactory(wcPackageReportHeader.indexOf("PID"),
      wcPackageReportHeader.indexOf("EVENTTS"),
      wcPackageReportHeader.indexOf("AREAID"), wcPackageReportHeader.indexOf("ZONEID"), wcPackageReportHeader.indexOf("EQUIPMENTID"))
  }

  def loadTrackingAndPackageReportFromCsv(): UnifiedEventLog = {

    val filenameList = bpiImporter.getFilenamesArray(bpiImporter.filenames.wcTrackingreport).toList ::: bpiImporter.getFilenamesArray(bpiImporter.filenames.wcPackagereport).toList
    val csvReader = new CsvReader()
    val traces = filenameList.flatMap { filename =>
      val (wcPackageReportHeader, wcPackageReportLines) = csvReader.read(filename)
      csvReader.parse(
        wcPackageReportLines,
        trackingAndPackageReportFactory(wcPackageReportHeader)
      )
    }.filter(_._1 != "0") //removing all events with zero PID (e.g. events from tubs)
    UnifiedEventLog.create(traces.par)

  }


  def aggregate(log: UnifiedEventLog): UnifiedEventLog = {
    val aggregation = new EventAggregatorImpl(aggregationIniFilename)
    val exAggregation = t aggregate UnifiedEventAggregator(aggregation)
    log
      .map(exAggregation)
      .remove(EventEx("ToBeDeleted"))
  }


  override def call(): String = {
    val log = aggregate(loadTrackingAndPackageReportFromCsv())
    CsvWriter.logToCsvLocalFilesystem(log, outputFilename, new CsvExportHelper(CsvExportHelper.FullTimestampPattern, CsvExportHelper.AmsterdamTimeZone, ",").timestamp2String)
    logger.info(s"$outputFilename is done.")
    outputFilename
  }
}

object T3LogAggregation {
  private val logger = LoggerFactory.getLogger(T3LogAggregation.getClass)

  def main(args: Array[String]): Unit = {
    PropertyConfigurator.configure("./log4j.properties")
    JvmParams.reportToLog(logger, "T3LogAggregation started")
    try {
      val dir = "G:\\full_converted"
      val aggregationIniFilename = ".\\sim_ein\\aggregation\\check_in_17.ini"
      val outDir = "G:\\PI1"

      val dirObj = new File(dir)
      val files = dirObj.listFiles().filter(_.getName.toLowerCase.endsWith(".csv"))
        .map(_.getName)
        .map(_.substring(0, 8))
        .distinct
        .map(prefix => new T3LogAggregation(dir, x => x.startsWith(prefix), aggregationIniFilename, s"$outDir/$prefix.csv"))

      val poolSize = 16
      val executorService = Executors.newFixedThreadPool(poolSize)
      val futures = files.map(executorService.submit(_))
      logger.info("All tasks are submitted.")
      futures.foreach(_.get())
      logger.info("All tasks are done.")
      executorService.shutdown()
      while (!executorService.awaitTermination(100, TimeUnit.MILLISECONDS)) {}
      logger.info("Thread pool is terminated.")
    } catch {
      case e: Throwable =>
        logger.error(EH.formatError(e.toString, e))
    }

    logger.info(s"App is completed.")
  }
}
