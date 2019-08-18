package org.processmining.scala.prediction.preprocessing.ppm_experiments

import java.io.File

import org.apache.log4j.PropertyConfigurator
import org.processmining.scala.log.utils.common.errorhandling.{EH, JvmParams}
import org.processmining.scala.prediction.preprocessing.t3.common.T3LogsToSegments
import org.processmining.scala.viewers.spectrum.builder.Logs2Segments
import org.slf4j.LoggerFactory



object T3LogsToSegmentsCode {
  private val logger = LoggerFactory.getLogger(T3LogsToSegmentsCode.getClass)

  def main(args: Array[String]): Unit = {
    PropertyConfigurator.configure("./log4j.properties")
    JvmParams.reportToLog(logger, "T3LogsToSegmentsApp started")
    try {
      val eventLogPath = "g:/tasks"
      val aggregationIniFilename = ".\\sim_ein\\aggregation\\empty.ini"
      val segmentsPath = "g:/tasks/segments"
      val exportAggregatedLog = false // hardcoded
      logger.info(s"Event log path='$eventLogPath'")
      val dir = new File(eventLogPath)
      val files = dir.listFiles().filter(_.getName.toLowerCase.endsWith(".csv")).map(_.getPath)
      val config = new T3LogsToSegments(eventLogPath, aggregationIniFilename, exportAggregatedLog)
      Logs2Segments.start(files.toList, config.factoryOfFactory, exportAggregatedLog, config.aggregate, segmentsPath)
    } catch {
      case e: Throwable =>
        logger.error(EH.formatError(e.toString, e))
    }

    logger.info(s"App is completed.")
  }
}

