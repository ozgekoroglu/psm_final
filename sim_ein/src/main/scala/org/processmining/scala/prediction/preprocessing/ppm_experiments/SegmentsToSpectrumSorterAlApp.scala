package org.processmining.scala.prediction.preprocessing.ppm_experiments

import org.apache.log4j.PropertyConfigurator
import org.processmining.scala.log.utils.common.errorhandling.{EH, JvmParams}
import org.processmining.scala.viewers.spectrum.builder.AbstractSegmentsToSpectrumSession


object SegmentsToSpectrumSorterAlApp extends AbstractSegmentsToSpectrumSession {

  val SegmentsPath = "G:\\T3\\csv"
  val SpectrumRoot = "G:/T3/ps_aggr_3m"
  val DatasetSizeDays = 186
  val startTime = "27-09-2017 00:00:00.000"
  val twSizeMs = 3 * 60 * 1000

  def main(args: Array[String]): Unit = {
    PropertyConfigurator.configure("./log4j.properties")
    JvmParams.reportToLog(logger, s"${SegmentsToSpectrumSorterAlApp.getClass} started")
    try {
      run()
    } catch {
      case e: Throwable => logger.error(EH.formatError(e.toString, e))
    }
    logger.info(s"App is completed.")
  }

}

