package org.processmining.scala.prediction.preprocessing.ppm_experiments

import org.apache.log4j.PropertyConfigurator
import org.processmining.scala.log.common.enhancment.segments.common.{DummyDurationClassifier, NormalSlowDurationClassifier}
import org.processmining.scala.log.utils.common.errorhandling.{EH, JvmParams}
import org.processmining.scala.viewers.spectrum.builder.AbstractSegmentsToSpectrumSession


object SegmentsToSpectrumCheckInApp extends AbstractSegmentsToSpectrumSession {

  val SegmentsPath = "G:\\T3_v7\\csv_al"
  val SpectrumRoot = "G:\\T3_v7\\csv_AL_debug_Dec_ps_15s"
  val DatasetSizeDays = 11
  val startTime = "20-12-2017 00:00:00.000"
  val twSizeMs = 15 * 1000
  override val classifier = new DummyDurationClassifier

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
