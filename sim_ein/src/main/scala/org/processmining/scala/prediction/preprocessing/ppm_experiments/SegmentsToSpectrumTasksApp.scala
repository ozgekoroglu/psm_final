package org.processmining.scala.prediction.preprocessing.ppm_experiments

//class SegmentsToSpectrumTasksApp {

import org.apache.log4j.PropertyConfigurator
import org.processmining.scala.log.common.enhancment.segments.common.{DummyDurationClassifier, NormalSlowDurationClassifier, NormalSlowVerySlowDurationClassifier}
import org.processmining.scala.log.utils.common.errorhandling.{EH, JvmParams}
import org.processmining.scala.viewers.spectrum.builder.AbstractSegmentsToSpectrumSession


object SegmentsToSpectrumTasksApp extends AbstractSegmentsToSpectrumSession {

  val SegmentsPath = "g:/tasks/segments"
  val SpectrumRoot = "g:/tasks/ps_60s"
  val DatasetSizeDays = 181
  val startTime = "27-09-2017 00:00:00.000"
  val twSizeMs = 60 * 1000
  override val classifier = new NormalSlowVerySlowDurationClassifier

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

