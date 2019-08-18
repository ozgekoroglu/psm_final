package org.processmining.scala.prediction.preprocessing.ppm_experiments

import org.apache.log4j.PropertyConfigurator
import org.processmining.scala.log.utils.common.errorhandling.{EH, JvmParams}


object SpectrumToDatasetP50 extends T3PerformanceSpectrumSession {

  val spectrumRoot = "G:/T3/csv_p50_1min_ps"
  val datasetDir = "G:/psm_ml2/data"
  val experimentName = "!p50_9"

  val dayStartOffsetHours = 10
  val dayDurationHours = 10
  val howFarInFutureBins = 2
  val historicalDataDurationBins = 5
  val binsPerLabel = 2

  val incomingFlowOffsetBins = -historicalDataDurationBins

  val incomingFlowDurationBins = incomingFlowOffsetBins + howFarInFutureBins

  val labelSegment = "P50-ScannersOut:Exit"

  val stateSegments =  labelSegment +:
    (Set(
      "P50-Link:ScannersIn",
      "P50-TL:ScannersIn",
      "P50-VC:ScannersIn",
      //"P50-VC:P50-2TL",
      "ScannersIn:P50-ScannersOut",
      "P50-Link:P50-2TL",
      "P40-Link:ScannersIn",
      "P50-ScannersOut:P50-2TL",
      "P40-ScannersOut::Exit"
      ) - labelSegment).toSeq

  val incomingFlowSegments =
    Set(
      "L4_1:L4_2",
      "TL:P50-TL",
      "7701.2.18:CheckIn",
      "7703.2.1:CheckIn",
      "7703.29.1:CheckIn",
      "7704.2.3:CheckIn",
      "7704.22.3:CheckIn",
      "7705.2.3:CheckIn",
      "7705.19.3:CheckIn",
      "7706.2.3:CheckIn",
      "7706.22.3:CheckIn",
      "7707.2.3:CheckIn",
      "7707.24.3:CheckIn",
      "7707.5.2:CheckIn",
      "7708.2.4:CheckIn",
      "7708.19.3:CheckIn",
      "7709.3.1:CheckIn").toSeq


  def main(args: Array[String]): Unit = {
    PropertyConfigurator.configure("./log4j.properties")
    JvmParams.reportToLog(logger, s"${SpectrumToDatasetCheckIn.getClass} started")
    try {
      run()
    } catch {
      case e: Throwable =>
        logger.error(EH.formatError(e.toString, e))
    }
    logger.info(s"App is completed.")
  }
}
