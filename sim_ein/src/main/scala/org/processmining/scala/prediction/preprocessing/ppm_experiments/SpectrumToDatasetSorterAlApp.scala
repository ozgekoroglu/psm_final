package org.processmining.scala.prediction.preprocessing.ppm_experiments

import org.apache.log4j.PropertyConfigurator
import org.processmining.scala.log.utils.common.errorhandling.{EH, JvmParams}


object SpectrumToDatasetSorterAlApp extends T3PerformanceSpectrumSession {

  val spectrumRoot = "G:/T3/ps_aggr_3m"
  val datasetDir = "G:/psm_ml_server/data"
  val experimentName = "SorterAlOnlyStartForIncFlow_3m_all_agg_func"

  val dayStartOffsetHours = 10
  val dayDurationHours = 10
  val howFarInFutureBins = 5
  val historicalDataDurationBins = 3
  val binsPerLabel = 1

  val incomingFlowOffsetBins = -historicalDataDurationBins

  val incomingFlowDurationBins = historicalDataDurationBins + howFarInFutureBins

  val stateSegments = Set("Link-P50:Scanners-P50", "Scanners-P50:P-AL", "Scanners-P50:P50-TL", "Scanners-P50:Scanners-P50", "Scanners-P50:ToFS", "TL-P50:Scanners-P50", "VC-P50:Scanners-P50").toSeq
  val incomingFlowSegments = Set("L1_1:L1_2", "L2_1:L2_2", "L3_1:L3_2", "L4_1:L4_2", "TL:TL-P50").toSeq
  val labelSegment = "Scanners-P50:P-AL"

  def main(args: Array[String]): Unit = {
    PropertyConfigurator.configure("./log4j.properties")
    JvmParams.reportToLog(logger, s"${SpectrumToDatasetSorterAlApp.getClass} started")
    try {
      run()
    } catch {
      case e: Throwable =>
        logger.error(EH.formatError(e.toString, e))
    }
    logger.info(s"App is completed.")
  }
}