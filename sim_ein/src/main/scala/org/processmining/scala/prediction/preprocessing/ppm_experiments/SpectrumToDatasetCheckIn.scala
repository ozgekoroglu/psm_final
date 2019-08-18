package org.processmining.scala.prediction.preprocessing.ppm_experiments

import java.io.PrintWriter

import org.apache.log4j.PropertyConfigurator
import org.processmining.scala.log.utils.common.errorhandling.{EH, JvmParams}
import org.processmining.scala.viewers.spectrum.features.SpectrumToDataset


object SpectrumToDatasetCheckIn extends T3PerformanceSpectrumSession {

  val spectrumRoot = "G:/T3/csv_check_in_ps_detailed_60"
  val datasetDir = "G:/psm_ml2/data"
  val experimentName = "17_02_Link2Scanners_2f"

  val dayStartOffsetHours = 7
  val dayDurationHours = 13
  val howFarInFutureBins = 3
  val historicalDataDurationBins = 6
  val binsPerLabel = 2

  val labelSegment = "P50-Link:ScannersIn"

  val incomingFlowOffsetBins = -historicalDataDurationBins

  val incomingFlowDurationBins = historicalDataDurationBins + howFarInFutureBins

  val stateSegments = Seq(
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
    "7709.3.1:CheckIn")



//  val stateSegments = Seq() //labelSegment +:
//  //    (Set(
//  //      "P50-Link:ScannersIn",
//  //      "P50-ScannersOut:P50-AL",
//  //      "P50-ScannersOut:Link-To-FS",
//  //      "P50-ScannersOut:P50-LOOP",
//  //      "P50-AL:Link-To-AL",
//  //      "P50-AL:P50-LOOP",
//  //      "P50-LOOP:P50-LOOP",
//  //      "P50-Link:P50-MC",
//  //      "P50-LOOP:P50-MC") - labelSegment).toSeq

  val incomingFlowSegments = Seq()
//    Seq(
//      "L4_1:L4_2",
//      "TL:P50-TL",
//      "7701.2.18:CheckIn",
//      "7703.2.1:CheckIn",
//      "7703.29.1:CheckIn",
//      "7704.2.3:CheckIn",
//      "7704.22.3:CheckIn",
//      "7705.2.3:CheckIn",
//      "7705.19.3:CheckIn",
//      "7706.2.3:CheckIn",
//      "7706.22.3:CheckIn",
//      "7707.2.3:CheckIn",
//      "7707.24.3:CheckIn",
//      "7707.5.2:CheckIn",
//      "7708.2.4:CheckIn",
//      "7708.19.3:CheckIn",
//      "7709.3.1:CheckIn").toSeq

  override protected def processIncomingFlow(incomingFlowFeatures: Seq[Seq[Double]], isEvaluationDataset: Boolean) = {
    val size = incomingFlowSegments.size
    val baseline2 =
      incomingFlowFeatures.map(row => {
        val zipped = row.zipWithIndex
        (
          zipped.filter(x => x._2 >= (howFarInFutureBins - 3) * size && x._2 < (howFarInFutureBins - 2) * size).map(_._1).sum +
            zipped.filter(x => x._2 >= (howFarInFutureBins - 2) * size && x._2 < (howFarInFutureBins - 1) * size).map(_._1).sum
          ) / 2
        //zipped.filter(_._2 < incomingFlowSegments.size).map(_._1).sum
      }
      )
    logger.info(s"Exporting baseline2...")
    val pw = new PrintWriter(s"${if (isEvaluationDataset) testDatasetDir else trainingDatasetDir}/baseline2.csv")
    baseline2.foreach(x => pw.println(SpectrumToDataset.rowToString(Seq(x))))
    pw.close()
  }

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
