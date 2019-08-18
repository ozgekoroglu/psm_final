package org.processmining.scala.prediction.preprocessing.ppm_experiments

import org.apache.log4j.PropertyConfigurator
import org.processmining.scala.log.utils.common.errorhandling.{EH, JvmParams}


object SpectrumToDatasetPreSorter extends T3PerformanceSpectrumSession {

  val spectrumRoot = "G:\\T3_v7\\csv_al_ps_2"
  val datasetDir = "G:/psm_ml2/data"
  val experimentName = "!al_33"

  val dayStartOffsetHours = 10
  val dayDurationHours = 10
  val howFarInFutureBins = 4
  val historicalDataDurationBins = 7
  val binsPerLabel = 3

  val labelSegment = "7750.38.95:P50-LOOP"

  val incomingFlowOffsetBins = -historicalDataDurationBins

  val incomingFlowDurationBins = historicalDataDurationBins + howFarInFutureBins

  val incomingFlowSegments = Set("P50-ScannersOut:7750.38.97", "P50-ScannersOut:7750.38.98", "P40-ScannersOut:7740.38.97", "P40-ScannersOut:7740.38.98",
    "ScannersIn:P50-ScannersOut"
  ).toSeq


  val stateSegments = //labelSegment +:
    (Set(
      "7740.38.98:7852.9.1",
      "7750.38.98:7852.9.1",
      "7852.9.1:7852.13.98",
      "7852.13.98:7852.2.2",
      "7740.38.96:7851.7.1",
      "7750.38.96:7851.7.1",
      "7851.7.1:7851.11.98",
      "7851.11.98:7851.2.2",
      "7740.38.97:7802.7.1",
      "7750.38.97:7802.7.1",
      "7802.7.1:7802.11.98",
      "7802.11.98:7802.2.2",
      "7740.38.95:7801.7.1",
      "7750.38.95:7801.7.1",
      "7801.7.1:7801.11.98",
      "7801.11.98:7801.2.2"
    ) - labelSegment).toSeq


  //val allDays = 64 until 98 //totalDaysFromFirstDayInPerformanceSpectrum
  val allDays = 50 until 115 //totalDaysFromFirstDayInPerformanceSpectrum
  override def daysForTest: Seq[Int] = Seq(dateToDayNumber("24-12-2017 00:00:00.000"), 60, 70, 80, 90, 100)
//    Seq(
//    dateToDayNumber("20-12-2017 00:00:00.000"),
//    dateToDayNumber("22-12-2017 00:00:00.000"),
//    dateToDayNumber("23-12-2017 00:00:00.000"),
//    dateToDayNumber("24-12-2017 00:00:00.000"),
//    dateToDayNumber("30-12-2017 00:00:00.000"),
//    dateToDayNumber("08-03-2018 00:00:00.000"),
//    dateToDayNumber("09-03-2018 00:00:00.000")
//  )


  override def daysForTraining: Seq[Int] = allDays.filter(!daysForTest.contains(_))


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
