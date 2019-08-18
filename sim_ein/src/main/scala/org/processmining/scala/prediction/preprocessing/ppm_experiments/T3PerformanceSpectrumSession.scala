package org.processmining.scala.prediction.preprocessing.ppm_experiments

import java.time.Duration

import org.processmining.scala.viewers.spectrum.features.{AbstractSpectrumToDatasetSession, SpectrumToDataset}

abstract class T3PerformanceSpectrumSession extends AbstractSpectrumToDatasetSession {
  val firstDayDateTime = "27-09-2017 00:00:00.000"
  val totalDaysFromFirstDayInPerformanceSpectrum = 186
  val daysNumberInTrainingDataset = 177

  def aggregation() = SpectrumToDataset.AggregationStart

  //private val importCsvHelperDays = new CsvImportHelper("dd-MM-yyyy", CsvExportHelper.AmsterdamTimeZone)

  override def daysForTraining: Seq[Int] = (0 until daysNumberInTrainingDataset)

  override def daysForTest: Seq[Int] = (daysNumberInTrainingDataset until totalDaysFromFirstDayInPerformanceSpectrum)

  def dateToDayNumber(s: String): Int =
    ((importCsvHelper.extractTimestamp(s) - importCsvHelper.extractTimestamp(firstDayDateTime)) / Duration.ofDays(1).toMillis).toInt


}
