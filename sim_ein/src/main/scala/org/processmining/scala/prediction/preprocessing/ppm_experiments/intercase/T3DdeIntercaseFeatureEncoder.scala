package org.processmining.scala.prediction.preprocessing.ppm_experiments.intercase


import java.io.File
import java.util.concurrent.{Callable, Executors, TimeUnit}

import org.apache.log4j.PropertyConfigurator
import org.processmining.scala.intercase._
import org.processmining.scala.log.common.csv.parallel.CsvReader
import org.processmining.scala.log.common.unified.event.UnifiedEvent
import org.processmining.scala.log.common.unified.log.parallel.UnifiedEventLog
import org.processmining.scala.log.utils.common.csv.common.{CsvExportHelper, CsvImportHelper}
import org.processmining.scala.log.utils.common.errorhandling.{EH, JvmParams}
import org.processmining.scala.prediction.preprocessing.ppm_experiments.SpectrumToDatasetCheckIn
import org.slf4j.LoggerFactory

class T3DdeIntercaseFeatureEncoder(filename: File, outputDir: String, activityA: String, activityB: String) extends BaseDdeInterCaseFeatureEncodingSession with Callable[(List[Sample], String)] with SegmentBasesFilters {

  val thresholds = List(10*1000L, 30*1000L, 60*1000L, 90*1000L, 180*1000L)
  val importCsvHelper = new CsvImportHelper(CsvExportHelper.FullTimestampPattern, CsvExportHelper.AmsterdamTimeZone)

  def loadFromCsv(filename: String): UnifiedEventLog = {
    def factory(caseIdIndex: Int, timestampIndex: Int, activityIndex: Int, row: Array[String]): (String, UnifiedEvent) =
      (
        row(caseIdIndex),
        UnifiedEvent(
          importCsvHelper.extractTimestamp(row(timestampIndex)),
          row(activityIndex)))

    val csvReader = new CsvReader()
    val (header, lines) = csvReader.read(filename)
    UnifiedEventLog.create(
      csvReader.parse(
        lines,
        factory(header.indexOf("id"),
          header.indexOf("timestamp"),
          header.indexOf("activity"),
          _: Array[String])
      ))
  }

  val historicSegments = SpectrumToDatasetCheckIn
    .stateSegments
    .map(_.split(":"))
    .map(x=>(x(0), x(1))).toSet

  def prefixFilterImpl(p: Prefix): Boolean = {
    if (p.prefix.size >= 2) {
      val s = (p.prefix.init.last, p.prefix.last)
      val ret = historicSegments.contains(s)
      //if(ret) logger.info(s"""$ret ${p.prefix.mkString("-")}""")
      val ret2 = if(ret && p.prefix.size >= 4) !findTargetSegment(activityA, activityB, p.prefix.init.init).isDefined else ret
      //logger.info(s"""$ret2 ${p.prefix.mkString("-")}""")
      ret2
    } else false
  }


  val outcome: (Prefix, List[UnifiedEvent]) => Option[Outcome] = outcomeForSegment(activityA, activityB)
  val isCompleted: (Prefix, List[UnifiedEvent]) => Boolean = (p, _) => findTargetSegment(activityA, activityB, p.prefix).isDefined
  val prefixFilter: Prefix => Boolean = prefixFilterImpl

  override def call(): (List[Sample], String) = {
    val log = loadFromCsv(filename.getPath)
    val date = filename.getName.substring(0, filename.getName.length - 4 )
    //val outDir = s"${outputDir}/$date"
//    new File(outDir).mkdirs()
    val samples = new DdeInterCaseFeatureEncoder(log, thresholds, outcome, isCompleted, prefixFilter).call()
    //exportSetAsAWhole(outDir, samples)
    (samples, date)
  }
}

object T3DdeIntercaseFeatureEncoder {
  private val logger = LoggerFactory.getLogger(T3DdeIntercaseFeatureEncoder.getClass)

  def main(args: Array[String]): Unit = {
    PropertyConfigurator.configure("./log4j.properties")
    JvmParams.reportToLog(logger, "T3DdeIntercaseFeatureEncoder started")
    try {
      val dir = "G:\\PI1_debug"

      val outDir = "G:\\PI1_debug\\dde"

      val dirObj = new File(dir)
      val files = dirObj.listFiles().filter(_.getName.toLowerCase.endsWith(".csv")).sortBy(_.getName)
      val obj = files.map(new T3DdeIntercaseFeatureEncoder(_, outDir, "P50-Link", "ScannersIn"))


      val poolSize = 24
      val executorService = Executors.newFixedThreadPool(poolSize)
      val futures = obj.toList.map(executorService.submit(_))
      logger.info("All tasks are submitted.")
      val samplesPerDay = futures.map(_.get())

      val (_, max) = Sample.getNonNormalizedSamplesAndMax(samplesPerDay.flatMap(_._1))

      val split = "20170928" //"20180323"
      val training = samplesPerDay.filter(_._2 < split)
      val test = samplesPerDay.filter(_._2 >= split)

      BaseDdeInterCaseFeatureEncodingSession.exportIntermediateDataset(s"$outDir/intermediate_training.csv", training.flatMap(_._1), true)
      Sample.export(s"$outDir/training.csv", Sample.normalize(None, training.flatMap(_._1), max)._1, max)

      BaseDdeInterCaseFeatureEncodingSession.exportIntermediateDataset(s"$outDir/intermediate_test.csv", test.flatMap(_._1), true)
      Sample.export(s"$outDir/test.csv", Sample.normalize(None, test.flatMap(_._1), max)._1, max)


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
