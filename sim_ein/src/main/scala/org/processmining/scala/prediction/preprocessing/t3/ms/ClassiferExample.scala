package org.processmining.scala.prediction.preprocessing.t3.ms

import java.util.Date

import org.apache.log4j.PropertyConfigurator
import org.processmining.scala.applications.mhs.bhs.t3.scenarios.spark.T3Session
import org.processmining.scala.log.common.enhancment.segments.common.{AbstractDurationClassifier, Normal12VerySlowDurationClassifier}
import org.processmining.scala.log.utils.common.errorhandling.{EH, JvmParams}
import org.processmining.scala.viewers.spectrum.builder.AbstractSegmentsToSpectrumSession

private object ClassifierExampleFileNameSettings {
  val inDir = "C://data//full_converted"
  val outDir = "C://ms/logs/classifier"
}

class MyLateBagsClassifier(lateBags: Set[String]) extends AbstractDurationClassifier {
  override def classify(duration: Long, q2: Double, median: Double, q4: Double, caseId: String, timestamp: Long, segmentName: String, medianAbsDeviation: Double): Int =
    if (lateBags.contains(caseId)) 1 else 0

  override def sparkSqlExpression(attrNameDuration: String, attrNameClazz: String): String = ??? //Implementation is not required

  override val legend = "BAGS%Normal%Late"

  override def classCount: Int = 2
}


class SegmentsToSpectrumCli(override val SegmentsPath: String, override val SpectrumRoot: String, override val DatasetSizeDays: Int, override val startTime: String, override val twSizeMs: Int, override val classifier: AbstractDurationClassifier) extends AbstractSegmentsToSpectrumSession



private object ClassiferExample extends T3Session(
  ClassifierExampleFileNameSettings.inDir,
  ClassifierExampleFileNameSettings.outDir,
  "17-11-2017 00:00:00", //does not matter here
  "18-11-2017 00:00:00", //does not matter here
  900000, //time window //does not matter here
  //s => s.substring(0, 8) == "20170928"
  _ => true
) {

  def main(args: Array[String]): Unit = {
    PropertyConfigurator.configure("./log4j.properties")
    JvmParams.reportToLog(logger, s"${ClassiferExample.getClass} started")
    if (args.isEmpty) {
      logger.info(s"Use the following arguments: SEGMENTS_DIR DAYS START_TIME BIN_DURATION_MS OUT_DIR")
    } else {
      logger.info(s"Cli args:${args.mkString(",")}")
      try {
        val segmentsPath = args(0)
        val spectrumRoot = args(4)
        val datasetSizeDays = args(1).toInt
        val startTime = args(2)
        val twSizeMs = args(3).toInt

        //This line reads all 180 files XXXXXX_WC_PACKAGEINFO.CSV (~150GB) and extract information about bag processes
        val lateBagsIds = bpiImporter
          .loadNotDistinctLateBagIdsFromCsv(spark)
          .collect() // to load into memory
          .toSet

        println(s"""Loading took ${(new Date().getTime - appStartTime.getTime) / 1000} seconds.""")
        val myLateBagsClassifier = new MyLateBagsClassifier(lateBagsIds)
        new SegmentsToSpectrumCli(segmentsPath, spectrumRoot, datasetSizeDays, startTime, twSizeMs,  myLateBagsClassifier()).run()
      } catch {
        case e: Throwable => logger.error(EH.formatError(e.toString, e))
      }
    }
    logger.info(s"App is completed.")
  }
}

