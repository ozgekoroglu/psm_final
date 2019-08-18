package org.processmining.scala.applications.bp.psmpatterns.parallel

import java.io.File
import java.time.Duration
import java.util.Date

import org.apache.log4j.PropertyConfigurator
import org.slf4j.LoggerFactory
import org.processmining.scala.log.common.enhancment.segments.common._
import org.processmining.scala.log.common.enhancment.segments.parallel.{DurationSegmentProcessor, SegmentProcessor, SegmentProcessorConfig}
import org.processmining.scala.log.common.filtering.expressions.traces.impl.TraceExpression
import org.processmining.scala.log.common.unified.event.UnifiedEvent
import org.processmining.scala.log.common.unified.log.parallel.UnifiedEventLog
import org.processmining.scala.log.common.unified.trace.UnifiedTraceId
import org.processmining.scala.log.common.xes.parallel.XesReader
import org.processmining.scala.log.utils.common.errorhandling.EH

private object Xes2Reports{

  private val logger = LoggerFactory.getLogger(Xes2Reports.getClass)
  private val InDir = "D:\\logs\\psm\\input"
  private val OutDir = "C:\\psm_datasets_parallel\\"

  private val t = TraceExpression()

  def createArtificialEvent(activity: String, delta: Long, id: UnifiedTraceId, e: UnifiedEvent): UnifiedEvent = {
    val zeroClazz: Byte = 0
    UnifiedEvent(
      e.timestamp + delta,
      activity)
  }


  private def processLog(originalLog: org.processmining.scala.log.common.unified.log.parallel.UnifiedEventLog, name: String, twSize: Long) = {
    logger.info(s"Log $name has ${originalLog.count()} traces.")
    //val lifecycleTransition = "lifecycle:transition"
    //val exp = t subtrace (EventEx("[AOW].*") >> EventEx("[AOW].*") >> EventEx("End"))


    val log = UnifiedEventLog
      .fromTraces(originalLog.traces())
    //      .map { t =>
    //        (t._1, t._2.map { x =>
    //          if (!x.hasAttribute(lifecycleTransition)) x else x.copy(s"${x.activity}_${x.getAs[String](lifecycleTransition)}")
    //        })
    //      }
    //.project(EventEx(".+").withValue("lifecycle:transition", "complete"))
    //      .map {
    //      TraceExpression().withArtificialStartsStops(
    //        createArtificialEvent("Start", 0, _: UnifiedTraceId, _: UnifiedEvent),
    //        createArtificialEvent("End", 0, _: UnifiedTraceId, _: UnifiedEvent)
    //      )
    //    }
    //      .map(exp)
    //.persist() // from par to spark

    //    val csvExportHelper = new CsvExportHelper(CsvExportHelper.DefaultTimestampPattern,
    //      CsvExportHelper.UtcTimeZone,
    //      CsvExportHelper.DefaultSeparator) //to export logs
    //    CsvWriter.logToCsvLocalFilesystem(log,
    //      "d:/logs/bpi2018_tmp.csv",
    //      csvExportHelper.timestamp2String, "(case) small farmer")


    val (timestamp1Ms, timestamp2Ms) = log.minMaxTimestamp()
    logger.info(s"Log $name has timestamps from ${new Date(timestamp1Ms)} to ${new Date(timestamp2Ms)}")
    val segments = log
      .map(SegmentUtils.convertToSegments(":", _: (UnifiedTraceId, List[UnifiedEvent])))

    val processorConfig = SegmentProcessorConfig(
      segments,
      timestamp1Ms, timestamp2Ms,
      twSize, InventoryAggregation)
    val (_, _, durationProcessor) = DurationSegmentProcessor(processorConfig, new Q4DurationClassifier())
    SegmentProcessor.toCsvV2(
      durationProcessor, OutDir + s"\\$name\\duration\\", durationProcessor.adc.legend)
  }

  def readAndProcess(name: String, twSizeMs: Long, subFolder: String, activityClassifier: String*) = {
    val logs = XesReader.read(new File(s"$InDir\\$name"), None, None, XesReader.DefaultTraceAttrNamePrefix, "-", activityClassifier: _*)
    if (logs.nonEmpty) {
      processLog(logs.head, s"$subFolder\\$name", twSizeMs)
    } else throw new IllegalArgumentException(s"Log $name is empty")
  }

  def main(args: Array[String]): Unit = {
    PropertyConfigurator.configure("./res/log4j.properties")
    logger.info("App started.")
    try {
      val days = 30
      readAndProcess("Road_Traffic_Fine_Management_Process.xes.gz", Duration.ofDays(days).toMillis, days.toString + "d")

    } catch {
      case e: Exception => EH().error(e)
    }
  }
}
