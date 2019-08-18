package org.processmining.scala.applications.bp.psmpatterns

import java.io.File
import java.time.Duration
import java.util.Date

import org.apache.log4j.PropertyConfigurator
import org.processmining.scala.log.common.enhancment.segments.common._
import org.processmining.scala.log.common.enhancment.segments.spark.{DurationSegmentProcessor, SegmentProcessor, SegmentProcessorConfig, SegmentUtils}
import org.processmining.scala.log.common.filtering.expressions.traces.impl.TraceExpression
import org.processmining.scala.log.common.unified.event.UnifiedEvent
import org.processmining.scala.log.common.unified.log.spark.UnifiedEventLog
import org.processmining.scala.log.common.unified.trace.UnifiedTraceId
import org.processmining.scala.log.common.utils.spark.AbstractSession
import org.processmining.scala.log.common.xes.parallel.XesReader
import org.processmining.scala.log.utils.common.errorhandling.EH
import org.slf4j.LoggerFactory

private object Xes2Reports extends AbstractSession("Xes2Reports") {
  private val logger = LoggerFactory.getLogger(Xes2Reports.getClass)
  private val InDir = "D:\\logs\\psm\\input"
  private val OutDir = "C:\\review\\psm_datasets\\"


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
      .fromTraces(originalLog.traces(), spark /*, 10000*/)
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
    val segments = log.map(org.processmining.scala.log.common.enhancment.segments.common.SegmentUtils.convertToSegments(SegmentUtils.DefaultSeparator, _: (UnifiedTraceId, List[UnifiedEvent])))
    log.unpersist()
    val processorConfig = SegmentProcessorConfig(spark,
      segments,
      timestamp1Ms, timestamp2Ms,
      twSize, InventoryAggregation)
    val (_, _, durationProcessor) = DurationSegmentProcessor(processorConfig, 0.5, new Q4DurationClassifier())
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
      val days = 1
      readAndProcess("Road_Traffic_Fine_Management_Process.xes.gz", Duration.ofDays(days).toMillis, days.toString + "d")
      readAndProcess("BPI_Challenge_2012.xes.gz", Duration.ofDays(days).toMillis, days.toString + "d")
      readAndProcess("BPI Challenge 2017.xes.gz", Duration.ofDays(days).toMillis, days.toString + "d")
//      //readAndProcess("BPI Challenge 2018.xes", Duration.ofDays(days).toMillis, days.toString + "d", "doctype", "subprocess", "activity")
      readAndProcess("Hospital Billing - Event Log.xes.gz", Duration.ofDays(days).toMillis, days.toString + "d")
      readAndProcess("Hospital_log.xes.gz", Duration.ofDays(days).toMillis, days.toString + "d")
      readAndProcess("BPI_Challenge_2013_incidents.xes.gz", Duration.ofDays(days).toMillis, days.toString + "d")
      readAndProcess("BPI_Challenge_2013_open_problems.xes.gz", Duration.ofDays(days).toMillis, days.toString + "d")
      readAndProcess("BPI_2014_Detail_Incident_Activity.xes", Duration.ofDays(days).toMillis, days.toString + "d")
      readAndProcess("BPIC15_1.xes", Duration.ofDays(days).toMillis, days.toString + "d")
      readAndProcess("BPIC15_2.xes", Duration.ofDays(days).toMillis, days.toString + "d")
      readAndProcess("BPIC15_3.xes", Duration.ofDays(days).toMillis, days.toString + "d")
      readAndProcess("BPIC15_4.xes", Duration.ofDays(days).toMillis, days.toString + "d")
      readAndProcess("BPIC15_5.xes", Duration.ofDays(days).toMillis, days.toString + "d")
    } catch {
      case e: Exception => EH().error(e)
    }
  }
}
