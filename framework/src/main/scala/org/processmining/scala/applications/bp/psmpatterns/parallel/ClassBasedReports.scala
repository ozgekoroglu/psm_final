package org.processmining.scala.applications.bp.psmpatterns.parallel

import java.io.File
import java.time.Duration
import java.util.Date

import org.apache.log4j.PropertyConfigurator
import org.processmining.scala.log.common.enhancment.segments.common._
import org.processmining.scala.log.common.enhancment.segments.parallel.{SegmentUtils => _, _}
import org.processmining.scala.log.common.unified.event.UnifiedEvent
import org.processmining.scala.log.common.unified.trace.UnifiedTraceId
import org.processmining.scala.log.common.xes.parallel.XesReader
import org.processmining.scala.log.utils.common.errorhandling.EH
import org.slf4j.LoggerFactory


class SomeClassifier extends AbstractClassifier {
  def func(e: (UnifiedTraceId, UnifiedEvent)): Int = e._2.activity.length % 5

  override def classCount: Int = 5

  override def legend: String = "SOME_CLASS%Q1%Q2%Q3%Q4%Q5"
}

private object ClassBasedReports {

  private val logger = LoggerFactory.getLogger(ClassBasedReports.getClass)
  private val InDir = "D:\\logs\\psm\\input"
  private val OutDir = "C:\\p\\some_class\\"


  private def processLog(originalLog: org.processmining.scala.log.common.unified.log.parallel.UnifiedEventLog,
                         logAttributes: org.processmining.scala.log.common.unified.log.parallel.UnifiedEventLog,
                         name: String,
                         twSize: Long,
                         classifierClassName: String) = {

    val classifier = AbstractClassifier(classifierClassName)

    logger.info(s"Log $name has ${originalLog.count()} traces.")
    val logAttributes = originalLog
    val (timestamp1Ms, timestamp2Ms) = originalLog.minMaxTimestamp()
    logger.info(s"Log $name has timestamps from ${new Date(timestamp1Ms)} to ${new Date(timestamp2Ms)}")
    val segments = originalLog
      .map(SegmentUtils.convertToSegments(":", _: (UnifiedTraceId, List[UnifiedEvent])))

    val processorConfig = SegmentProcessorConfig(
      segments,
      timestamp1Ms, timestamp2Ms,
      twSize, InventoryAggregation)


    val processor = new ClazzBasedSegmentProcessor[(UnifiedTraceId, UnifiedEvent)](
      processorConfig, classifier.classCount, logAttributes.events().toList, _._1.id, _._2.timestamp, classifier.func)

    SegmentProcessor.toCsvV2(
      processor, OutDir + s"\\$name\\activity_length\\", classifier.legend)
  }

  def readAndProcess(name: String, twSizeMs: Long, subFolder: String, activityClassifier: String*) = {
    val logs = XesReader.read(new File(s"$InDir\\$name"), None, None, XesReader.DefaultTraceAttrNamePrefix, "-", activityClassifier: _*)
    if (logs.nonEmpty) {
      processLog(logs.head, logs.head, s"$subFolder\\$name", twSizeMs,
        "org.processmining.scala.applications.bp.psmpatterns.parallel.SomeClassifier")
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


