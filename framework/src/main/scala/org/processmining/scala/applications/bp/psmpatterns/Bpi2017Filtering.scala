package org.processmining.scala.applications.bp.psmpatterns


import java.time.Duration
import java.util.Date

import org.apache.log4j.PropertyConfigurator
import org.slf4j.LoggerFactory
import org.processmining.scala.log.common.csv.spark.CsvWriter
import org.processmining.scala.log.common.enhancment.segments.common.SegmentUtils
import org.processmining.scala.log.common.enhancment.segments.spark.{DurationSegmentProcessor, SegmentProcessor, SegmentProcessorConfig}
import org.processmining.scala.log.common.filtering.expressions.events.regex.impl.EventEx
import org.processmining.scala.log.common.filtering.expressions.events.variables.EventVar
import org.processmining.scala.log.common.filtering.expressions.traces._
import org.processmining.scala.log.common.filtering.expressions.traces.impl.TraceExpression
import org.processmining.scala.log.common.unified.event.UnifiedEvent
import org.processmining.scala.log.common.unified.log.spark.UnifiedEventLog
import org.processmining.scala.log.common.unified.trace.UnifiedTraceId
import org.processmining.scala.log.common.utils.spark.AbstractSession
import org.processmining.scala.log.common.xes.parallel.XesReader
import org.processmining.scala.log.utils.common.csv.common.CsvExportHelper
import org.processmining.scala.log.utils.common.errorhandling.EH

private object Bpi2017Filtering extends AbstractSession("Bpi2017Filtering") {
  private val logger = LoggerFactory.getLogger(Xes2Reports.getClass)
  private val InDir = "f:\\logs\\psm"
  private val OutDir = "f:\\logs\\psm\\output"
  protected val timeZone = CsvExportHelper.UtcTimeZone
  protected val csvExportHelper = new CsvExportHelper(CsvExportHelper.DefaultTimestampPattern,
    timeZone,
    CsvExportHelper.DefaultSeparator) //to export logs

  val t = TraceExpression()

  private def calculateDurations(log: UnifiedEventLog, name: String, twSize: Long) = {
    val (timestamp1Ms, timestamp2Ms) = log.minMaxTimestamp()
    val segments = log.map(SegmentUtils.convertToSegments(SegmentUtils.DefaultSeparator, _: (UnifiedTraceId, List[UnifiedEvent])))
    val processorConfig = SegmentProcessorConfig(spark,
      segments,
      timestamp1Ms, timestamp2Ms,
      twSize)
    val (_, _, durationProcessor) = DurationSegmentProcessor(processorConfig, 0.5)
    SegmentProcessor.toCsvV2(
      durationProcessor, OutDir + s"\\$name\\duration\\", durationProcessor.adc.legend)
  }


  private def calculateDurations2(log: UnifiedEventLog, twSize: Long) = {
    val (timestamp1Ms, timestamp2Ms) = log.minMaxTimestamp()
    val segments = log.map(SegmentUtils.convertToSegments(SegmentUtils.DefaultSeparator, _: (UnifiedTraceId, List[UnifiedEvent])))
    val processorConfig = SegmentProcessorConfig(spark,
      segments,
      timestamp1Ms, timestamp2Ms,
      twSize)
    val (_, _, durationProcessor) = DurationSegmentProcessor(processorConfig, 0.5)
    val segmentLog = durationProcessor.getClassifiedSegmentLog()
    val exGreen = t contains EventEx("W_Call incomplete files:O_Accepted").withValue[Byte]("class", 0)
    val exNotGreen = t contains EventEx("W_Call incomplete files:O_Accepted").withRange[Byte]("class", 1, 6)
    val logGreen = (log join segmentLog.filter(exGreen))
      .map(x => (x._1, x._2.filter(!_.hasAttribute("class"))))
    CsvWriter.logToCsvLocalFilesystem(logGreen, s"$OutDir/logGreen.csv", csvExportHelper.timestamp2String, "org:resource")
    val logNotGreen = (log join segmentLog.filter(exNotGreen))
      .map(x => (x._1, x._2.filter(!_.hasAttribute("class"))))
    CsvWriter.logToCsvLocalFilesystem(logNotGreen, s"$OutDir/logNotGreen.csv", csvExportHelper.timestamp2String, "org:resource")

  }

  private def calculateDurations3(log: UnifiedEventLog) = {

    val ex2x = (EventVar("W_Call incomplete files") >> EventVar("O_Accepted").defineTimestamp("t")).where(
      (_, timestamps, _) => {
        val ts = timestamps.get("t")
        if (ts.isDefined) {
          import java.util.Calendar
          val cal = Calendar.getInstance
          cal.setTime(new Date(ts.get))
          val day = cal.get(Calendar.DAY_OF_MONTH)
          (day == 21 || day == 22 || day == 23)
        } else false
      }
    )

    val exNot2x = (EventVar("W_Call incomplete files") >> EventVar("O_Accepted").defineTimestamp("t")).where(
      (_, timestamps, _) => {
        val ts = timestamps.get("t")
        if (ts.isDefined) {
          import java.util.Calendar
          val cal = Calendar.getInstance
          cal.setTime(new Date(ts.get))
          val day = cal.get(Calendar.DAY_OF_MONTH)
          (day < 21 || day > 23)
        } else false
      }
    )
    val log1 = log.filter(ex2x)

    CsvWriter.logToCsvLocalFilesystem(log1, s"$OutDir/log2x.csv", csvExportHelper.timestamp2String, "org:resource")
    CsvWriter.logToCsvLocalFilesystem(log.filter(exNot2x), s"$OutDir/logNot2x.csv", csvExportHelper.timestamp2String, "org:resource")

  }

  private def processLog(originalLog: org.processmining.scala.log.common.unified.log.parallel.UnifiedEventLog, name: String, twSize: Long) = {
    logger.info(s"Log $name has ${originalLog.count()} traces.")
    val log = UnifiedEventLog.fromTraces(originalLog.traces(), spark).persist() // from par to spark
    //val log = UnifiedEventLog.fromTraces(originalLog.projectAttributes(Set()).traces(), spark).persist() // from par to spark


    val exSentMailAndOnline = EventEx("O_Sent \\(mail and online\\)")
    val exSentOnlineOnly = EventEx("O_Sent \\(online only\\)")
    val exReturned = EventEx("O_Returned")
    val exAccepted = EventEx("O_Accepted")
    val exRefused = EventEx("O_Refused")
    val exACancelled = EventEx("A_Cancelled")


    val exF1_Accepted_Mail_OnlineOnly = (t contains (exSentMailAndOnline >-> exReturned >-> exAccepted)) or
      (t contains (exSentOnlineOnly >-> exReturned >-> exAccepted))

    val exF2_Refused_Mail_OnlineOnly = (t contains (exSentMailAndOnline >-> exReturned >-> exRefused)) or
      (t contains (exSentOnlineOnly >-> exReturned >-> exRefused))

    val exF3_ACancelled_Mail_OnlineOnly = (t contains (exSentMailAndOnline >-> exReturned >-> exACancelled)) or
      (t contains (exSentOnlineOnly >-> exReturned >-> exACancelled))


    val ACancelled_Mail_NotReturned = (t contains (exSentMailAndOnline >-> exACancelled)) and
      (t contains exReturned).unary_!()
    val ACancelled_OnlineOnly_NotReturned = (t contains (exSentOnlineOnly >-> exACancelled)) and
      (t contains exReturned).unary_!()
    val exF4_ACancelled_Mail_OnlineOnly_NotReturned = ACancelled_Mail_NotReturned or ACancelled_OnlineOnly_NotReturned


    val log_exF1_Accepted_Mail_OnlineOnly = log.filter(exF1_Accepted_Mail_OnlineOnly).persist()
    calculateDurations2(log_exF1_Accepted_Mail_OnlineOnly, Duration.ofDays(1).toMillis)
    calculateDurations3(log_exF1_Accepted_Mail_OnlineOnly)

    calculateDurations(log_exF1_Accepted_Mail_OnlineOnly, "exF1_Accepted_Mail_OnlineOnly", Duration.ofDays(1).toMillis)
    CsvWriter.logToCsvLocalFilesystem(log_exF1_Accepted_Mail_OnlineOnly, s"$OutDir/exF1_Accepted_Mail_OnlineOnly.csv", csvExportHelper.timestamp2String)
    log_exF1_Accepted_Mail_OnlineOnly.unpersist()

    val log_exF2_Refused_Mail_OnlineOnly = log.filter(exF2_Refused_Mail_OnlineOnly).persist()
    calculateDurations(log_exF2_Refused_Mail_OnlineOnly, "exF2_Refused_Mail_OnlineOnly", Duration.ofDays(1).toMillis)
    CsvWriter.logToCsvLocalFilesystem(log_exF2_Refused_Mail_OnlineOnly, s"$OutDir/exF2_Refused_Mail_OnlineOnly.csv", csvExportHelper.timestamp2String)
    log_exF2_Refused_Mail_OnlineOnly.unpersist()

    val log_exF3_ACancelled_Mail_OnlineOnly = log.filter(exF3_ACancelled_Mail_OnlineOnly).persist()
    calculateDurations(log_exF3_ACancelled_Mail_OnlineOnly, "exF3_ACancelled_Mail_OnlineOnly", Duration.ofDays(1).toMillis)
    CsvWriter.logToCsvLocalFilesystem(log_exF3_ACancelled_Mail_OnlineOnly, s"$OutDir/exF3_ACancelled_Mail_OnlineOnly.csv", csvExportHelper.timestamp2String)
    log_exF3_ACancelled_Mail_OnlineOnly.unpersist()

    val log_exF4_ACancelled_Mail_OnlineOnly_NotReturned = log.filter(exF4_ACancelled_Mail_OnlineOnly_NotReturned).persist()
    calculateDurations(log_exF4_ACancelled_Mail_OnlineOnly_NotReturned, "exF4_ACancelled_Mail_OnlineOnly_NotReturned", Duration.ofDays(1).toMillis)
    CsvWriter.logToCsvLocalFilesystem(log_exF4_ACancelled_Mail_OnlineOnly_NotReturned, s"$OutDir/exF4_ACancelled_Mail_OnlineOnly_NotReturned.csv", csvExportHelper.timestamp2String)
    log_exF4_ACancelled_Mail_OnlineOnly_NotReturned.unpersist()

    val log_exF5_Accepted_OnlineOnly = log.filter((t contains (exSentOnlineOnly >-> exReturned >-> exAccepted))).persist()
    calculateDurations(log_exF5_Accepted_OnlineOnly, "log_exF5_Accepted_OnlineOnly", Duration.ofDays(1).toMillis)
    CsvWriter.logToCsvLocalFilesystem(log_exF5_Accepted_OnlineOnly, s"$OutDir/log_exF5_Accepted_OnlineOnly.csv", csvExportHelper.timestamp2String)
    log_exF5_Accepted_OnlineOnly.unpersist()

  }

  def readAndProcess(name: String, twSizeMs: Long, subFolder: String) = {
    val logs = XesReader.read(s"$InDir\\$name")
    if (logs.nonEmpty) {
      processLog(logs.head, s"$subFolder\\$name", twSizeMs)
    } else throw new IllegalArgumentException(s"Log $name is empty")
  }

  def main(args: Array[String]): Unit = {
    PropertyConfigurator.configure("./res/log4j.properties")
    logger.info("App started.")
    try {

      readAndProcess("BPI Challenge 2017.xes.gz", Duration.ofDays(1).toMillis, "1d")
    } catch {
      case e: Exception => EH().error(e)
    }
  }
}
