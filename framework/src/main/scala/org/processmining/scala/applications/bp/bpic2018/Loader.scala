package org.processmining.scala.applications.bp.bpic2018


import java.io.File
import java.time.Duration

import org.apache.log4j.PropertyConfigurator
import org.processmining.scala.log.common.csv.spark.CsvWriter
import org.processmining.scala.log.common.filtering.expressions.traces.impl.TraceExpression
import org.processmining.scala.log.common.unified.log.spark.UnifiedEventLog
import org.processmining.scala.log.common.utils.spark.AbstractSession
import org.processmining.scala.log.common.xes.parallel.XesReader
import org.processmining.scala.log.utils.common.csv.common.CsvExportHelper
import org.processmining.scala.log.utils.common.errorhandling.EH
import org.slf4j.LoggerFactory

private object Loader extends AbstractSession("Xes2Reports") {
  private val logger = LoggerFactory.getLogger(Loader.getClass)
  private val InDir = "D:\\logs\\bpic2018"
  private val OutDir = "D:\\logs\\bpic2018\\output"


  private val t = TraceExpression()

  private def processLog(originalLog: org.processmining.scala.log.common.unified.log.parallel.UnifiedEventLog, name: String, twSize: Long) = {
    logger.info(s"Log $name has ${originalLog.count()} traces.")
    //val lifecycleTransition = "lifecycle:transition"
    //val exp = t subtrace (EventEx("[AOW].*") >> EventEx("[AOW].*") >> EventEx("End"))


    val log = UnifiedEventLog
      .fromTraces(originalLog.traces(), spark, 100)
      .persist() // from par to spark


    CsvWriter.logToCsvLocalFilesystem(log,
      s"$OutDir/bpi2018.csv",
      CsvExportHelper().timestamp2String, "success")
  }

  def readAndProcess(name: String, twSizeMs: Long, subFolder: String) = {
    val logs = XesReader.read(new File(s"$InDir\\$name"), None, None, XesReader.DefaultTraceAttrNamePrefix, " ", "doctype", "subprocess", "activity")
    if (logs.nonEmpty) {
      processLog(logs.head, s"$subFolder\\$name", twSizeMs)
    } else throw new IllegalArgumentException(s"Log $name is empty")
  }

  def main(args: Array[String]): Unit = {
    PropertyConfigurator.configure("./res/log4j.properties")
    logger.info("App started.")
    try {
      readAndProcess("BPI Challenge 2018.xes", Duration.ofDays(7).toMillis, "7d")
    } catch {
      case e: Exception => EH().error(e)
    }
  }
}
