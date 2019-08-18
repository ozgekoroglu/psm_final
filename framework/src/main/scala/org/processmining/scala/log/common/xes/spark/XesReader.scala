package org.processmining.scala.log.common.xes.spark

import org.slf4j.LoggerFactory
import org.apache.spark.sql.SparkSession
import org.deckfour.xes.model._

object XesReader {

  private val logger = LoggerFactory.getLogger(XesReader.getClass)

  def xesToUnifiedEventLog(spark: SparkSession,
                           numSlices: Int,
                           xLog: XLog,
                           traceLevelAttributes: Option[Set[String]],
                           eventLevelAttributes: Option[Set[String]],
                           sep: String,
                           activityClassifier: String*) =
    org.processmining.scala.log.common.unified.log.spark.UnifiedEventLog.fromTraces(
      org.processmining.scala.log.common.xes.parallel.XesReader.xesToTraces(xLog, traceLevelAttributes, eventLevelAttributes, sep, activityClassifier: _*),
      spark, numSlices)

}
