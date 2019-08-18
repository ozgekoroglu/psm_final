package org.processmining.scala.prediction.preprocessing.t3.common

import org.processmining.scala.log.common.filtering.expressions.traces.impl.TraceExpression
import org.processmining.scala.log.common.unified.event.UnifiedEvent
import org.processmining.scala.log.common.unified.log.parallel.UnifiedEventLog
import org.processmining.scala.log.common.unified.trace.UnifiedEventAggregator
import org.processmining.scala.log.common.utils.common.EventAggregatorImpl
import org.processmining.scala.log.utils.common.csv.common.{CsvExportHelper, CsvImportHelper}
import org.slf4j.LoggerFactory

class T3LogsToSegments(eventLogPath: String, aggregationIniFilename: String, exportAggregatedLog: Boolean = false) {
  private val logger = LoggerFactory.getLogger(classOf[T3LogsToSegments])
  private val importCsvHelper = new CsvImportHelper(CsvExportHelper.FullTimestampPattern, CsvExportHelper.AmsterdamTimeZone)
  private val t = TraceExpression()

  private def factory(caseIdIndex: Int, completeTimestampIndex: Int, activityIndex: Int)(row: Array[String]): (String, UnifiedEvent) =
    (
      row(caseIdIndex),
      UnifiedEvent(
        importCsvHelper.extractTimestamp(row(completeTimestampIndex)),
        row(activityIndex)))

  def factoryOfFactory(header: Array[String]): Array[String] => (String, UnifiedEvent) = {
    factory(header.indexOf("id"),
      header.indexOf("timestamp"),
      header.indexOf("activity"))
  }

  def aggregate(log: UnifiedEventLog): UnifiedEventLog = {
    val aggregation = new EventAggregatorImpl(aggregationIniFilename)
    val exAggregation = t aggregate UnifiedEventAggregator(aggregation)
    log
//      .map(exAggregation)
//      .remove(EventEx("ToBeDeleted"))
  }
}

