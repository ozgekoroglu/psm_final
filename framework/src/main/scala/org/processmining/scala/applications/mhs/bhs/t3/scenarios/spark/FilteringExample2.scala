package org.processmining.scala.applications.mhs.bhs.t3.scenarios.spark

import java.util.Date

import org.processmining.scala.applications.mhs.bhs.t3.eventsources.{BpiImporter, TrackingReportSchema}
import org.processmining.scala.log.common.csv.spark.CsvWriter
import org.processmining.scala.log.common.enhancment.segments.spark.DurationSegmentProcessor
import org.processmining.scala.log.common.filtering.expressions.events.regex.impl.EventEx
import org.processmining.scala.log.common.filtering.expressions.traces._
import org.processmining.scala.log.common.filtering.expressions.traces.impl.TraceExpression
import org.processmining.scala.log.common.unified.event.CommonAttributeSchemas
import org.processmining.scala.log.common.unified.trace.UnifiedEventAggregator
import org.processmining.scala.log.common.utils.common.EventAggregatorImpl


private object FilteringExample2 extends T3Session(
  "d:/logs/20160719",
  "D:/tmp/evaluation/july_19",
  "19-07-2016 12:00:00",
  "19-07-2016 16:00:00",
  900000 //time window
) {

  val t = TraceExpression()

  def main(args: Array[String]): Unit = {

//    val durationProcessorConfig = SegmentProcessorConfig(spark, logSegments,
//      config.timestamp1Ms, config.timestamp2Ms,
//      config.twSize)

    val (_, _, durationProcessor) = DurationSegmentProcessor(processorConfig, config.percentile)
    val classifiedDurationLog = durationProcessor.getClassifiedSegmentLog()

    val aggregator = new EventAggregatorImpl("D:/t3_data/july_19_v.aggregated/aggregator.ini")
    val exAggregation = t aggregate UnifiedEventAggregator(aggregator)
    val logAggregatedMovements = logMovements.map(exAggregation)
    val exCheckInLinkBridge = EventEx("\\d+.\\d+.\\d+:772[123].\\d+.\\d+|772[123].\\d+.\\d+:\\d+.\\d+.\\d+").withRange[Byte](CommonAttributeSchemas.AttrNameClass, 1, 5)
    val filteredClassifiedDurationLog =
      classifiedDurationLog
        .filter(t contains exCheckInLinkBridge)
        .map(t take 1)

    val exCheckIn = EventEx("c00.checkin")
    val exPreSorters = EventEx("d...77[45]0.\\w*")

    val filteredLog4 = (filteredClassifiedDurationLog join logAggregatedMovements)
      .filterByAttributeNames(CommonAttributeSchemas.AggregatedEventsSchema)
      .map(t subtrace (exCheckIn >-> exPreSorters))
      .map(t deaggregate)

    CsvWriter.logToCsvLocalFilesystem(filteredLog4,
      s"${config.outDir}/filteredLog4.csv",
      csvExportHelper.timestamp2String,
      TrackingReportSchema.Schema.toArray : _*)

    println(s"""Pre-processing took ${(new Date().getTime - appStartTime.getTime) / 1000} seconds.""")
  }
}
