package org.processmining.scala.applications.mhs.bhs.t3.scenarios.spark

import java.time.Duration
import java.util.Date

import org.processmining.scala.applications.mhs.bhs.t3.eventsources.{BpiImporter, TrackingReportSchema}
import org.processmining.scala.log.common.csv.spark.CsvWriter
import org.processmining.scala.log.common.filtering.expressions.events.variables.EventVar
import org.processmining.scala.log.common.filtering.expressions.traces.impl.TraceExpression


private object FilteringExample3Vars extends T3Session(
  "d:/logs/20160719",
  "d:/tmp/july_19_v.Sept26_15m_filtering/",
  "19-07-2016 15:00:00",
  "20-07-2016 20:00:00",
  900000 //time window
) {

  def main(args: Array[String]): Unit = {
    /*
    Problem:
     */

    val exLoop = (EventVar(".+").defineActivity("a1").defineTimestamp("t1") >|> EventVar(".+").defineActivity("a2").defineTimestamp("t2")).where(
      (_, t, a) => {
        val a1 = a.get("a1")
        val a2 = a.get("a2")
        val t1 = t.get("t1")
        val t2 = t.get("t2")
        a1.isDefined && a2.isDefined &&
          t1.isDefined && t2.isDefined &&
          a1.get == a2.get &&
          (t2.get - t1.get) >= Duration.ofMinutes(1).toMillis
      }
    )

    val t = TraceExpression()

    val filteredLog = logMovements // could be Spark or single-machine impl. (parallel collections)
      .map(t trimToTimeframe(from, until))
      .filter(exLoop)


    CsvWriter.logToCsvLocalFilesystem(filteredLog.filterByAttributeNames(TrackingReportSchema.Schema),
      s"${config.outDir}/filtered_log_loops.csv",
      csvExportHelper.timestamp2String,
      TrackingReportSchema.Schema.toArray : _*)

    println(s"""Pre-processing took ${(new Date().getTime - appStartTime.getTime) / 1000} seconds.""")
  }
}
