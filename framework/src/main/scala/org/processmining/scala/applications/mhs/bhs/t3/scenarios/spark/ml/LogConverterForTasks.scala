package org.processmining.scala.applications.mhs.bhs.t3.scenarios.spark.ml

import java.util.Date

import org.processmining.scala.applications.mhs.bhs.t3.scenarios.spark.T3Session
import org.processmining.scala.log.common.unified.log.spark.UnifiedEventLog


private object TaskFileNameSettings {
  val inDir = "g:/full_converted"
  val outDir = s"g:/tasks"

  val year = "2018"
  val month = "03"
  val outFileName = s"$outDir/$year.$month.01.csv"

  def predicate(s: String): Boolean = s.substring(4, 6) == month
}

private object LogConverterForTasks extends T3Session(
  TaskFileNameSettings.inDir,
  TaskFileNameSettings.outDir,
  "19-07-2016 12:00:00",
  "20-07-2016 22:00:00",
  900000, //time window
  TaskFileNameSettings.predicate
) {


  def switchToLpc(log: UnifiedEventLog, lpcByPid: Map[String, String]): UnifiedEventLog = {

    UnifiedEventLog.create(
      log.events().map(x => {
        val lpc = lpcByPid.get(x._1.id)
        val newId = if (lpc.isDefined) lpc.get else x._1.id
        (newId, x._2)
      }))
  }


  def main(args: Array[String]): Unit = {

    //val lpcByPid = bpiImporter.loadLpcFromCsv(spark).collect().toMap

    val logTaskReport = UnifiedEventLog.create(eventsTaskReport).persist()

//    println(s"Size before: ${logTaskReport.count()}")
//    val lpcTasksLog = switchToLpc(logTaskReport, lpcByPid).persist()
//    println(s"Size after: ${lpcTasksLog.count()}")
    export(logTaskReport, TaskFileNameSettings.outFileName)


//    val logLocationsTasks = logMovementsWoTimeFrameTrim.persist() // fullOuterJoin logTaskReport
//    println(s"Size before: ${logLocationsTasks.count()}")
//    val log = switchToLpc(logLocationsTasks, lpcByPid).persist()
//    println(s"Size after: ${log.count()}")
//
//    //    val log = logMovementsWoTimeFrameTrim
//    //      .map(x => (x._1, x._2.map(e => e.copy("isLateBag", lateBagsIds.contains(x._1.id)))))
//    //
//    export(log, "g:/tmp/log_lpc.csv")

    println(s"""Pre-processing took ${(new Date().getTime - appStartTime.getTime) / 1000} seconds.""")
  }
}


