package org.processmining.scala.applications.mhs.bhs.t3.scenarios.spark.ml

import java.util.Date

import org.processmining.scala.applications.mhs.bhs.t3.scenarios.spark.T3Session


private object FileNameSettings {
  val inDir = "f:/zip"
  val outDir = s"$inDir/dst"

  val year = "2018"
  val month = "03"
  val outFileName = s"$outDir/$year.$month.01.csv"

  def predicate(s: String): Boolean = s.substring(4, 6) == month
}

private object LogConverter extends T3Session(
  FileNameSettings.inDir,
  FileNameSettings.outDir,
  "19-07-2016 12:00:00",
  "20-07-2016 22:00:00",
  900000, //time window
  FileNameSettings.predicate
) {

  val isLateBag = "isLateBag"


  def main(args: Array[String]): Unit = {

    val lateBagsIds = bpiImporter.loadLateBagIdsFromCsv(spark).collect().toSet

    println("Late bags loaded seconds.")

    val log = logMovementsWoTimeFrameTrim
      .map(x => (x._1, x._2.map(e => e.copy("isLateBag", lateBagsIds.contains(x._1.id)))))

    export(log, FileNameSettings.outFileName, isLateBag)

    println(s"""Pre-processing took ${(new Date().getTime - appStartTime.getTime) / 1000} seconds.""")
  }
}

