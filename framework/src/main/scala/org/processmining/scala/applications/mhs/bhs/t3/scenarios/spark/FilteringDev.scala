package org.processmining.scala.applications.mhs.bhs.t3.scenarios.spark

import java.util.Date


private object FilteringDev extends T3Session(
  "C:\\evaluation\\input\\20171001",
  "C:\\evaluation\\output\\20171001_xxx",
  "01-10-2017 12:00:00",
  "02-10-2017 12:00:00",
  900000 //time window
) {

  def main(args: Array[String]): Unit = {


    report()

//    CsvWriter.logToCsvLocalFilesystem(logMovements,
//      s"${config.outDir}/log.csv",
//      csvExportHelper.timestamp2String,
//      BpiImporter.TrackingReportAttributesSchema)

    println(s"""Pre-processing took ${(new Date().getTime - appStartTime.getTime) / 1000} seconds.""")
  }
}
