package org.processmining.scala.applications.mhs.bhs.t3.scenarios.spark

import java.time.Duration
import java.util.Date

private object SpectrumMinerReport extends T3Session(
  "d:/logs/20160719",
  "D:/tmp/evaluation/july_19/spectrum",
  "19-07-2016 01:00:00",
  "20-07-2016 01:00:00",
  Duration.ofMinutes(15).toMillis //time window
) {
  def main(args: Array[String]): Unit = {
    report()
    println(s"""Pre-processing took ${(new Date().getTime - appStartTime.getTime) / 1000} seconds.""")
  }
}
