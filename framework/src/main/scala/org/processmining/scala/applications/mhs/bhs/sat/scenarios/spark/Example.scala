package org.processmining.scala.applications.mhs.bhs.sat.scenarios.spark

import java.time.Duration

object Example extends SATSessionTemplate("D:\\logs\\SAT", "D:\\logs\\SAT\\output", Duration.ofMinutes(15).toMillis) {
  def main(args: Array[String]): Unit = {
    export(log, "D:\\logs\\SAT\\output\\log.csv")
    report()
  }

}
