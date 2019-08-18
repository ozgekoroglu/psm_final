package org.processmining.scala.applications.mhs.bhs.ein

import java.awt.Color
import java.io.{File, PrintWriter}
import java.text.{DecimalFormat, DecimalFormatSymbols}
import java.time.Duration
import java.util.Locale
import java.util.concurrent.TimeUnit

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import org.processmining.scala.log.common.enhancment.segments.common.FasterNormal23VerySlowDurationClassifier
import org.processmining.scala.log.common.filtering.expressions.events.regex.impl.EventEx
import org.processmining.scala.log.common.filtering.expressions.traces._
import org.processmining.scala.log.common.unified.event.UnifiedEvent
import org.processmining.scala.log.common.unified.log.spark.UnifiedEventLog
import org.processmining.scala.log.common.utils.common.export._
import org.processmining.scala.log.common.utils.common.export.MatrixOfRelationsExporter

object EIN extends EindhovenTemplate {


  def makeScenario(someLog: UnifiedEventLog, filename: String): Unit = {
    val attrScreenL2 = "ScreenL2"
    val attrScreenL3 = "ScreenL3"
    val attrScreenL4 = "ScreenL4"
    val attrDestination = "Destination"
    val attrDereg = "Dereg"
    val cleanLogWithAdditionalAttrs = someLog
      .mapIf(t contains EventEx("14.66.ScreenL3"),
        t modifyEvents ((_, e, i) => if (i == 0) e.copy(attrScreenL3, 1) else e),
        t modifyEvents ((_, e, i) => if (i == 0) e.copy(attrScreenL3, 0) else e))
      .mapIf(t contains EventEx("14.66.ScreenL1L2"),
        t modifyEvents ((_, e, i) => if (i == 0) e.copy(attrScreenL2, 1) else e),
        t modifyEvents ((_, e, i) => if (i == 0) e.copy(attrScreenL2, 0) else e))
      .mapIf(t contains EventEx("14.70.ScreenL4"),
        t modifyEvents ((_, e, i) => if (i == 0) e.copy(attrScreenL4, 1) else e),
        t modifyEvents ((_, e, i) => if (i == 0) e.copy(attrScreenL4, 0) else e))
      .project(activitiesThatExistInSimModel: _*)
      .map(
        t modifyEvents { (t, e, i) =>
          if (i == 0) {
            val pid = e.attributes(PidAttrName).toString
            val opt = t._2.zipWithIndex.find(_._1.attributes(PidAttrName) != pid)
            if (opt.isDefined) {
              val index = opt.get._2 - 1
              e.copy(attrDereg, t._2(index).activity)
            } else e.copy(attrDereg, "none")
          } else e
        }
      )
      .map(t modifyEvents ((t, e, i) => if (i == 0) e.copy(attrDestination, t._2.last.activity) else e))

    val ddd = cleanLogWithAdditionalAttrs.traces().collect()

    val events = cleanLogWithAdditionalAttrs
      .map(t take 1)
      .events()
      .collect()
      .sortBy(_._2.timestamp)

    val df = new DecimalFormat("#.######", DecimalFormatSymbols.getInstance(Locale.US))
    val shift = events.head._2.timestamp
    val lines = events.
      foldLeft((List[String](), shift)) { (z, pair) =>
        val (id, e) = (pair._1.id, pair._2)
        val delta = df.format((e.timestamp - z._2) / 1000.0)
        val line = s"$id\t$delta\t${e.activity}\t${e.attributes(attrDestination)}\t${e.attributes(attrScreenL2)}\t${e.attributes(attrScreenL3)}\t${e.attributes(attrScreenL4)}\t${e.attributes(attrDereg)}"
        (line :: z._1, e.timestamp)
      }.
      _1
      .reverse
    val pw = new PrintWriter(filename)
    pw.println(df.format(shift / 1000.0))
    lines.foreach(pw.println)
    pw.println("exit")
    pw.close()
  }

  def exportEfrDfr(logs: Seq[UnifiedEventLog], isEfr: Boolean) = {
    val sortedActivities = List[String](
      "R.14.40.1", "14.40.ScreenL1L2", "14.52.1", "14.66.ScreenL1L2", "14.66.ScreenL3", "14.70.ScreenL4", "21.20.3",
      "R.11.40.2", "11.40.ScreenL1L2", "11.52.1", "11.78.1", "21.21.3",
      "21.30.1", "21.31.1", "21.32.1", "21.33.1", "21.34.1", "21.35.1", "21.36.1", "21.37.1", "21.38.1",
      "21.40.1", "21.41.1", "21.42.1", "21.43.1", "21.44.1", "21.45.1", "21.46.1", "21.47.1", "21.48.1"
    )

    val relImg = if (isEfr) FileNames.efr_img else FileNames.dfr_img
    val absImg = s"${FileNames.analysis}/$relImg"
    new File(absImg).mkdirs()

    val dsMatrices = logs.map(_.combineAndReduce[Long, DescriptiveStatistics, (DescriptiveStatisticsEntry, String)](
      if (isEfr) EventRelations.efr[Long]((_, _) => false, _: List[UnifiedEvent], -_.timestamp + _.timestamp) else EventRelations.dfr[Long]((_, _) => false, _: List[UnifiedEvent], -_.timestamp + _.timestamp),
      EventRelationsStatistics.createCombiner,
      EventRelationsStatistics.scoreCombiner,
      EventRelationsStatistics.scoreMerger,
      EventRelationsStatistics.reducerWithDistr(50,
        (k1, k2) => (s"$absImg/$k1$k2.png", s"$relImg/$k1$k2.png"),
        1.0 / (24 * 60 * 60 * 1000.0),
        if (isEfr) Color.RED else Color.BLUE,
        80,
        60
      )))
      .map(MatrixOfRelations(_,
        MatrixOfRelations.sortByOrderedList(sortedActivities, _: Set[String]),
        (EmptyDescriptiveStatisticsEntry, "")))

    MatrixOfRelationsExporter.exportDs(dsMatrices,
      if (isEfr) FileNames.efr_ds_html else FileNames.dfr_ds_html, sortedActivities)

    val dseMatrices = dsMatrices.map(p => (p._1.map(_.map(x => (x._1._1, x._2._1, x._3))), p._2))
    MatrixOfRelationsExporter.exportDse(dseMatrices,
      if (isEfr) FileNames.efr_dse_html else FileNames.dfr_dse_html, sortedActivities, decimalFormatHelper)

  }

  private def decimalFormatHelper(msD: Double): String = {
    if (msD.isNaN) "NaN" else {
      val ms = msD.asInstanceOf[Long]
      val s = "%02d:%02d:%02d".format(
        TimeUnit.MILLISECONDS.toHours(ms) - TimeUnit.DAYS.toHours(TimeUnit.MILLISECONDS.toDays(ms)),
        TimeUnit.MILLISECONDS.toMinutes(ms) - TimeUnit.HOURS.toMinutes(TimeUnit.MILLISECONDS.toHours(ms)),
        TimeUnit.MILLISECONDS.toSeconds(ms) - TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(ms))
      )
      if (ms == -1000) "" else if (ms == 0) "0" else s
    }
  }


  def createScenario(): Unit = {
    logNotSplit.persist()
    exportXes(logNotSplit, s"${FileNames.LpcLogNotSplit}")
    log.persist()
    exportXes(log, s"${FileNames.LpcLog}")
    logComplete.persist()
    exportXes(logComplete, s"${FileNames.LpcLogComplete}")
    logForComparativeAnalysis.persist()
    exportXes(logForComparativeAnalysis, s"${FileNames.LpcLogModelActivities}")
    makeScenario(logForComparativeAnalysis, FileNames.SimScenario)
  }

  def analyze() = {
    simLog.persist()
    logForComparativeAnalysis.persist()
    exportEfrDfr(Seq(logForComparativeAnalysis, simLog), false)
    exportEfrDfr(Seq(logForComparativeAnalysis, simLog), true)

    reportDurations(simLog,
      FileNames.psmData("sim_model"), Duration.ofMinutes(1).toMillis, new FasterNormal23VerySlowDurationClassifier)

//    reportDurations(logForComparativeAnalysis,
//      FileNames.psmData("real_log_model_activities"), Duration.ofMinutes(1).toMillis, new FasterNormal23VerySlowDurationClassifier)
  }


  def main(args: Array[String]): Unit = {
    logger.info(s"EIN ${FileNames.name} started")
    //createScenario()
    analyze()
  }

}


