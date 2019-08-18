package org.processmining.scala.applications.mhs.bhs.ein

import java.io.File
import java.time.Duration

import org.processmining.scala.applications.common.spark.EvaluationHelper
import org.processmining.scala.log.common.filtering.expressions.events.regex.impl.EventEx
import org.processmining.scala.log.common.unified.log.spark.UnifiedEventLog
import org.processmining.scala.log.common.xes.parallel.XesReader
import org.processmining.scala.log.common.filtering.expressions.traces._
import org.processmining.scala.log.common.unified.event.UnifiedEvent
import org.processmining.scala.log.common.unified.log.common.UnifiedEventLog.UnifiedTrace
import org.processmining.scala.log.common.unified.trace.{UnifiedTraceId, UnifiedTraceIdImpl}

class BaseFileNames(val name: String, val simName: String, val numSlices: Int) extends Serializable {
  private val Root = s"D:/logs/EIN/$name/"
  val Log = s"$Root/real_logs/xes/$name.xes"
  val LpcLogNotSplit = s"$Root/real_logs/xes/lpc_not_split_$name.xes"
  val LpcLog = s"$Root/real_logs/xes/lpc_$name.xes"
  val LpcLogComplete = s"$Root/real_logs/xes/lpc_complete_$name.xes"
  val LpcLogModelActivities = s"$Root/real_logs/xes/complete_model_activities_$name.xes"
  val SimLog = s"$Root/sim_logs/xes/sim_$name$simName.xes"
  val SimScenario = s"$Root/scenarios/input_$name.txt"
  val SimScenarioNotSplit = s"$Root/scenarios/input_not_split_$name.txt"
  new File(s"$Root/scenarios").mkdirs()
  new File(s"$Root/sim_logs/csv").mkdirs()
  new File(s"$Root/sim_logs/xes").mkdirs()
  val analysis = s"$Root/analysis/$name$simName/"
  val efr_img = "efr_img/"
  val efr_ds_html = s"$analysis/efr_distr.html"
  val dfr_ds_html = s"$analysis/dfr_distr.html"
  val dfr_img = "dfr_img/"

  def efr_dse_html(title: String) = s"$analysis/efr_$title.html"

  def dfr_dse_html(title: String) = s"$analysis/dfr_$title.html"

  def psmData(title: String) = s"$analysis/$title$simName/"

}

//object FileNames extends BaseFileNames("1d_161217", "_replay", -1)

//object FileNames extends BaseFileNames("1m_since161217", "_replay", 128)
object FileNames extends BaseFileNames("1m_since161217", "_all_random_10_removed", 128)

class EindhovenTemplate extends EvaluationHelper(s"EIN ${FileNames.name}") {
  val t = org.processmining.scala.log.common.filtering.expressions.traces.impl.TraceExpression()
  val NumSlices = FileNames.numSlices
  val LpcAttrName = "LPC"
  val PidAttrName = "PID"
  val LpcLifecycleDurationDays = 1

  val logPid: UnifiedEventLog = org.processmining.scala.log.common.xes.spark.XesReader.xesToUnifiedEventLog(spark,
    NumSlices,
    XesReader.readXes(new File(FileNames.Log)).head,
    Some(Set()),
    Some(Set("concept:name", "time:timestamp", "LOCATION", "LPC")),
    XesReader.DefaultActivitySep)

  val expPropagateFirstNonEmptyLpcAttr = t modifyEvents { (trace, event, i) =>
    val optFirstEventWithNonEmptyLpc = trace._2.find { e =>
      val optLpc = e.attributes.get(LpcAttrName)
      optLpc.isDefined && optLpc.get.toString.nonEmpty
    }
    val traceId = trace._1.id
    val lpc = if (optFirstEventWithNonEmptyLpc.isDefined) optFirstEventWithNonEmptyLpc.get.attributes(LpcAttrName) else traceId
    event.copy(LpcAttrName, lpc).copy(PidAttrName, traceId)
  }

  def splitDistantSubtraces(delta: Long)(t: UnifiedTrace): List[UnifiedTrace] = {

    val id = t._1.id
    t._2.foldLeft((List[(String, UnifiedEvent)](), 0, None: Option[Long])) { (z, e) =>
      val suffix = if (z._2 == 0) "" else s"_${z._2}"
      if (!z._3.isDefined || e.timestamp - z._3.get < delta) ((id + suffix, e) :: z._1, z._2, Some(e.timestamp))
      else {
        val newSuffixValue = z._2 + 1
        val newSuffix = s"_$newSuffixValue"
        ((id + newSuffix, e) :: z._1, newSuffixValue, Some(e.timestamp))
      }
    }._1
      .groupBy(_._1)
      .map(p => (new UnifiedTraceIdImpl(p._1), p._2.map(_._2)))
      .toList
  }

  val logNotSplit = UnifiedEventLog.create(
    logPid
      .map(expPropagateFirstNonEmptyLpcAttr)
      .events()
      .map(_._2)
      .filter((_.activity.nonEmpty))
      .filter(_.attributes(LpcAttrName).toString.nonEmpty)
      .map(e => (e.attributes(LpcAttrName).toString, e)))


  val log = logNotSplit.flatMap(splitDistantSubtraces(Duration.ofDays(LpcLifecycleDurationDays).toMillis)(_))


  val logWithTooLongTraces =
    log.filter(t.duration(Duration.ofDays(LpcLifecycleDurationDays).toMillis, Duration.ofDays(LpcLifecycleDurationDays * 1000).toMillis))

  lazy val simLog: UnifiedEventLog = org.processmining.scala.log.common.xes.spark.XesReader.xesToUnifiedEventLog(spark,
    NumSlices,
    XesReader.readXes(new File(FileNames.SimLog)).head,
    None,
    None,
    XesReader.DefaultActivitySep)

  val exRegistrationOnStart11 = EventEx("R\\.11\\.40\\.2")
  val exRegistrationOnStart14 = EventEx("R\\.14\\.40\\.1")
  val exLateral = EventEx("21\\.[34][0-8]\\.1")
  val exLateralDeregistration = EventEx("D\\.21\\.[34][0-8].*")
  val exNotRegistrationDeregistration = EventEx("\\d.*")

  //complete traces start from exRegistrationOnStart11/14 and end with laterals
  val exCompleteTrace =
    (t matches (exRegistrationOnStart11 >-> exLateral)) or (t matches (exRegistrationOnStart14 >-> exLateral)) or
      (t matches (exRegistrationOnStart11 >-> exLateralDeregistration)) or (t matches (exRegistrationOnStart14 >-> exLateralDeregistration))

  val logComplete = log
    .remove(EventEx("NA"), EventEx(""))
    .filter(exCompleteTrace)

  val activitiesThatExistInSimModel = Seq(exRegistrationOnStart11,
    exRegistrationOnStart14,
    exLateral,
    EventEx("11\\.40\\.ScreenL1L2"),
    EventEx("14\\.40\\.ScreenL1L2"),
    EventEx("14\\.66\\.ScreenL1L2"),
    EventEx("14\\.66\\.ScreenL3"),
    EventEx("14\\.70\\.ScreenL4"),
    EventEx("1[14]\\.52\\.1"),
    EventEx("11\\.78\\.1"),
    EventEx("21\\.2[01]\\.3")
  )

  val logForComparativeAnalysis =
    logComplete
      .project(activitiesThatExistInSimModel: _*)


}
