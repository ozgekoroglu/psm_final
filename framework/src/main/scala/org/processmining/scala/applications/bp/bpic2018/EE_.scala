package org.processmining.scala.applications.bp.bpic2018

import java.awt.Color
import java.io._
import java.time.temporal.ChronoUnit
import java.time.{Duration, ZoneId}

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import org.processmining.scala.log.common.enhancment.segments.common.{Q4DurationClassifier, StartAggregation}
import org.processmining.scala.log.common.filtering.expressions.events.regex.impl.EventEx
import org.processmining.scala.log.common.filtering.expressions.traces._
import org.processmining.scala.log.common.unified.event.UnifiedEvent
import org.processmining.scala.log.common.unified.log.spark.UnifiedEventLog
import org.processmining.scala.log.common.unified.log.common.UnifiedEventLog.UnifiedTrace
import org.processmining.scala.log.common.utils.common.export.MatrixOfRelationsExporter
import org.processmining.scala.log.common.utils.common._
import org.processmining.scala.log.common.utils.common.export._
import org.processmining.scala.log.common.xes.parallel.XesReader
import org.processmining.scala.log.utils.common.csv.common.CsvExportHelper

import scala.io.Source


object EE_ extends Bpic2018Template {


  /**
    * create logs for testing 1%
    */
  def step_1a() = {
    val originalLogPath = FileNames.OriginalLogOnlyMainAttrs
    val newLogPath = FileNames.testLog2_sample01
    val newLogPathPSM = FileNames.testLog2_sample01_PSM
    createLogForTesting(originalLogPath, newLogPath, 0.01)
    reportDurationsForDays(loadStrippedLog(newLogPath), newLogPathPSM, Duration.ofDays(1).toMillis, false)
  }

  /**
    * create logs for testing 0.1%
    */
  def step_1b() = {
    val originalLogPath = FileNames.OriginalLogOnlyMainAttrs
    val newLogPath = FileNames.testLog3_sample001
    val newLogPathPSM = FileNames.testLog3_sample001_PSM
    createLogForTesting(originalLogPath, newLogPath, 0.001)
    reportDurationsForDays(loadStrippedLog(newLogPath), newLogPathPSM, Duration.ofDays(1).toMillis, false)
  }

  /**
    * create log with only main attributes and stripped
    */
  def step_2_createCompleteLogWithOnlyMainAttributesAndStripped = {
    val log: UnifiedEventLog = org.processmining.scala.log.common.xes.spark.XesReader.xesToUnifiedEventLog(spark,
      NumSlices,
      XesReader.readXes(new File(FileNames.OriginalLog)).head,
      Some(TraceAttributes.SetOfMainAttributes),
      Some(EventAttributes.SetOfMainAttributes),
      XesReader.DefaultActivitySep, DefaultClassifier: _*)
    exportExt(log, FileNames.OriginalLogOnlyMainAttrs, addStripped = true)
  }

  //  def step_3new_renameLast = {
  //    val exDecide = EventEx(".+")
  //      .withValue(EventAttributes.Doctype, "Payment application")
  //      .withValue(EventAttributes.Subprocess, "Application")
  //      .withValue(EventAttributes.Activity, "decide")
  //      .withValue(EventAttributes.Success, true)
  //    val exRevokedDecision = EventEx(".+")
  //      .withValue(EventAttributes.Doctype, "Payment application")
  //      .withValue(EventAttributes.Subprocess, "Application")
  //      .withValue(EventAttributes.Activity, "revoke decision")
  //      .withValue(EventAttributes.Success, true)
  //    val exLastDecide = exDecide >-> exRevokedDecision ///////////////////////
  //    def renameLastActivity(e: UnifiedEvent, counter: Int): UnifiedEvent =  e.copy("decide Last")
  //
  //    val log = loadLog(FileNames.OriginalLogOnlyMainAttrs)
  //    val newLog = log.map (t keepLast(exDecide, exLastDecide, renameLastActivity(_: UnifiedEvent, _: Int)))
  //    exportExt(log, FileNames.OriginalLogOnlyMainAttrs, addStripped = true)
  //  }

  /**
    * TODO rewrite
    */
  def step_3_splitCompleteLogByYears() = {
    splitByYears()
  }


  def step_4a_completeLogToPsm_1day(): Unit = {
    reportDurationsForDays(logCompleteActivitiesStripped, FileNames.completePsmActivities, Duration.ofDays(1).toMillis, false)
    reportDurationsForDays(logCompleteActivitiesStripped, FileNames.completePsmResources, Duration.ofDays(1).toMillis, false)
  }

  def step_4b_completeLogToPsm_1week(): Unit = {
    reportDurationsForWeeks(logCompleteActivitiesStripped, FileNames.completePsmActivities_1week, Duration.ofDays(7).toMillis, false)
  }

  def step_4c_completeLogToPsm_1week(): Unit = {
    reportAttribute(
      logCompleteActivitiesStripped,
      s"${FileNames.rootYear("allYears")}/psm/ConceptName/complete_activities_1week.4q",
      Duration.ofDays(7).toMillis, EventAttributes.ConceptName, "")
  }

  def step_5_allYearsToPsm(): Unit = {
    reportDurationsForDays(log1_2015, FileNames.completePsmDurationForYear("2015"), Duration.ofDays(1).toMillis, false)
    reportDurationsForDays(log1_2016, FileNames.completePsmDurationForYear("2016"), Duration.ofDays(1).toMillis, false)
    reportDurationsForDays(log1_2017, FileNames.completePsmDurationForYear("2017"), Duration.ofDays(1).toMillis, false)
  }

  /** Main activities v2
    * filtering лога на каждый год по списку ключевых активити
    * (лучше использовать уже разделенный лог тк целый лог с трудом помещается в память)
    */
  def step_10a() = {
    (2015 to 2017).foreach { year => {
      val log = getLogByYear(year).project(
        EventEx("Payment application-Application-mail income"),
        EventEx("Payment application-Application-mail valid"),
        EventEx("Control summary-Main-initialize"),
        EventEx("Parcel document-Main-initialize"),
        EventEx("Geo parcel document-Main-initialize"),
        EventEx("Reference alignment-Main-initialize"),
        EventEx("Reference alignment-Main-performed"),
        EventEx("Department control parcels-Main-performed"),
        EventEx("Entitlement application-Main-initialize"),
        EventEx("Entitlement application-Main-decide"),
        EventEx("Payment application-Application-initialize"),
        EventEx("Payment application-Application-decide"),
        EventEx("Payment application-Application-begin payment"),
        EventEx("Payment application-Application-abort payment"),
        EventEx("Payment application-Application-finish payment")
      )
      exportExt(log, s"${FileNames.rootYear(year.toString)}/level_3_${year}_mainActiv2.xes", true)
    }
    }
  }

  /** Main activities v2
    * объединение отфильтрованных логов
    * (тк целый лог с трудом помещается в память)
    */
  def step_10b() = {
    val level_3_2015_mainActiv2 = loadLog(FileNames.level_3_2015_mainActiv2)
    val level_3_2016_mainActiv2 = loadLog(FileNames.level_3_2016_mainActiv2)
    val level_3_2017_mainActiv2 = loadLog(FileNames.level_3_2017_mainActiv2)
    val logMerged = level_3_2015_mainActiv2.fullOuterJoin(level_3_2016_mainActiv2).fullOuterJoin(level_3_2017_mainActiv2)
    reportDurationsForDays(logMerged, FileNames.level_3_201567_mainActiv2_PSM,
      Duration.ofDays(1).toMillis, false)
  }

  /** Main activities v2
    * создание лога 201567 со смещенными годами и подсчет для PSM
    */
  def step_10c() = {
    val shiftedLog2015 = shiftYears(loadLog(FileNames.level_3_2015_mainActiv2), "2015", 0)
    val shiftedLog2016 = shiftYears(loadLog(FileNames.level_3_2016_mainActiv2), "2016", 1)
    val shiftedLog2017 = shiftYears(loadLog(FileNames.level_3_2017_mainActiv2), "2017", 2)
    val logMerged = shiftedLog2015.fullOuterJoin(shiftedLog2016).fullOuterJoin(shiftedLog2017).persist()
    val newLogPath = FileNames.level_3_201567_shiftedYears_mainActiv2
    exportExt(logMerged, newLogPath, false)
    reportDurationsForDays(logMerged, FileNames.level_3_201567_shiftedYears_mainActiv2_PSM,
      Duration.ofDays(1).toMillis, false)
    logMerged.unpersist()
    reportAttribute(
      loadStrippedLogWithYear(newLogPath),
      s"${FileNames.rootYear("allYears")}/psm/ConceptName/level_3_201567_shiftedYears_mainActiv2",
      Duration.ofDays(1).toMillis, EventAttributes.ConceptName, "")
  }

  /** Main activities v2
    * создание подсчет для ПСМ level_4_201567_shiftedYears_mainActiv2_keyVariants_PSM
    */
  def step_20a() = {
    shiftYears_LogWithDefaultClassifier(
      FileNames.level_4_2015_mainActiv2_keyVariants,
      FileNames.level_4_2016_mainActiv2_keyVariants,
      FileNames.level_4_2017_mainActiv2_keyVariants,
      FileNames.level_4_201567_shiftedYears_mainActiv2_keyVariants_PSM)
  }

  def step_20b() = {
    val newFileNamePSM = s"${FileNames.rootYear("allYears")}/psm/ConceptName/level_4_201567_shiftedYears_mainActiv2_keyVariants_PSM"
    shiftYears_LogWithDefaultClassifier_reportAttribute(
      FileNames.level_4_2015_mainActiv2_keyVariants,
      FileNames.level_4_2016_mainActiv2_keyVariants,
      FileNames.level_4_2017_mainActiv2_keyVariants,
      newFileNamePSM)
  }

  def step_20c() = {
    val logNames = Seq(
      FileNames.level_4_2015_mainActiv2_keyVariants,
      FileNames.level_4_2016_mainActiv2_keyVariants,
      FileNames.level_4_2017_mainActiv2_keyVariants)
    val sortedOrderPath = FileNames.sorting_order_template_byOriginalActivities_v8
    val newFileName = "level_4_201567_mainActiv2_keyVariants_sorting8_5perc"
    exportEfrDfrFootprint(logNames, false, sortedOrderPath, newFileName)
    exportEfrDfrFootprint(logNames, true, sortedOrderPath, newFileName)
    exportEfrDfr(logNames, true, sortedOrderPath, newFileName)
    exportEfrDfr(logNames, false, sortedOrderPath, newFileName)
  }

  def step_20d() = { //34699
    val newLogPath = FileNames.level_4_201567_mainActiv2_keyVariants
    val log2015 = loadLog(FileNames.level_4_2015_mainActiv2_keyVariants)
    val log2016 = loadLog(FileNames.level_4_2016_mainActiv2_keyVariants)
    val log2017 = loadLog(FileNames.level_4_2017_mainActiv2_keyVariants)
    val logMerged = log2015.fullOuterJoin(log2016).fullOuterJoin(log2017)
    exportExt(logMerged, newLogPath, false)
  }

  def step_20dd() = { //34699
    val ids = Source.fromFile("C:\\bpic2018_work\\filtering by ids\\level_4_201567_mainActiv2_keyVariants_34699.txt").getLines.toList
    val newLog = logOnlyMainAttrs.filterByTraceIds(ids: _*).project(
      EventEx("Payment application-Application-mail income"),
      EventEx("Payment application-Application-mail valid"),
      EventEx("Control summary-Main-initialize"),
      EventEx("Parcel document-Main-initialize"),
      EventEx("Geo parcel document-Main-initialize"),
      EventEx("Reference alignment-Main-initialize"),
      EventEx("Reference alignment-Main-performed"),
      EventEx("Department control parcels-Main-performed"),
      EventEx("Entitlement application-Main-initialize"),
      EventEx("Entitlement application-Main-decide"),
      EventEx("Payment application-Application-initialize"),
      EventEx("Payment application-Application-decide"),
      EventEx("Payment application-Application-begin payment"),
      EventEx("Payment application-Application-abort payment"),
      EventEx("Payment application-Application-finish payment")
    )
    val newLogPath = FileNames.level_4_201567_mainActiv2_keyVariants
    exportExt(newLog, newLogPath, false)
  }

  def step_20ddd() = { //34699
    val newLogPath = FileNames.level_44_201567_mainActiv3_keyVariants
    val ids = Source.fromFile("C:\\bpic2018_work\\filtering by ids\\level_4_201567_mainActiv2_keyVariants_34699.txt").getLines.toList
    val newLog = logOnlyMainAttrs.filterByTraceIds(ids: _*).project(
      EventEx("Payment application-Application-mail income"),
      EventEx("Payment application-Application-mail valid"),
      EventEx("Control summary-Main-initialize"),
      EventEx("Parcel document-Main-initialize"),
      EventEx("Geo parcel document-Main-initialize"),
      EventEx("Reference alignment-Main-initialize"),
      EventEx("Department control parcels-Main-performed"),
      EventEx("Entitlement application-Main-initialize"),
      EventEx("Entitlement application-Main-decide"),
      EventEx("Payment application-Application-initialize"),
      EventEx("Payment application-Application-decide"),
      EventEx("Payment application-Application-begin payment"),
      EventEx("Payment application-Application-abort payment"),
      EventEx("Payment application-Application-finish payment")
    )
    exportExt(newLog, newLogPath, false)
  }

  def step_20e() = {
    val newFileNamePSM = s"${FileNames.rootYear("allYears")}/psm/ConceptName/level_4_201567_NOTshiftedYears_mainActiv2_keyVariants_PSM"
    val log = loadStrippedLog(FileNames.level_4_201567_mainActiv2_keyVariants)
    reportAttribute(log, newFileNamePSM,
      Duration.ofDays(1).toMillis, EventAttributes.ConceptName, "")
  }

  def step_20ee() = {
    val newFileNamePSM = s"${FileNames.rootYear("allYears")}/psm/ConceptName/level_44_201567_NOTshiftedYears_mainActiv3_keyVariants_PSM"
    val log = loadStrippedLog(FileNames.level_44_201567_mainActiv3_keyVariants)
    reportAttribute(log, newFileNamePSM,
      Duration.ofDays(1).toMillis, EventAttributes.ConceptName, "")
  }

  def step_20f_level_4_201567_NOTshiftedYears_mainActiv2_keyVariants_PSM() = {
    val newFileNamePSM = FileNames.level_4_201567_NOTshiftedYears_mainActiv2_keyVariants_PSM
    val logPath = FileNames.level_4_201567_mainActiv2_keyVariants
    reportDurationsForDays(loadStrippedLog(logPath), newFileNamePSM, Duration.ofDays(1).toMillis, false)
  }

  def step_20ff_level_44_201567_NOTshiftedYears_mainActiv3_keyVariants_PSM() = {
    val newFileNamePSM = FileNames.level_44_201567_NOTshiftedYears_mainActiv3_keyVariants_PSM
    val logPath = FileNames.level_44_201567_mainActiv3_keyVariants
    reportDurationsForDays(loadStrippedLog(logPath), newFileNamePSM, Duration.ofDays(1).toMillis, false)
  }

  /**
    * level_5_201567_keyVariants -- create log
    */
  def step_50a_level_5_201567_keyVariants() = {
    val ids = Source.fromFile("C:\\bpic2018_work\\filtering by ids\\level_4_201567_mainActiv2_keyVariants_34699.txt").getLines.toList
    val newLog = logOnlyMainAttrs.filterByTraceIds(ids: _*) //.persist()
    val newLogPath = FileNames.level_5_201567_keyVariants
    exportExt(newLog, newLogPath, false)
  }

  /**
    * level_5_201567_keyVariants -- to PSM
    */
  def step_50b_level_5_201567_keyVariants_PSM_NOTshiftedYears() = {
    val newFileNamePSM = FileNames.level_5_201567_keyVariants_PSM_NOTshiftedYears
    val logPath = FileNames.level_5_201567_keyVariants
    reportDurationsForDays(loadStrippedLog(logPath), newFileNamePSM, Duration.ofDays(1).toMillis, false)
  }

  /**
    * level_5_***_keyVariants -- shift years and to PSM
    */
  def step_50c_level_5_201567_keyVariants_PSM_shiftedYears() = {
    val originalLogPath = FileNames.level_5_201567_keyVariants
    val (log2015, log2016, log2017) = splitByYears2(originalLogPath, TraceAttributes)
    val newFileNamePSM = FileNames.level_5_201567_keyVariants_PSM_shiftedYears
    shiftYears_LogWithDefaultClassifier(log2015, log2016, log2017, newFileNamePSM)

    val newFileNamePSM2 = s"${FileNames.rootYear("allYears")}/psm/ConceptName/level_5_201567_keyVariants_shiftedYears"
    val mergedLog = log2015.fullOuterJoin(log2016).fullOuterJoin(log2017)
    reportAttribute(mergedLog, newFileNamePSM2,
      Duration.ofDays(1).toMillis, EventAttributes.ConceptName, "")
  }

  /**
    * (Geo) Parcel document from Key variants -- create log 201567 + shift years and to PSM
    */
  def step_60a_level_6_201567_keyVariants_GPD_PSM_shiftedYears() = {
    val newLog = loadLog(FileNames.level_5_201567_keyVariants).project(
      EventEx(".+").withValue(EventAttributes.Doctype, "Parcel document"),
      EventEx(".+").withValue(EventAttributes.Doctype, "Geo parcel document"))
    val newLogPath = FileNames.level_6_201567_keyVariants_GPD
    exportExt(newLog, newLogPath, false)

    val (log2015, log2016, log2017) = splitByYears2(newLog, TraceAttributes)
    val newFileNamePSM = FileNames.level_6_201567_keyVariants_GPD_PSM_shiftedYears
    shiftYears_LogWithDefaultClassifier(log2015, log2016, log2017, newFileNamePSM)
  }

  def step_70a_level_7_201567_keyVariants_decide_PSM_shiftedYears() = {
    val exp = t subtrace EventEx("Payment application-Application-initialize") >->
      EventEx("Payment application-Application-decide")
    val newLog = loadLog(FileNames.level_5_201567_keyVariants).map(exp)
    val newLogPath = FileNames.level_7_201567_keyVariants_decide
    exportExt(newLog, newLogPath, false)

    val (log2015, log2016, log2017) = splitByYears2(newLog, TraceAttributes)
    val newFileNamePSM = FileNames.level_7_201567_keyVariants_decide_PSM_shiftedYears
    shiftYears_LogWithDefaultClassifier(log2015, log2016, log2017, newFileNamePSM)
  }

  def step_70b_level_7_201567_keyVariants_validParcel_PSM_shiftedYears() = {
    val exp1 = t subtrace EventEx("Payment application-Application-mail valid") >->
      EventEx("Parcel document-Main-initialize")
    val exp2 = t subtrace EventEx("Payment application-Application-mail valid") >->
      EventEx("Geo parcel document-Main-initialize")
    val newLog1 = loadLog(FileNames.level_5_201567_keyVariants).map(exp1)
    val newLog2 = loadLog(FileNames.level_5_201567_keyVariants).map(exp2)
    val newLogMerged = newLog1.fullOuterJoin(newLog2)
    val newLogPath = FileNames.level_7_201567_keyVariants_validParcel
    exportExt(newLogMerged, newLogPath, false)

    val (log2015, log2016, log2017) = splitByYears2(newLogMerged, TraceAttributes)
    val newFileNamePSM = FileNames.level_7_201567_keyVariants_validParcel_PSM_shiftedYears
    shiftYears_LogWithDefaultClassifier(log2015, log2016, log2017, newFileNamePSM)

    //    exportEfrDfr(Seq(newLogPath), true)
  }

  def step_70c_level_7_201567_keyVariants_RAperformedPAinitialize_PSM_shiftedYears() = {
    val exp = t subtrace EventEx("Reference alignment-Main-performed") >->
      EventEx("Payment application-Application-initialize")
    val newLog = loadLog(FileNames.level_5_201567_keyVariants).map(exp)
    val newLogPath = FileNames.level_7_201567_keyVariants_RAperformedPAinitialize
    exportExt(newLog, newLogPath, false)

    val (log2015, log2016, log2017) = splitByYears2(newLog, TraceAttributes)
    val newFileNamePSM = FileNames.level_7_201567_keyVariants_RAperformedPAinitialize_PSM_shiftedYears
    shiftYears_LogWithDefaultClassifier(log2015, log2016, log2017, newFileNamePSM)
  }

  def step_80a = {
    reportAttribute(
      loadLogWithYear(FileNames.level_5_201567_keyVariants),
      s"${FileNames.rootYear("allYears")}/psm/ConceptName/level_5_201567_keyVariants",
      Duration.ofDays(1).toMillis, EventAttributes.ConceptName, "")
  }

  def step_80b = {
    reportAttribute(
      loadStrippedLogWithYear(FileNames.OriginalLogShiftedYears),
      s"${FileNames.rootYear("allYears")}/psm/ConceptName/level_0_allYears_ShiftedYears",
      Duration.ofDays(1).toMillis, EventAttributes.ConceptName, "")
  }

  //  def step_80c = {
  //    val log2015 = loadStrippedLogWithYear(FileNames.level_3_2015_mainActiv2)
  //    val log2016 = loadStrippedLogWithYear(FileNames.level_3_2016_mainActiv2)
  //    val log2017 = loadStrippedLogWithYear(FileNames.level_3_2017_mainActiv2)
  //    val newFileNamePSM = FileNames.level_5_201567_keyVariants_PSM_shiftedYears
  //    shiftYears_LogWithDefaultClassifier(log2015, log2016, log2017, newFileNamePSM)
  //
  //    reportAttribute(mergedLog,
  //      s"${FileNames.rootYear("allYears")}/psm/ConceptName/level_3_201567_mainActiv2",
  //      Duration.ofDays(1).toMillis, EventAttributes.ConceptName, "")
  //  }

  /**
    * renameLastEvent
    */
  def step_90a = {
    val newLogPath = FileNames.level_10_201567_activAll_casesAll_sealed
    val originalLog = loadLog(FileNames.OriginalLogOnlyMainAttrs)
    val ex1 = EventEx(".+")
      .withValue(EventAttributes.Doctype, "Payment application")
      .withValue(EventAttributes.Subprocess, "Application")
      .withValue(EventAttributes.Activity, "begin payment")
    val isNotFollowedBy1 = EventEx(".+")
      .withValue(EventAttributes.Doctype, "Payment application")
      .withValue(EventAttributes.Subprocess, "Application")
      .withValue(EventAttributes.Activity, "abort payment")
      .withValue(EventAttributes.Success, "true")

    val ex2 = EventEx(".+")
      .withValue(EventAttributes.Doctype, "Payment application")
      .withValue(EventAttributes.Subprocess, "Application")
      .withValue(EventAttributes.Activity, "decide")
    val isNotFollowedBy2 = EventEx(".+")
      .withValue(EventAttributes.Doctype, "Payment application")
      .withValue(EventAttributes.Subprocess, "Application")
      .withValue(EventAttributes.Activity, "revoke decision")
      .withValue(EventAttributes.Success, "true")

    val ex3 = EventEx(".+")
      .withValue(EventAttributes.Doctype, "Entitlement application")
      .withValue(EventAttributes.Subprocess, "Main")
      .withValue(EventAttributes.Activity, "decide")
    val isNotFollowedBy3 = EventEx(".+")
      .withValue(EventAttributes.Doctype, "Entitlement application")
      .withValue(EventAttributes.Subprocess, "Main")
      .withValue(EventAttributes.Activity, "revoke decision")
      .withValue(EventAttributes.Success, "true")
    val newLog = originalLog
      .map(renameLastEventIf(ex1, isNotFollowedBy1)(_: UnifiedTrace))
      .map(renameLastEventIf(ex2, isNotFollowedBy2)(_: UnifiedTrace))
      .map(renameLastEventIf(ex3, isNotFollowedBy3)(_: UnifiedTrace))

    exportExt(newLog, newLogPath, addStripped = false)
  }

  def step_90aa = {
    val newLogPath = FileNames.level_10_201567_activAll_casesAll_sealed
    val originalLog = loadLog(FileNames.OriginalLogOnlyMainAttrs)
    val ex1 = EventEx("Entitlement application-Change-decide")
      .withValue(EventAttributes.Success, "true")
    val isNotFollowedBy1 = EventEx("Entitlement application-Change-revoke decision")
      .withValue(EventAttributes.Success, "true")

    val ex2 = EventEx("Entitlement application-Change-withdraw")
      .withValue(EventAttributes.Success, "true")
    val isNotFollowedBy2 = EventEx("Entitlement application-Change-revoke withdrawal")
      .withValue(EventAttributes.Success, "true")

    val ex3 = EventEx("Entitlement application-Main-decide")
      .withValue(EventAttributes.Success, "true")
    val isNotFollowedBy3 = EventEx("Entitlement application-Main-revoke decision")
      .withValue(EventAttributes.Success, "true")

    val ex4 = EventEx("Entitlement application-Main-withdraw")
      .withValue(EventAttributes.Success, "true")
    val isNotFollowedBy4 = EventEx("Entitlement application-Main-revoke withdrawal")
      .withValue(EventAttributes.Success, "true")

    val ex5 = EventEx("Entitlement application-Objection-approve")
      .withValue(EventAttributes.Success, "true")
    val isNotFollowedBy5 = EventEx("Entitlement application-Objection-revoke approval")
      .withValue(EventAttributes.Success, "true")

    val ex6 = EventEx("Entitlement application-Objection-decide")
      .withValue(EventAttributes.Success, "true")
    val isNotFollowedBy6 = EventEx("Entitlement application-Objection-revoke decision")
      .withValue(EventAttributes.Success, "true")

    val ex7 = EventEx("Entitlement application-Objection-withdraw")
      .withValue(EventAttributes.Success, "true")
    val isNotFollowedBy7 = EventEx("Entitlement application-Objection-revoke withdrawal")
      .withValue(EventAttributes.Success, "true")

    val ex8 = EventEx("Payment application-Application-begin payment")
      .withValue(EventAttributes.Success, "true")
    val isNotFollowedBy8 = EventEx("Payment application-Application-abort payment")
      .withValue(EventAttributes.Success, "true")

    val ex9 = EventEx("Payment application-Application-decide")
      .withValue(EventAttributes.Success, "true")
    val isNotFollowedBy9 = EventEx("Payment application-Application-revoke decision")
      .withValue(EventAttributes.Success, "true")

    val ex10 = EventEx("Payment application-Application-withdraw")
      .withValue(EventAttributes.Success, "true")
    val isNotFollowedBy10 = EventEx("Payment application-Application-revoke withdrawal")
      .withValue(EventAttributes.Success, "true")

    val ex11 = EventEx("Payment application-Change-begin payment")
      .withValue(EventAttributes.Success, "true")
    val isNotFollowedBy11 = EventEx("Payment application-Change-abort payment")
      .withValue(EventAttributes.Success, "true")

    val ex12 = EventEx("Payment application-Change-decide")
      .withValue(EventAttributes.Success, "true")
    val isNotFollowedBy12 = EventEx("Payment application-Change-revoke decision")
      .withValue(EventAttributes.Success, "true")

    val ex13 = EventEx("Payment application-Change-withdraw")
      .withValue(EventAttributes.Success, "true")
    val isNotFollowedBy13 = EventEx("Payment application-Change-revoke withdrawal")
      .withValue(EventAttributes.Success, "true")

    val ex14 = EventEx("Payment application-Objection-begin payment")
      .withValue(EventAttributes.Success, "true")
    val isNotFollowedBy14 = EventEx("Payment application-Objection-abort payment")
      .withValue(EventAttributes.Success, "true")

    val ex15 = EventEx("Payment application-Objection-approve")
      .withValue(EventAttributes.Success, "true")
    val isNotFollowedBy15 = EventEx("Payment application-Objection-revoke approval")
      .withValue(EventAttributes.Success, "true")

    val ex16 = EventEx("Payment application-Objection-decide")
      .withValue(EventAttributes.Success, "true")
    val isNotFollowedBy16 = EventEx("Payment application-Objection-revoke decision")
      .withValue(EventAttributes.Success, "true")

    val ex17 = EventEx("Payment application-Objection-withdraw")
      .withValue(EventAttributes.Success, "true")
    val isNotFollowedBy17 = EventEx("Payment application-Objection-revoke withdrawal")
      .withValue(EventAttributes.Success, "true")

    val newLog = originalLog
      .map(renameLastEventIf(ex1, isNotFollowedBy1)(_: UnifiedTrace))
      .map(renameLastEventIf(ex2, isNotFollowedBy2)(_: UnifiedTrace))
      .map(renameLastEventIf(ex3, isNotFollowedBy3)(_: UnifiedTrace))
      .map(renameLastEventIf(ex4, isNotFollowedBy4)(_: UnifiedTrace))
      .map(renameLastEventIf(ex5, isNotFollowedBy5)(_: UnifiedTrace))
      .map(renameLastEventIf(ex6, isNotFollowedBy6)(_: UnifiedTrace))
      .map(renameLastEventIf(ex7, isNotFollowedBy7)(_: UnifiedTrace))
      .map(renameLastEventIf(ex8, isNotFollowedBy8)(_: UnifiedTrace))
      .map(renameLastEventIf(ex9, isNotFollowedBy9)(_: UnifiedTrace))
      .map(renameLastEventIf(ex10, isNotFollowedBy10)(_: UnifiedTrace))
      .map(renameLastEventIf(ex11, isNotFollowedBy11)(_: UnifiedTrace))
      .map(renameLastEventIf(ex12, isNotFollowedBy12)(_: UnifiedTrace))
      .map(renameLastEventIf(ex13, isNotFollowedBy13)(_: UnifiedTrace))
      .map(renameLastEventIf(ex14, isNotFollowedBy14)(_: UnifiedTrace))
      .map(renameLastEventIf(ex15, isNotFollowedBy15)(_: UnifiedTrace))
      .map(renameLastEventIf(ex16, isNotFollowedBy16)(_: UnifiedTrace))
      .map(renameLastEventIf(ex17, isNotFollowedBy17)(_: UnifiedTrace))

    exportExt(newLog, newLogPath, addStripped = false)
  }

  def step_1001_create_log_L2_sealed = {
    val newLogPath = FileNames.log_L2_activ187_cases100_sealed
    val originalLog = loadLog(FileNames.OriginalLog)
    val ex1 = EventEx("Entitlement application-Change-decide")
      .withValue(EventAttributes.Success, "true")
    val isNotFollowedBy1 = EventEx("Entitlement application-Change-revoke decision")
      .withValue(EventAttributes.Success, "true")

    val ex2 = EventEx("Entitlement application-Change-withdraw")
      .withValue(EventAttributes.Success, "true")
    val isNotFollowedBy2 = EventEx("Entitlement application-Change-revoke withdrawal")
      .withValue(EventAttributes.Success, "true")

    val ex3 = EventEx("Entitlement application-Main-decide")
      .withValue(EventAttributes.Success, "true")
    val isNotFollowedBy3 = EventEx("Entitlement application-Main-revoke decision")
      .withValue(EventAttributes.Success, "true")

    val ex4 = EventEx("Entitlement application-Main-withdraw")
      .withValue(EventAttributes.Success, "true")
    val isNotFollowedBy4 = EventEx("Entitlement application-Main-revoke withdrawal")
      .withValue(EventAttributes.Success, "true")

    val ex5 = EventEx("Entitlement application-Objection-approve")
      .withValue(EventAttributes.Success, "true")
    val isNotFollowedBy5 = EventEx("Entitlement application-Objection-revoke approval")
      .withValue(EventAttributes.Success, "true")

    val ex6 = EventEx("Entitlement application-Objection-decide")
      .withValue(EventAttributes.Success, "true")
    val isNotFollowedBy6 = EventEx("Entitlement application-Objection-revoke decision")
      .withValue(EventAttributes.Success, "true")

    val ex7 = EventEx("Entitlement application-Objection-withdraw")
      .withValue(EventAttributes.Success, "true")
    val isNotFollowedBy7 = EventEx("Entitlement application-Objection-revoke withdrawal")
      .withValue(EventAttributes.Success, "true")

    val ex8 = EventEx("Payment application-Application-begin payment")
      .withValue(EventAttributes.Success, "true")
    val isNotFollowedBy8 = EventEx("Payment application-Application-abort payment")
      .withValue(EventAttributes.Success, "true")

    val ex9 = EventEx("Payment application-Application-decide")
      .withValue(EventAttributes.Success, "true")
    val isNotFollowedBy9 = EventEx("Payment application-Application-revoke decision")
      .withValue(EventAttributes.Success, "true")

    val ex10 = EventEx("Payment application-Application-withdraw")
      .withValue(EventAttributes.Success, "true")
    val isNotFollowedBy10 = EventEx("Payment application-Application-revoke withdrawal")
      .withValue(EventAttributes.Success, "true")

    val ex11 = EventEx("Payment application-Change-begin payment")
      .withValue(EventAttributes.Success, "true")
    val isNotFollowedBy11 = EventEx("Payment application-Change-abort payment")
      .withValue(EventAttributes.Success, "true")

    val ex12 = EventEx("Payment application-Change-decide")
      .withValue(EventAttributes.Success, "true")
    val isNotFollowedBy12 = EventEx("Payment application-Change-revoke decision")
      .withValue(EventAttributes.Success, "true")

    val ex13 = EventEx("Payment application-Change-withdraw")
      .withValue(EventAttributes.Success, "true")
    val isNotFollowedBy13 = EventEx("Payment application-Change-revoke withdrawal")
      .withValue(EventAttributes.Success, "true")

    val ex14 = EventEx("Payment application-Objection-begin payment")
      .withValue(EventAttributes.Success, "true")
    val isNotFollowedBy14 = EventEx("Payment application-Objection-abort payment")
      .withValue(EventAttributes.Success, "true")

    val ex15 = EventEx("Payment application-Objection-approve")
      .withValue(EventAttributes.Success, "true")
    val isNotFollowedBy15 = EventEx("Payment application-Objection-revoke approval")
      .withValue(EventAttributes.Success, "true")

    val ex16 = EventEx("Payment application-Objection-decide")
      .withValue(EventAttributes.Success, "true")
    val isNotFollowedBy16 = EventEx("Payment application-Objection-revoke decision")
      .withValue(EventAttributes.Success, "true")

    val ex17 = EventEx("Payment application-Objection-withdraw")
      .withValue(EventAttributes.Success, "true")
    val isNotFollowedBy17 = EventEx("Payment application-Objection-revoke withdrawal")
      .withValue(EventAttributes.Success, "true")

    val newLog = originalLog
      .map(renameLastEventIf(ex1, isNotFollowedBy1)(_: UnifiedTrace))
      .map(renameLastEventIf(ex2, isNotFollowedBy2)(_: UnifiedTrace))
      .map(renameLastEventIf(ex3, isNotFollowedBy3)(_: UnifiedTrace))
      .map(renameLastEventIf(ex4, isNotFollowedBy4)(_: UnifiedTrace))
      .map(renameLastEventIf(ex5, isNotFollowedBy5)(_: UnifiedTrace))
      .map(renameLastEventIf(ex6, isNotFollowedBy6)(_: UnifiedTrace))
      .map(renameLastEventIf(ex7, isNotFollowedBy7)(_: UnifiedTrace))
      .map(renameLastEventIf(ex8, isNotFollowedBy8)(_: UnifiedTrace))
      .map(renameLastEventIf(ex9, isNotFollowedBy9)(_: UnifiedTrace))
      .map(renameLastEventIf(ex10, isNotFollowedBy10)(_: UnifiedTrace))
      .map(renameLastEventIf(ex11, isNotFollowedBy11)(_: UnifiedTrace))
      .map(renameLastEventIf(ex12, isNotFollowedBy12)(_: UnifiedTrace))
      .map(renameLastEventIf(ex13, isNotFollowedBy13)(_: UnifiedTrace))
      .map(renameLastEventIf(ex14, isNotFollowedBy14)(_: UnifiedTrace))
      .map(renameLastEventIf(ex15, isNotFollowedBy15)(_: UnifiedTrace))
      .map(renameLastEventIf(ex16, isNotFollowedBy16)(_: UnifiedTrace))
      .map(renameLastEventIf(ex17, isNotFollowedBy17)(_: UnifiedTrace))

    exportExt(newLog, newLogPath, addStripped = false)
  }

  def step_90b = {
    val originalLogPath = FileNames.level_10_201567_activAll_casesAll_sealed
    val (log2015, log2016, log2017) = splitByYears2(originalLogPath, TraceAttributes)
    Statistics.caseAndAbsoluteFrequencyV2(log2015, EventAttributes.ConceptName, "C:\\bpic2018_work\\statistic\\conceptnameFrequency_2015_sealed.txt")
    Statistics.caseAndAbsoluteFrequencyV2(log2016, EventAttributes.ConceptName, "C:\\bpic2018_work\\statistic\\conceptnameFrequency_2016_sealed.txt")
    Statistics.caseAndAbsoluteFrequencyV2(log2017, EventAttributes.ConceptName, "C:\\bpic2018_work\\statistic\\conceptnameFrequency_2017_sealed.txt")
  }

  def step_90c = { // можно было обойтись фильтрацией, потом определить кейвааринты, и только тогда создавать лог из кейвариантов и кейактивити НО Диско плохо фильтрует по вариантам
    val newLogPath = FileNames.level_11_201567_activMilestone4_casesAll_sealed_stripped
    val originalLog = loadStrippedLog(FileNames.level_10_201567_activAll_casesAll_sealed)
    val newLog = originalLog.project(
      EventEx("Payment application-Application-mail income"),
      EventEx("Payment application-Application-mail valid"),
      EventEx("Parcel document-Main-initialize"),
      EventEx("Geo parcel document-Main-initialize"),
      EventEx("Control summary-Main-initialize"),
      EventEx("Reference alignment-Main-initialize"),
      EventEx("Department control parcels-Main-performed"),
      EventEx("Entitlement application-Main-initialize"),
      EventEx("Payment application-Application-initialize"),
      EventEx("Entitlement application-Main-sealed decide"),
      EventEx("Payment application-Application-sealed decide"),
      EventEx("Payment application-Application-sealed begin payment"),
      EventEx("Payment application-Application-finish payment")
    )
    exportExt(newLog, newLogPath, false)
  }

  def step_1002_create_log_L3_activ17_cases100 = { // можно было обойтись фильтрацией, потом определить кейвааринты, и только тогда создавать лог из кейвариантов и кейактивити НО Диско плохо фильтрует по вариантам
    val newLogPath = FileNames.log_L3_activ17_cases100_sealed // stripped?
  val originalLog = loadLog(FileNames.log_L2_activ187_cases100_sealed) // stripped?
  val newLog = originalLog.project(
    EventEx("Payment application-Application-mail income"),
    EventEx("Payment application-Application-mail valid"),
    EventEx("Parcel document-Main-initialize"),
    EventEx("Parcel document-Main-begin editing"), // aggregated
    EventEx("Geo parcel document-Main-initialize"),
    EventEx("Control summary-Main-initialize"),
    EventEx("Control summary-Main-begin editing"), // aggregated
    EventEx("Control summary-Main-finish editing"), // aggregated
    EventEx("Reference alignment-Main-initialize"),
    EventEx("Reference alignment-Main-performed"), // aggregated
    EventEx("Department control parcels-Main-performed"),
    EventEx("Entitlement application-Main-initialize"),
    EventEx("Payment application-Application-initialize"),
    EventEx("Entitlement application-Main-sealed decide"),
    EventEx("Payment application-Application-sealed decide"),
    EventEx("Payment application-Application-sealed begin payment"),
    EventEx("Payment application-Application-finish payment")
  )
    exportExt(newLog, newLogPath, false)
  }

  def step_1003_create_log_L4_activ13_cases100_sealed = { // можно было обойтись фильтрацией, потом определить кейвааринты, и только тогда создавать лог из кейвариантов и кейактивити НО Диско плохо фильтрует по вариантам
    val newLogPath = FileNames.log_L4_activ13_cases100_sealed // stripped
  val originalLog = loadLog(FileNames.log_L3_activ17_cases100_sealed) // stripped
  val newLog = originalLog.project(
    EventEx("Payment application-Application-mail income"),
    EventEx("Payment application-Application-mail valid"),
    EventEx("Parcel document-Main-initialize"),
    EventEx("Geo parcel document-Main-initialize"),
    EventEx("Control summary-Main-initialize"),
    EventEx("Reference alignment-Main-initialize"),
    EventEx("Department control parcels-Main-performed"),
    EventEx("Entitlement application-Main-initialize"),
    EventEx("Payment application-Application-initialize"),
    EventEx("Entitlement application-Main-sealed decide"),
    EventEx("Payment application-Application-sealed decide"),
    EventEx("Payment application-Application-sealed begin payment"),
    EventEx("Payment application-Application-finish payment")
  )
    exportExt(newLog, newLogPath, false)
  }

  def step_90d = {
    val newLogPath = FileNames.level_12_201567_activAll_casesMilestone4_sealed
    val originalLog = loadLog(FileNames.level_10_201567_activAll_casesAll_sealed)
    val simplifiedLog = loadStrippedLog(FileNames.level_11_201567_activMilestone4_casesAll_sealed_stripped)

    val income = EventEx("Payment application-Application-mail income")
    val valid = EventEx("Payment application-Application-mail valid")
    val parcel = EventEx("Parcel document-Main-initialize")
    val geoparcel = EventEx("Geo parcel document-Main-initialize")
    val control = EventEx("Control summary-Main-initialize")
    val reference = EventEx("Reference alignment-Main-initialize")
    val department = EventEx("Department control parcels-Main-performed")
    val eiInit = EventEx("Entitlement application-Main-initialize")
    val paInit = EventEx("Payment application-Application-initialize")
    val eaDecide = EventEx("Entitlement application-Main-sealed decide")
    val paDecide = EventEx("Payment application-Application-sealed decide")
    val beginPay = EventEx("Payment application-Application-sealed begin payment")
    val finishPay = EventEx("Payment application-Application-finish payment")

    val idsCasesKeyVariants2015 = simplifiedLog.filter(t matches (
      income.withValue(TraceAttributes.Year, "2015") >>
        valid >> parcel >> control >> reference >> department >> eiInit >> paInit >> eaDecide >> paDecide >> beginPay >> finishPay))

    val idsCasesKeyVariants2016 = simplifiedLog.filter(t matches (
      income.withValue(TraceAttributes.Year, "2016") >>
        valid >> geoparcel >> control >> reference >> department >> paInit >> paDecide >> beginPay >> finishPay))

    val idsCasesKeyVariants2017 = simplifiedLog.filter(t matches (
      income.withValue(TraceAttributes.Year, "2017") >>
        valid >> geoparcel >> control >> reference >> paInit >> paDecide >> beginPay >> finishPay))

    val logActivAllCasesMilestone2015 = originalLog.filterByIds(idsCasesKeyVariants2015, true)
    val logActivAllCasesMilestone2016 = originalLog.filterByIds(idsCasesKeyVariants2016, true)
    val logActivAllCasesMilestone2017 = originalLog.filterByIds(idsCasesKeyVariants2017, true)

    val logActivAllCasesMilestone201567 = UnifiedEventLog.fromTraces(logActivAllCasesMilestone2015.traces()
      union logActivAllCasesMilestone2016.traces()
      union logActivAllCasesMilestone2017.traces())

    exportExt(logActivAllCasesMilestone201567, newLogPath, false)
  }

  def step_90e = {
    val newLogPath = FileNames.level_13_201567_activMilestone4_casesMilestone4_sealed
    val originalLog = loadLog(FileNames.level_12_201567_activAll_casesMilestone4_sealed)
    val newLog = originalLog.project(
      EventEx("Payment application-Application-mail income"),
      EventEx("Payment application-Application-mail valid"),
      EventEx("Parcel document-Main-initialize"),
      EventEx("Geo parcel document-Main-initialize"),
      EventEx("Control summary-Main-initialize"),
      EventEx("Reference alignment-Main-initialize"),
      EventEx("Department control parcels-Main-performed"),
      EventEx("Entitlement application-Main-initialize"),
      EventEx("Payment application-Application-initialize"),
      EventEx("Entitlement application-Main-sealed decide"),
      EventEx("Payment application-Application-sealed decide"),
      EventEx("Payment application-Application-sealed begin payment"),
      EventEx("Payment application-Application-finish payment")
    )
    exportExt(newLog, newLogPath, false)
  }


  def step_100() = {
    val newFileNamePSM = FileNames.level_13_201567_activMilestone4_casesMilestone4_sealed_PSM
    val logPath = FileNames.level_13_201567_activMilestone4_casesMilestone4_sealed
    reportDurationsForDays(loadStrippedLog(logPath), newFileNamePSM, Duration.ofDays(1).toMillis, false)
  }

  def step_100a() = {
    val newFileNamePSM = FileNames.level_13_201567_activMilestone4_casesMilestone4_sealed_1w_PSM
    val logPath = FileNames.level_13_201567_activMilestone4_casesMilestone4_sealed
    reportDurationsForWeeks(loadStrippedLog(logPath), newFileNamePSM, Duration.ofDays(7).toMillis, false)
  }

  def step_101a() = {
    val newFileNamePSM = s"${FileNames.rootYear("allYears")}/psm/ConceptName/level_13_201567_activMilestone4_casesMilestone4_sealed_1d"
    val logPath = FileNames.level_13_201567_activMilestone4_casesMilestone4_sealed
    reportAttribute(loadStrippedLog(logPath), newFileNamePSM, Duration.ofDays(1).toMillis, EventAttributes.ConceptName, "")
  }

  def step_101b() = {
    val newFileNamePSM = s"${FileNames.rootYear("allYears")}/psm/ConceptName/level_13_201567_activMilestone4_casesMilestone4_sealed_1w"
    val logPath = FileNames.level_13_201567_activMilestone4_casesMilestone4_sealed
    reportAttribute(loadStrippedLog(logPath), newFileNamePSM, Duration.ofDays(7).toMillis, EventAttributes.ConceptName, "")
  }

  def step_103a() = {
    val newFileNamePSM = FileNames.level_12_201567_activAll_casesMilestone4_sealed_PSM
    val logPath = FileNames.level_12_201567_activAll_casesMilestone4_sealed
    reportDurationsForDays(loadStrippedLog(logPath), newFileNamePSM, Duration.ofDays(1).toMillis, false)
  }

  def step_103a2() = {
    val newFileNamePSM = FileNames.level_12_201567_activAll_casesMilestone4_sealed_PSM_1w
    val logPath = FileNames.level_12_201567_activAll_casesMilestone4_sealed
    reportDurationsForWeeks(loadStrippedLog(logPath), newFileNamePSM, Duration.ofDays(7).toMillis, false)
  }

  def step_103b() = {
    val newFileNamePSM = s"${FileNames.rootYear("allYears")}/psm/ConceptName/level_12_201567_activAll_casesMilestone4_sealed_1d"
    val logPath = FileNames.level_12_201567_activAll_casesMilestone4_sealed
    reportAttribute(loadStrippedLog(logPath), newFileNamePSM, Duration.ofDays(1).toMillis, EventAttributes.ConceptName, "")
  }

  def step_103c() = {
    val newFileNamePSM = s"${FileNames.rootYear("allYears")}/psm/ConceptName/level_12_201567_activAll_casesMilestone4_sealed_1w"
    val logPath = FileNames.level_12_201567_activAll_casesMilestone4_sealed
    reportAttribute(loadStrippedLog(logPath), newFileNamePSM, Duration.ofDays(7).toMillis, EventAttributes.ConceptName, "")
  }

  def step_104() = {
    val originalLogPath = FileNames.level_13_201567_activMilestone4_casesMilestone4_sealed
    val (log2015, log2016, log2017) = splitByYears2(originalLogPath, TraceAttributes)
    exportExt(log2015, FileNames.level_13_2015_activMilestone4_casesMilestone4_sealed, false)
    exportExt(log2016, FileNames.level_13_2016_activMilestone4_casesMilestone4_sealed, false)
    exportExt(log2017, FileNames.level_13_2017_activMilestone4_casesMilestone4_sealed, false)
  }

  def step_104b() = {
    val newFileNamePSM = FileNames.level_13_201567_activMilestone4_casesMilestone4_sealed_Shifted_1w_PSM
    shiftYears_LogWithDefaultClassifier_1week(
      loadStrippedLog(FileNames.level_13_2015_activMilestone4_casesMilestone4_sealed),
      loadStrippedLog(FileNames.level_13_2016_activMilestone4_casesMilestone4_sealed),
      loadStrippedLog(FileNames.level_13_2017_activMilestone4_casesMilestone4_sealed),
      newFileNamePSM)
  }

  def step_104b2() = {
    val newFileNamePSM = FileNames.level_13_201567_activMilestone4_casesMilestone4_sealed_Shifted_1d_PSM
    shiftYears_LogWithDefaultClassifier(
      loadStrippedLog(FileNames.level_13_2015_activMilestone4_casesMilestone4_sealed),
      loadStrippedLog(FileNames.level_13_2016_activMilestone4_casesMilestone4_sealed),
      loadStrippedLog(FileNames.level_13_2017_activMilestone4_casesMilestone4_sealed),
      newFileNamePSM)
  }

  def step_104c() = {
    val newFileNamePSM = FileNames.level_11_201567_activMilestone4_casesAll_sealed_Shifted_1w_PSM
    shiftYears_LogWithDefaultClassifier_1week(
      loadStrippedLog(FileNames.level_11_2015_activMilestone4_casesAll_sealed_stripped),
      loadStrippedLog(FileNames.level_11_2016_activMilestone4_casesAll_sealed_stripped),
      loadStrippedLog(FileNames.level_11_2017_activMilestone4_casesAll_sealed_stripped),
      newFileNamePSM)
  }

  def step_104d() = {
    val originalLogPath = FileNames.level_12_201567_activAll_casesMilestone4_sealed
    val (log2015, log2016, log2017) = splitByYears2(originalLogPath, TraceAttributes)
    exportExt(log2015, FileNames.level_12_2015_activAll_casesMilestone4_sealed, false)
    exportExt(log2016, FileNames.level_12_2016_activAll_casesMilestone4_sealed, false)
    exportExt(log2017, FileNames.level_12_2017_activAll_casesMilestone4_sealed, false)
  }

  def step_104e() = {
    val newFileNamePSM = FileNames.level_12_201567_activAll_casesMilestone4_sealed_Shifted_1w
    shiftYears_LogWithDefaultClassifier_1week(
      loadStrippedLog(FileNames.level_12_2015_activAll_casesMilestone4_sealed),
      loadStrippedLog(FileNames.level_12_2016_activAll_casesMilestone4_sealed),
      loadStrippedLog(FileNames.level_12_2017_activAll_casesMilestone4_sealed),
      newFileNamePSM)
  }

  def step_104f() = {
    val newFileNamePSM = FileNames.level_12_201567_activAll_casesMilestone4_sealed_Shifted_1d
    shiftYears_LogWithDefaultClassifier(
      loadStrippedLog(FileNames.level_12_2015_activAll_casesMilestone4_sealed),
      loadStrippedLog(FileNames.level_12_2016_activAll_casesMilestone4_sealed),
      loadStrippedLog(FileNames.level_12_2017_activAll_casesMilestone4_sealed),
      newFileNamePSM)
  }

  def step_104a() = {
    val originalLogPath = FileNames.level_11_201567_activMilestone4_casesAll_sealed_stripped
    val (log2015, log2016, log2017) = splitByYears2(originalLogPath, TraceAttributes)
    exportExt(log2015, FileNames.level_11_2015_activMilestone4_casesAll_sealed_stripped, false)
    exportExt(log2016, FileNames.level_11_2016_activMilestone4_casesAll_sealed_stripped, false)
    exportExt(log2017, FileNames.level_11_2017_activMilestone4_casesAll_sealed_stripped, false)
  }

  def step_105() = {
    val logNames = Seq(
      FileNames.level_13_2015_activMilestone4_casesMilestone4_sealed,
      FileNames.level_13_2016_activMilestone4_casesMilestone4_sealed,
      FileNames.level_13_2017_activMilestone4_casesMilestone4_sealed)
    val sortedOrderPath = FileNames.sorting_order_template_byOriginalActivities_v9
    val newFileName = "level_13_201567_activMilestone4_casesMilestone4_sealed_sorting9_5perc"
    exportEfrDfrFootprint(logNames, false, sortedOrderPath, newFileName)
    exportEfrDfrFootprint(logNames, true, sortedOrderPath, newFileName)
    exportEfrDfr(logNames, true, sortedOrderPath, newFileName)
    exportEfrDfr(logNames, false, sortedOrderPath, newFileName)
  }

  def step_105a() = {
    val originalLogPath = FileNames.level_10_201567_activAll_casesAll_sealed
    val (log2015, log2016, log2017) = splitByYears2(originalLogPath, TraceAttributes)
    exportExt(log2015, FileNames.level_10_2015_activAll_casesAll_sealed, false)
    exportExt(log2016, FileNames.level_10_2016_activAll_casesAll_sealed, false)
    exportExt(log2017, FileNames.level_10_2017_activAll_casesAll_sealed, false)
  }

  def step_105b() = {
    val logNames = Seq(
      FileNames.level_10_2015_activAll_casesAll_sealed,
      FileNames.level_10_2016_activAll_casesAll_sealed,
      FileNames.level_10_2017_activAll_casesAll_sealed)
    val sortedOrderPath = FileNames.sorting_order_template_byOriginalActivities_v9
    val newFileName = "level_10_201567_activAll_casesAll_sealed_sorting9_5perc"
    exportEfrDfrFootprint(logNames, false, sortedOrderPath, newFileName)
    exportEfrDfrFootprint(logNames, true, sortedOrderPath, newFileName)
    exportEfrDfr(logNames, true, sortedOrderPath, newFileName)
    exportEfrDfr(logNames, false, sortedOrderPath, newFileName)
  }

  def step_105c() = {
    val logNames = Seq(
      FileNames.level_10_2015_activAll_casesAll_sealed,
      FileNames.level_10_2016_activAll_casesAll_sealed,
      FileNames.level_10_2017_activAll_casesAll_sealed)
    val sortedOrderPath = FileNames.sorting_order_template_byOriginalActivities_v9
    val newFileName = "level_10_201567_activAll_casesAll_sealed_sorting13_5perc"
    exportEfrDfrFootprint(logNames, false, sortedOrderPath, newFileName)
    exportEfrDfrFootprint(logNames, true, sortedOrderPath, newFileName)
    exportEfrDfr(logNames, true, sortedOrderPath, newFileName)
    exportEfrDfr(logNames, false, sortedOrderPath, newFileName)
  }

  def step_105c2() = {
    val logNames = Seq(
      FileNames.level_10_2017_activAll_casesAll_sealed)
    val sortedOrderPath = FileNames.sorting_order_template_byOriginalActivities_v9
    val newFileName = "level_10_2017_activAll_casesAll_sealed_sorting13_5perc"
    exportEfrDfrFootprint(logNames, false, sortedOrderPath, newFileName)
    exportEfrDfrFootprint(logNames, true, sortedOrderPath, newFileName)
    exportEfrDfr(logNames, true, sortedOrderPath, newFileName)
    exportEfrDfr(logNames, false, sortedOrderPath, newFileName)
  }

  def step_106a() = {
    val exp = t subtrace EventEx("Payment application-Application-initialize") >->
      EventEx("Payment application-Application-sealed decide")
    val newLog = loadLog(FileNames.level_12_201567_activAll_casesMilestone4_sealed).map(exp)
    val newLogPath = FileNames.level_14_201567_activAll_casesMilestone4_sealed_PAinitPAsdecide
    exportExt(newLog, newLogPath, false)
  }

  def step_106b() = {
    val log = loadStrippedLog(FileNames.level_14_201567_activAll_casesMilestone4_sealed_PAinitPAsdecide).persist()
    reportDurationsForDays(log, FileNames.level_14_201567_activAll_casesMilestone4_sealed_PAinitPAsdecide_1d, Duration.ofDays(1).toMillis, false)
    reportDurationsForWeeks(log, FileNames.level_14_201567_activAll_casesMilestone4_sealed_PAinitPAsdecide_1w, Duration.ofDays(7).toMillis, false)
    reportAttribute(log, s"${FileNames.rootYear("allYears")}/psm/ConceptName/level_14_201567_activAll_casesMilestone4_sealed_PAinitPAsdecide_PSM_1d", Duration.ofDays(1).toMillis, EventAttributes.ConceptName, "")
    reportAttribute(log, s"${FileNames.rootYear("allYears")}/psm/ConceptName/level_14_201567_activAll_casesMilestone4_sealed_PAinitPAsdecide_PSM_1w", Duration.ofDays(7).toMillis, EventAttributes.ConceptName, "")
  }

  def step_107() = {
    val exp1 = t subtrace EventEx("Payment application-Application-mail valid") >->
      EventEx("Parcel document-Main-initialize")
    val exp2 = t subtrace EventEx("Payment application-Application-mail valid") >->
      EventEx("Geo parcel document-Main-initialize")
    val newLog1 = loadLog(FileNames.level_12_201567_activAll_casesMilestone4_sealed).map(exp1)
    val newLog2 = loadLog(FileNames.level_12_201567_activAll_casesMilestone4_sealed).map(exp2)
    val newLog = newLog1.fullOuterJoin(newLog2).persist()
    val newLogPath = FileNames.level_14_201567_activAll_casesMilestone4_sealed_mailValidGeoParcel
    exportExt(newLog, newLogPath, false)

    reportDurationsForDays(newLog, FileNames.level_14_201567_activAll_casesMilestone4_sealed_mailValidGeoParcel_1d, Duration.ofDays(1).toMillis, false)
    reportDurationsForWeeks(newLog, FileNames.level_14_201567_activAll_casesMilestone4_sealed_mailValidGeoParcel_1w, Duration.ofDays(7).toMillis, false)
    reportAttribute(newLog, s"${FileNames.rootYear("allYears")}/psm/ConceptName/level_14_201567_activAll_casesMilestone4_sealed_mailValidGeoParcel_1d", Duration.ofDays(1).toMillis, EventAttributes.ConceptName, "")
    reportAttribute(newLog, s"${FileNames.rootYear("allYears")}/psm/ConceptName/level_14_201567_activAll_casesMilestone4_sealed_mailValidGeoParcel_PSM_1w", Duration.ofDays(7).toMillis, EventAttributes.ConceptName, "")
    //    val (log2015, log2016, log2017) = splitByYears2(newLogMerged, TraceAttributes)
    //    val newFileNamePSM = FileNames.level_7_201567_keyVariants_validParcel_PSM_shiftedYears
    //    shiftYears_LogWithDefaultClassifier(log2015, log2016, log2017, newFileNamePSM)
    //    exportEfrDfr(Seq(newLogPath), true)
  }

  def step_108() = {
    val exp = t subtrace EventEx("Reference alignment-Main-initialize") >->
      EventEx("Payment application-Application-initialize")
    val newLog = loadLog(FileNames.level_12_201567_activAll_casesMilestone4_sealed).map(exp).persist()
    val newLogPath = FileNames.level_14_201567_activAll_casesMilestone4_sealed_RefinitPAinit
    exportExt(newLog, newLogPath, false)

    //    reportDurationsForDays(newLog, FileNames.level_14_201567_activAll_casesMilestone4_sealed_RefinitPAinit_1d, Duration.ofDays(1).toMillis, false)
    reportDurationsForWeeks(newLog, FileNames.level_14_201567_activAll_casesMilestone4_sealed_RefinitPAinit_1w, Duration.ofDays(7).toMillis, false)
    //    reportAttribute(newLog, s"${FileNames.rootYear("allYears")}/psm/ConceptName/level_14_201567_activAll_casesMilestone4_sealed_mailValidGeoParcel_1d", Duration.ofDays(1).toMillis, EventAttributes.ConceptName, "")
    reportAttribute(newLog, s"${FileNames.rootYear("allYears")}/psm/ConceptName/level_14_201567_activAll_casesMilestone4_sealed_RefinitPAinit_1w", Duration.ofDays(7).toMillis, EventAttributes.ConceptName, "")
    //    val (log2015, log2016, log2017) = splitByYears2(newLogMerged, TraceAttributes)
    //    val newFileNamePSM = FileNames.level_7_201567_keyVariants_validParcel_PSM_shiftedYears
    //    shiftYears_LogWithDefaultClassifier(log2015, log2016, log2017, newFileNamePSM)
    //    exportEfrDfr(Seq(newLogPath), true)
  }

  def step_109() = {
    val newLog = loadLogResourceDepartment(FileNames.level_12_201567_activAll_casesMilestone4_sealed)
    val newLogPath = FileNames.level_15_201567_activAll_casesMilestone4_sealed_RecDep
    exportExt(newLog, newLogPath, false)
  }

  def step_110() = {
    val log = loadLogResourceDepartmentStripped(FileNames.level_15_201567_activAll_casesMilestone4_sealed_RecDep).persist()
    val newLogPath = FileNames.level_15_201567_activAll_casesMilestone4_sealed_RecDep_1w
    reportDurationsForWeeks(log, newLogPath, Duration.ofDays(7).toMillis, false)
    reportAttribute(log,
      s"${FileNames.rootYear("allYears")}/psm/ConceptName/level_15_201567_activAll_casesMilestone4_sealed_RecDep_1w",
      Duration.ofDays(7).toMillis, EventAttributes.ConceptName, "")
  }

  def step_111() = {
    val log = loadLogDepartmentStripped(FileNames.level_15_201567_activAll_casesMilestone4_sealed_RecDep).persist()
    val newLogPath = FileNames.level_16_201567_activAll_casesMilestone4_sealed_Dep_1w
    reportDurationsForWeeks(log, newLogPath, Duration.ofDays(7).toMillis, false)
    reportAttribute(log,
      s"${FileNames.rootYear("allYears")}/psm/ConceptName/level_16_201567_activAll_casesMilestone4_sealed_Dep_1w",
      Duration.ofDays(7).toMillis, EventAttributes.ConceptName, "")
  }

  def step_112a() = {
    val newFileNamePSM = FileNames.level_13_201567_activMilestone4_casesMilestone4_sealed_firstActiv_PSM
    val logPath = FileNames.level_13_201567_activMilestone4_casesMilestone4_sealed
    reportDurations(loadStrippedLog(logPath),
      newFileNamePSM,
      Duration.ofDays(1).toMillis,
      DateRounding.truncatedTo(ZoneId.of(CsvExportHelper.AmsterdamTimeZone), ChronoUnit.DAYS, _),
      new Q4DurationClassifier, false, StartAggregation)
  }

  def step_112b() = {
    val newFileNamePSM = FileNames.level_13_201567_activMilestone4_casesMilestone4_sealed_firstActiv_1w_PSM
    val logPath = FileNames.level_13_201567_activMilestone4_casesMilestone4_sealed
    reportDurations(loadStrippedLog(logPath),
      newFileNamePSM,
      Duration.ofDays(7).toMillis,
      DateRounding.truncatedToWeeks(ZoneId.of(CsvExportHelper.AmsterdamTimeZone), _),
      new Q4DurationClassifier, false, StartAggregation)
  }

  def step_113a() = {
    val newFileNamePSM = FileNames.level_12_201567_activAll_casesMilestone4_sealed_firstActiv_PSM_1w
    val logPath = FileNames.level_12_201567_activAll_casesMilestone4_sealed
    reportDurations(loadStrippedLog(logPath),
      newFileNamePSM,
      Duration.ofDays(7).toMillis,
      DateRounding.truncatedToWeeks(ZoneId.of(CsvExportHelper.AmsterdamTimeZone), _),
      new Q4DurationClassifier, false, StartAggregation)
  }

  def step_114a() = {
    val newFileNamePSM = FileNames.level_13_201567_activMilestone4_casesMilestone4_sealed_firstActiv_Shifted_1d_PSM
    val shiftedLog2015 = shiftYears(loadStrippedLog(FileNames.level_13_2015_activMilestone4_casesMilestone4_sealed), "2015", 0)
    val shiftedLog2016 = shiftYears(loadStrippedLog(FileNames.level_13_2016_activMilestone4_casesMilestone4_sealed), "2016", 1)
    val shiftedLog2017 = shiftYears(loadStrippedLog(FileNames.level_13_2017_activMilestone4_casesMilestone4_sealed), "2017", 2)
    val logMerged = shiftedLog2015.fullOuterJoin(shiftedLog2016).fullOuterJoin(shiftedLog2017)
    reportDurations(logMerged,
      newFileNamePSM,
      Duration.ofDays(1).toMillis,
      DateRounding.truncatedTo(ZoneId.of(CsvExportHelper.AmsterdamTimeZone), ChronoUnit.DAYS, _),
      new Q4DurationClassifier, false, StartAggregation)
  }

  def step_115a() = {
    val newLog = loadLogActivDepartment(FileNames.level_12_201567_activAll_casesMilestone4_sealed)
    val newLogPath = FileNames.level_17_201567_activAll_casesMilestone4_sealed_ActDep
    exportExt(newLog, newLogPath, false)
  }

  def step_115b() = {
    val log = loadLogActivDepartmentStripped(FileNames.level_17_201567_activAll_casesMilestone4_sealed_ActDep).persist()
    val newFileNamePSM = FileNames.level_17_201567_activAll_casesMilestone4_sealed_ActDep_firstActiv_1w_PSM
    reportDurations(log,
      newFileNamePSM,
      Duration.ofDays(7).toMillis,
      DateRounding.truncatedToWeeks(ZoneId.of(CsvExportHelper.AmsterdamTimeZone), _),
      new Q4DurationClassifier, false, StartAggregation)
  }

  def createLogForTesting(originalLogPath: String, newLogPath: String, fraction: Double): Unit = {
    val log = loadLog(originalLogPath).sample(false, fraction, 1L)
    exportExt(log, newLogPath, false)
  }

  def createLogWithDoctypeSubprocessClassifier(originalLogPath: String, newLogPath: String, setOfAttributesTrace: Set[String], setOfAttributesEvent: Set[String]): Unit = {
    val log = XesReader.xesToUnifiedEventLog(
      XesReader.readXes(new File(originalLogPath)).head,
      Some(setOfAttributesTrace),
      Some(setOfAttributesEvent),
      XesReader.DefaultActivitySep, DoctypeSubprocessClassifier: _*)
    exportExt(log, newLogPath, addStripped = true)
  }

  def createLogWithDefaultYearClassifier(originalLogPath: String, newLogPath: String, setOfAttributesTrace: Set[String], setOfAttributesEvent: Set[String]): Unit = {
    val log = XesReader.xesToUnifiedEventLog(
      XesReader.readXes(new File(originalLogPath)).head,
      Some(setOfAttributesTrace),
      Some(setOfAttributesEvent),
      XesReader.DefaultActivitySep, DefaultYearClassifier: _*)
    exportExt(log, newLogPath, addStripped = true)
  }

  def createLogWithResourceDepartmentClassifier(originalLogPath: String, newLogPath: String): Unit = {
    val log = loadLogResourceDepartment(originalLogPath)
    exportExt(log, newLogPath, false)
  }

  /**
    * Creates log1_201[5-7].gz    *
    */
  def splitByYears(): Unit = {
    def extractYear(year: String, originalLog: org.processmining.scala.log.common.unified.log.parallel.UnifiedEventLog): Unit = {
      val exTimePeriodFilter = t contains EventEx(".+").withValue(TraceAttributes.Year, year)
      val log =
        originalLog
          .filter(exTimePeriodFilter)
      exportExt(log, FileNames.completeLogForYear(year), addStripped = true)
    }

    val originalLog = XesReader.xesToUnifiedEventLog(
      XesReader.readXes(new File(FileNames.OriginalLog)).head,
      Some(TraceAttributes.SetOfMainAttributes),
      Some(EventAttributes.SetOfMainAttributes),
      XesReader.DefaultActivitySep, DefaultClassifier: _*)

    extractYear("2015", originalLog)
    extractYear("2016", originalLog)
    extractYear("2017", originalLog)
  }

  def splitByYears2(originalLogPath: String, traceAttributes: BaseTraceAttributes): (UnifiedEventLog, UnifiedEventLog, UnifiedEventLog) = {
    def extractYear(year: String, originalLog: UnifiedEventLog): UnifiedEventLog = {
      val exTimePeriodFilter = t contains EventEx(".+").withValue(traceAttributes.Year, year)
      originalLog.filter(exTimePeriodFilter)
    }

    val originalLog = loadLog(originalLogPath)
    (extractYear("2015", originalLog),
      extractYear("2016", originalLog),
      extractYear("2017", originalLog))
  }

  def splitByYears2(originalLog: UnifiedEventLog, traceAttributes: BaseTraceAttributes): (UnifiedEventLog, UnifiedEventLog, UnifiedEventLog) = {
    def extractYear(year: String, originalLog: UnifiedEventLog): UnifiedEventLog = {
      val exTimePeriodFilter = t contains EventEx(".+").withValue(traceAttributes.Year, year)
      originalLog.filter(exTimePeriodFilter)
    }

    (extractYear("2015", originalLog),
      extractYear("2016", originalLog),
      extractYear("2017", originalLog))
  }

  def allYearsToEFRPsm(): Unit = {
    reportDurationsForDays(log1_2015Stripped, FileNames.completePsmDurationForYear("2015"), Duration.ofDays(7).toMillis, true)
    //    reportDurationsForDays(log1_2016, FileNames.completePsmDurationForYear("2016"), Duration.ofDays(1).toMillis, true)
    //    reportDurationsForDays(log1_2017, FileNames.completePsmDurationForYear("2017"), Duration.ofDays(1).toMillis, true)
  }

  def q1IdsOfUndesired() {
    def q1IdsOfUndesired(year: String, outcomeNumber: Int): Unit = {
      val ids = loadLog(FileNames.q1UndesiredOutcom(year, outcomeNumber))
        .traces()
        .map(_._1.id)
        .collect()
        .mkString(";")
      new PrintWriter(FileNames.q1UndesiredOutcomIds(year, outcomeNumber)) {
        write(ids);
        close
      }
    }

    (0 to 2).foreach(x => {
      q1IdsOfUndesired("2015", x)
      q1IdsOfUndesired("2016", x)
      q1IdsOfUndesired("2017", x)
    })
  }


  def extractQ1DesiredAndUndesiredOutcome(): Unit = {

    def extractQ1DesiredAndUndesiredOutcome(originalLog: UnifiedEventLog, year: Int): Unit = {

      originalLog.persist()

      val exSubprocessChange = (t contains EventEx(".+").withValue(EventAttributes.Subprocess, "Change")) or (t contains EventEx(".+").withValue(EventAttributes.Subprocess, "Objection"))
      val logQ1UndesiredOutcome2 = originalLog
        .filter(exSubprocessChange)
        .persist()
      exportExt(logQ1UndesiredOutcome2, FileNames.q1UndesiredOutcom(year.toString, 2), addStripped = true)


      val exBeginPayment = EventEx(".+")
        .withValue(EventAttributes.Doctype, "Payment application")
        .withValue(EventAttributes.Subprocess, "Application")
        .withValue(EventAttributes.Activity, "begin payment")

      val exBeginPaymentWithinTheNextYear = exBeginPayment
        .during(dateHelper.extractTimestamp(s"01-01-${
          (year + 1).toString
        } 00:00:00"), dateHelper.extractTimestamp(s"01-01-${
          (year + 5).toString
        } 00:00:00"))

      val exAbort = EventEx(".+")
        .withValue(EventAttributes.Doctype, "Payment application")
        .withValue(EventAttributes.Subprocess, "Application")
        .withValue(EventAttributes.Activity, "abort payment") //TODO: abort external ????

      val logQ1UndesiredOutcome1Modified =
        originalLog
          .project(EventEx(".+").withValue(EventAttributes.Success, "true"))
          .map(t keepLast(exBeginPayment, exBeginPayment, "remCnt"))
          .filter(t contains exBeginPaymentWithinTheNextYear or (t contains (exBeginPayment >-> exAbort)) or (t contains exBeginPayment).unary_!())
          .persist()

      val logQ1DesiredOutcome =
        originalLog
          .filterByIds(logQ1UndesiredOutcome1Modified, keepContained = false)
          .filterByIds(logQ1UndesiredOutcome2, keepContained = false)

      exportExt(logQ1DesiredOutcome, FileNames.q1DesiredOutcom(year.toString), addStripped = false)

      val logQ1UndesiredOutcome1 = originalLog
        .filterByIds(logQ1UndesiredOutcome1Modified, keepContained = true)
        .persist()

      logQ1UndesiredOutcome1Modified.unpersist()
      originalLog.unpersist()

      exportExt(logQ1UndesiredOutcome1, FileNames.q1UndesiredOutcom(year.toString, 1), addStripped = false)


      val logQ1UndesiredOutcome = logQ1UndesiredOutcome1 fullOuterJoin logQ1UndesiredOutcome2
      exportExt(logQ1UndesiredOutcome, FileNames.q1UndesiredOutcom(year.toString, 0), addStripped = false)
    }

    extractQ1DesiredAndUndesiredOutcome(log1_2015, 2015)
    extractQ1DesiredAndUndesiredOutcome(log1_2016, 2016)
    extractQ1DesiredAndUndesiredOutcome(log1_2017, 2017)
  }

  def shiftYears(log: UnifiedEventLog, year: String, numSiftedYears: Int): UnifiedEventLog = {
    UnifiedEventLog.create(log
      .events()
      .map { e =>
        val newTs = DateRounding
          .toZonedDateTime(ZoneId.of(CsvExportHelper.AmsterdamTimeZone), e._2.timestamp)
          .minusYears(numSiftedYears)
        val newEvent = e._2
          .copy(
            newTs
              .toInstant
              .toEpochMilli)
          .copy(s"$year-${e._2.activity}")
        (e._1.id, newEvent)
      })
  }

  def shiftYears_LogWithDefaultClassifier(log2015: String, log2016: String, log2017: String, newFileNamePSM: String): Unit = {
    val shiftedLog2015 = shiftYears(loadStrippedLog(log2015), "2015", 0)
    val shiftedLog2016 = shiftYears(loadStrippedLog(log2016), "2016", 1)
    val shiftedLog2017 = shiftYears(loadStrippedLog(log2017), "2017", 2)
    val logMerged = shiftedLog2015.fullOuterJoin(shiftedLog2016).fullOuterJoin(shiftedLog2017)
    reportDurationsForDays(logMerged, newFileNamePSM,
      Duration.ofDays(1).toMillis, false)
  }

  def shiftYears_LogWithDefaultClassifier_reportAttribute(log2015: String, log2016: String, log2017: String, newFileNamePSM: String): Unit = {
    val shiftedLog2015 = shiftYears(loadStrippedLog(log2015), "2015", 0)
    val shiftedLog2016 = shiftYears(loadStrippedLog(log2016), "2016", 1)
    val shiftedLog2017 = shiftYears(loadStrippedLog(log2017), "2017", 2)
    val logMerged = shiftedLog2015.fullOuterJoin(shiftedLog2016).fullOuterJoin(shiftedLog2017)
    reportAttribute(
      logMerged,
      newFileNamePSM,
      Duration.ofDays(1).toMillis, EventAttributes.ConceptName, "")
  }

  def shiftYears_LogWithDefaultClassifier(log2015: UnifiedEventLog, log2016: UnifiedEventLog, log2017: UnifiedEventLog, newFileNamePSM: String): Unit = {
    val shiftedLog2015 = shiftYears(log2015, "2015", 0)
    val shiftedLog2016 = shiftYears(log2016, "2016", 1)
    val shiftedLog2017 = shiftYears(log2017, "2017", 2)
    val logMerged = shiftedLog2015.fullOuterJoin(shiftedLog2016).fullOuterJoin(shiftedLog2017)
    reportDurationsForDays(logMerged, newFileNamePSM,
      Duration.ofDays(1).toMillis, false)
  }

  def shiftYears_LogWithDefaultClassifier_1week(log2015: UnifiedEventLog, log2016: UnifiedEventLog, log2017: UnifiedEventLog, newFileNamePSM: String): Unit = {
    val shiftedLog2015 = shiftYears(log2015, "2015", 0)
    val shiftedLog2016 = shiftYears(log2016, "2016", 1)
    val shiftedLog2017 = shiftYears(log2017, "2017", 2)
    val logMerged = shiftedLog2015.fullOuterJoin(shiftedLog2016).fullOuterJoin(shiftedLog2017)
    reportDurationsForWeeks(logMerged, newFileNamePSM,
      Duration.ofDays(7).toMillis, false)
  }

  def shiftYears_LogWithResourceDepartmentClassifier(log2015: String, log2016: String, log2017: String, newFileNamePSM: String): Unit = {
    val shiftedLog2015 = shiftYears(loadLogResourceDepartment(log2015), "2015", 0) //TODO loadLogResourceDepartmentStripped
    val shiftedLog2016 = shiftYears(loadLogResourceDepartment(log2016), "2016", 1)
    val shiftedLog2017 = shiftYears(loadLogResourceDepartment(log2017), "2017", 2)
    val logMerged = shiftedLog2015.fullOuterJoin(shiftedLog2016).fullOuterJoin(shiftedLog2017)
    reportDurationsForDays(logMerged, newFileNamePSM,
      Duration.ofDays(1).toMillis, false)
  }


  def getGroupedActivities(log: UnifiedEventLog, deltaMs: Long, sortActivity: Boolean): Array[(String, Int)] =
    log.aggregate(EventRelations.mergeSimilarActivities(
      (e1, e2) => Math.abs(e1.timestamp - e2.timestamp) < deltaMs && e1.attributes.get(EventAttributes.Resource) == e2.attributes.get(EventAttributes.Resource),
      sortActivity,
      _: List[UnifiedEvent]))
      .sortBy(_._2)
      .reverse

  def writeGroupedActivities(filename: String, data: Array[(String, Int)]) = {
    val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(filename)))
    writer.write(s""""activity",s"count"\n""")
    data.foreach(x => writer.write(s""""${x._1}",${x._2}\n"""))
    writer.close()
  }

  def writeGroupedActivities2(filename: String, data: Array[(String, String, Int)]) = {
    val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(filename)))
    writer.write(s""""activity",s"count"\n""")
    data.foreach(x => writer.write(s""""${x._1}","${x._2}",${x._3}\n"""))
    writer.close()
  }


  def exportEfrDfr(logNames: Seq[String], isEfr: Boolean, sortedOrderPath: String, newFileName: String) = {
    val sortedOrder = Source.fromFile(sortedOrderPath).getLines.toList
    val relImg = if (isEfr) FileNames.efr_img else FileNames.dfr_img
    val absImg = s"${if (isEfr) FileNames.rootFollowings else FileNames.rootFollowings}/${newFileName}/$relImg"
    new File(absImg).mkdirs()

    val dsMatrices = logNames.map(loadLog(_)
      .combineAndReduce[Long, DescriptiveStatistics, (DescriptiveStatisticsEntry, String)](
      if (isEfr) EventRelations.efr[Long]((_, _) => false, _: List[UnifiedEvent], -_.timestamp + _.timestamp) else EventRelations.dfr[Long]((_, _) => false, _: List[UnifiedEvent], -_.timestamp + _.timestamp),
      EventRelationsStatistics.createCombiner,
      EventRelationsStatistics.scoreCombiner,
      EventRelationsStatistics.scoreMerger,
      EventRelationsStatistics.reducerWithDistr(50,
        (k1, k2) => (s"$absImg/$k1$k2.png", s"$relImg/$k1$k2.png"),
        1.0 / (24 * 60 * 60 * 1000.0),
        if (isEfr) Color.RED else Color.BLUE,
        100,
        70
      )))
      .map(MatrixOfRelations(_,
        MatrixOfRelations.sortByOrderedList(sortedOrder, _: Set[String]),
        (EmptyDescriptiveStatisticsEntry, "")))

    MatrixOfRelationsExporter.exportDs(dsMatrices,
      //      if (isEfr) FileNames.efr_ds_html else FileNames.dfr_ds_html, sortedOrder) // OLD
      if (isEfr) s"${FileNames.rootFollowings}/${newFileName}/efr_${newFileName}_ds_distr.html" else s"${FileNames.rootFollowings}/${newFileName}/dfr_${newFileName}_ds_distr.html", sortedOrder)

    val dseMatrices = dsMatrices.map(p => (p._1.map(_.map(x => (x._1._1, x._2._1, x._3))), p._2))
    MatrixOfRelationsExporter.exportDse(dseMatrices,
      if (isEfr) FileNames.efr_dse_html else FileNames.dfr_dse_html, sortedOrder)
    //      if (isEfr) s"${FileNames.rootFollowings}/${newFileName}/efr_${newFileName}" ++ FileNames.efr_dse_html else s"${FileNames.dfr_dse_html}_${newFileName}_dse.html", sortedOrder) // TODO
  }


  def exportEfrDfrFootprint(logNames: Seq[String], isDfr: Boolean, sortedOrderPath: String, newFileName: String) = {
    val sortedOrder = Source.fromFile(sortedOrderPath).getLines.toList
    val f = if (!isDfr) EventRelations.efr((_, _) => false, _: List[UnifiedEvent], EventRelations.count) else EventRelations.dfr((_, _) => false, _: List[UnifiedEvent], EventRelations.count)

    val all = logNames.map(fn => MatrixOfRelations(
      loadLog(fn)
        .combineAndReduce[Int, Int, Int](
        f, x => x, _ + _, _ + _, (_, x) => x),
      MatrixOfRelations.sortByOrderedList(sortedOrder, _: Set[String]),
      0))


    MatrixOfRelationsExporter.csvExport2[Int](
      all,
      //      if (isDfr) FileNames.dfr_201567_mainActiv2_sorting8_5perc else FileNames.efr_201567_mainActiv2_sorting8_5perc,
      if (isDfr) s"${FileNames.rootFollowings}/${newFileName}/dfr_${newFileName}.csv"
      else s"${FileNames.rootFollowings}/${newFileName}/efr_${newFileName}.csv",
      MatrixOfRelationsExporter.format(5.0),
      MatrixOfRelations.sortByOrderedList(sortedOrder, _: Set[String]),
      if (isDfr) "DFR" else "EFR")


    //    MatrixOfRelationsExporter.htmlExportDescriptiveStatistics(all,
    //      if(isDfr) FileNames.dfr_html else FileNames.efr_html,
    //      if(isDfr) "DFR" else "EFR",
    //      MatrixOfRelations.sortByOrderedList(sortedActivities8, _: Set[String]),
    //
    //
    //
    //
    //    )
  }


  def main(args: Array[String]): Unit = {
    logger.info("EvaluationEnvironment started")

    //    step_1a() // create log for testing 1%
    //    step_1b() // create log for testing 0.1%
    //
    //    step_2_createCompleteLogWithOnlyMainAttributesAndStripped // create log with only main attributes and stripped
    //    step_3_splitCompleteLogByYears()
    //    step_4a_completeLogToPsm_1day()
    //    step_4b_completeLogToPsm_1week()
    //    step_4c_completeLogToPsm_1week()
    //    step_5_allYearsToPsm()
    //
    //    extractQ1DesiredAndUndesiredOutcome()
    //    q1IdsOfUndesired()

    //    //    create log with only Doctype+Subprocess in events names
    //    createLogWithDoctypeSubprocessClassifier(FileNames.OriginalLogOnlyMainAttrs, FileNames.OriginalLogDoctypeSubprocess, TraceAttributes.SetOfMainAttributes, EventAttributes.SetOfMainAttributes)
    //    reportDurationsForDays(log_level_0_allYears_onlyMinAttrs_DoctypeSubprocess, FileNames.OriginalLogOnlyMinAttrsPSM, Duration.ofDays(1).toMillis)

    // Step 8 - shift years
    //    val shiftedLog2015 = shiftYears(log1_2015, "2015", 0)
    //    val shiftedLog2016 = shiftYears(log1_2016, "2016", 1)
    //    val shiftedLog2017 = shiftYears(log1_2017, "2017", 2)
    //    val logMerged = shiftedLog2015.fullOuterJoin(shiftedLog2016).fullOuterJoin(shiftedLog2017)
    //    exportExt(logMerged, FileNames.OriginalLogShiftedYears, true)
    //    reportDurationsForDays(logShiftedStripped, FileNames.OriginalLogShiftedYearsPSM, Duration.ofDays(1).toMillis)

    //writeGroupedActivities(FileNames.MergedActivities2015, getGroupedActivities(log1_2015, 1000L))
    //    writeGroupedActivities(FileNames.MergedActivities, getGroupedActivities(logCompleteActivitiesStripped, 1000L))
    //writeGroupedActivities("C:/bpic2018_work/merged_activities1000ms_res_not_sorted.csv", getGroupedActivities(logOnlyMainAttrs, 1000, false))
    //    writeGroupedActivities("C:/bpic2018_work/merged_activities300ms.csv", getGroupedActivities(logCompleteActivitiesStripped, 300))
    //    writeGroupedActivities("C:/bpic2018_work/merged_activities100ms.csv", getGroupedActivities(logCompleteActivitiesStripped, 100))
    //    writeGroupedActivities("C:/bpic2018_work/merged_activities1000ms_res_not_sorted_2015.csv", getGroupedActivities(log1_2015, 1000, false))

    //    val efrMatrix = MatrixOfRelations.apply(logCompleteActivitiesStripped.aggregate(EventRelations.efr((_, _) => false, _: List[UnifiedEvent])))
    //    MatrixOfRelationsExporter.csvExport(efrMatrix, "C:\\bpic2018_work\\followings\\EFR_matrix.csv")
    //
    //    val dfrMatrix = MatrixOfRelations.apply(logCompleteActivitiesStripped.aggregate(EventRelations.dfr((_, _) => false, _: List[UnifiedEvent])))
    //    MatrixOfRelationsExporter.csvExport(dfrMatrix, "C:\\bpic2018_work\\followings\\DFR_matrix.csv")

    //    allYearsToEFRPsm()

    //    val result = getGroupedActivities(logOnlyMainAttrs, 1000, false)
    //    val result2 = result.map{x => (x._1.split(":").toSet.mkString(":"), x._1, x._2)}
    //    writeGroupedActivities2("C:/bpic2018_work/merged_activities1000ms_res_not_sorted2.csv", result2)

    //    val sortedActivities2 = Source.fromFile("C:\\bpic2018_work\\sorting_order\\sorting_order_byOriginalActivities_v2_template.txt").getLines.toList
    //    val efrMatrix2015 = MatrixOfRelations.apply(log1_2015Stripped.aggregate(EventRelations.efr((_, _) => false, _: List[UnifiedEvent])), sortedActivities2)
    //    MatrixOfRelationsExporter.csvExport(efrMatrix2015, "C:\\bpic2018_work\\followings\\efr_2015_sorting2.csv")

    //    val sortedActivities2 = Source.fromFile("C:\\bpic2018_work\\sorting_order\\sorting_order_byOriginalActivities_v2_template.txt").getLines.toList
    //    val efrMatrix = MatrixOfRelations.apply(logCompleteActivitiesStripped.aggregate(EventRelations.efr((_, _) => false, _: List[UnifiedEvent])), sortedActivities2)
    //    MatrixOfRelationsExporter.csvExport(efrMatrix, "C:\\bpic2018_work\\followings\\efr_sorting2.csv")

    //        val sortedActivities2 = Source.fromFile("C:\\bpic2018_work\\sorting_order\\sorting_order_byOriginalActivities_v2_template.txt").getLines.toList
    //    val efrMatrix2015mainActiv1 = MatrixOfRelations.apply(loadLog("C:\\bpic2018_work\\2015\\level_3_2015_mainActiv1.xes").aggregate(EventRelations.efr((_, _) => false, _: List[UnifiedEvent])), sortedActivities2)
    //    MatrixOfRelationsExporter.csvExport(efrMatrix2015mainActiv1, "C:\\bpic2018_work\\followings\\efr_2015_mainActiv1_sorting2.csv")

    //    val sortedActivities2 = Source.fromFile("C:\\bpic2018_work\\sorting_order\\sorting_order_byOriginalActivities_v3_template_2015.txt").getLines.toList
    //    val efrMatrix2015mainActiv1 = MatrixOfRelations.apply(loadLog("C:\\bpic2018_work\\2015\\level_3_2015_mainActiv1.xes").aggregate(EventRelations.efr((_, _) => false, _: List[UnifiedEvent])), sortedActivities2)
    //    MatrixOfRelationsExporter.csvExport(efrMatrix2015mainActiv1, "C:\\bpic2018_work\\followings\\efr_2015_mainActiv1_sorting3.csv")

    //    val sortedActivities2 = Source.fromFile("C:\\bpic2018_work\\sorting_order\\sorting_order_byOriginalActivities_v5_template_2015.txt").getLines.toList
    //    val efrMatrix2015mainActiv1 = MatrixOfRelations.apply(loadLog("C:\\bpic2018_work\\2015\\level_3_2015_mainActiv1.xes").aggregate(EventRelations.efr((_, _) => false, _: List[UnifiedEvent])), sortedActivities2)
    //    MatrixOfRelationsExporter.csvExport(efrMatrix2015mainActiv1, "C:\\bpic2018_work\\followings\\efr_2015_mainActiv1_sorting5.csv")

    //    val sortedActivities2 = Source.fromFile("C:\\bpic2018_work\\sorting_order\\sorting_order_byOriginalActivities_v5_template_2015.txt").getLines.toList
    //    val efrMatrix2015mainActiv1 = MatrixOfRelations(
    //      loadLog("C:\\bpic2018_work\\2015\\level_3_2015_mainActiv1.xes").aggregate(EventRelations.efr((_, _) => false, _: List[UnifiedEvent])),
    //      MatrixOfRelations.sortByOrderedList(sortedActivities2, _: Set[String]))
    //    MatrixOfRelationsExporter.csvExport(efrMatrix2015mainActiv1, "C:\\bpic2018_work\\followings\\efr_2015_mainActiv1_sorting5_5perc.csv", 5.0, "EFR")

    //    val sortedActivities2 = Source.fromFile("C:\\bpic2018_work\\sorting_order\\sorting_order_byOriginalActivities_v6_template_201567.txt").getLines.toList
    //    val efrMatrix2015 = MatrixOfRelations(
    //      loadLog("C:\\bpic2018_work\\2015\\level_3_2015_mainActiv1.xes").aggregate(EventRelations.efr((_, _) => false, _: List[UnifiedEvent])),
    //      MatrixOfRelations.sortByOrderedList(sortedActivities2, _: Set[String]))
    //    val efrMatrix2016 = MatrixOfRelations(
    //      loadLog("C:\\bpic2018_work\\2016\\level_3_2016_mainActiv1.xes").aggregate(EventRelations.efr((_, _) => false, _: List[UnifiedEvent])),
    //      MatrixOfRelations.sortByOrderedList(sortedActivities2, _: Set[String]))
    //    val efrMatrix2017 = MatrixOfRelations(
    //      loadLog("C:\\bpic2018_work\\2017\\level_3_2017_mainActiv1.xes").aggregate(EventRelations.efr((_, _) => false, _: List[UnifiedEvent])),
    //      MatrixOfRelations.sortByOrderedList(sortedActivities2, _: Set[String]))
    //    val efrMatrix201567 = Seq (efrMatrix2015, efrMatrix2016, efrMatrix2017)
    //        MatrixOfRelationsExporter.csvExport(efrMatrix201567,
    //      "C:\\bpic2018_work\\followings\\efr_201567_mainActiv1_sorting6_5perc.csv",
    //      5.0,
    //      MatrixOfRelations.sortByOrderedList(sortedActivities2, _: Set[String]),
    //      "EFR")

    //    val sortedActivities2 = Source.fromFile("C:\\bpic2018_work\\sorting_order\\sorting_order_byOriginalActivities_v6_template_201567.txt").getLines.toList
    //    val dfrMatrix2015 = MatrixOfRelations(
    //      loadLog("C:\\bpic2018_work\\2015\\level_3_2015_mainActiv1.xes").aggregate(EventRelations.dfr((_, _) => false, _: List[UnifiedEvent])),
    //      MatrixOfRelations.sortByOrderedList(sortedActivities2, _: Set[String]))
    //    val dfrMatrix2016 = MatrixOfRelations(
    //      loadLog("C:\\bpic2018_work\\2016\\level_3_2016_mainActiv1.xes").aggregate(EventRelations.dfr((_, _) => false, _: List[UnifiedEvent])),
    //      MatrixOfRelations.sortByOrderedList(sortedActivities2, _: Set[String]))
    //    val dfrMatrix2017 = MatrixOfRelations(
    //      loadLog("C:\\bpic2018_work\\2017\\level_3_2017_mainActiv1.xes").aggregate(EventRelations.dfr((_, _) => false, _: List[UnifiedEvent])),
    //      MatrixOfRelations.sortByOrderedList(sortedActivities2, _: Set[String]))
    //    val dfrMatrix201567 = Seq (dfrMatrix2015, dfrMatrix2016, dfrMatrix2017)
    //    MatrixOfRelationsExporter.csvExport(dfrMatrix201567,
    //      "C:\\bpic2018_work\\followings\\dfr_201567_mainActiv1_sorting6_5perc.csv",
    //      5.0,
    //      MatrixOfRelations.sortByOrderedList(sortedActivities2, _: Set[String]),
    //      "DFR")

    //// shift years for mainActiv1
    //        val shiftedLog2015 = shiftYears(loadLog("C:\\bpic2018_work\\2015\\level_3_2015_mainActiv1.xes"), "2015", 0)
    //        val shiftedLog2016 = shiftYears(loadLog("C:\\bpic2018_work\\2016\\level_3_2016_mainActiv1.xes"), "2016", 1)
    //        val shiftedLog2017 = shiftYears(loadLog("C:\\bpic2018_work\\2017\\level_3_2017_mainActiv1.xes"), "2017", 2)
    //        val logMerged = shiftedLog2015.fullOuterJoin(shiftedLog2016).fullOuterJoin(shiftedLog2017)
    //        exportExt(logMerged, FileNames.OriginalLogShiftedYearsMainActiv1, true)
    //    reportDurationsForDays(loadShiftedStrippedLog("C:\\bpic2018_work\\allYears\\level_3_allYears_ShiftedYears_mainActiv1.xesstripped.xes"),
    //      "C:\\bpic2018_work\\psm\\level_3_allYears_ShiftedYears_mainActiv1.xes",
    //      Duration.ofDays(1).toMillis,
    //      false)

    ////    Payment application
    // не хватило памяти
    //        val expPA = t contains EventEx(".+").withValue(EventAttributes.Doctype, "Payment application")
    //        val logPA201567 = logOnlyMainAttrs.filter(expPA)
    //        exportExt(logPA201567, "C:\\bpic2018_work\\Payment application\\PA_201567_XXX.xes", true)
    //    splitByYears("C:\\bpic2018_work\\Payment application\\PA_201567.xes", "Payment application", "PA", DiscoTraceAttributes)
    //    // shift years for PA_201567.xes
    //        val shiftedLog2015 = shiftYears(loadLog("C:\\bpic2018_work\\Payment application\\PA_2015.xes"), "2015", 0)
    //        val shiftedLog2016 = shiftYears(loadLog("C:\\bpic2018_work\\Payment application\\PA_2016.xes"), "2016", 1)
    //        val shiftedLog2017 = shiftYears(loadLog("C:\\bpic2018_work\\Payment application\\PA_2017.xes"), "2017", 2)
    //        val logMerged = shiftedLog2015.fullOuterJoin(shiftedLog2016).fullOuterJoin(shiftedLog2017)
    //        exportExt(logMerged, "C:\\bpic2018_work\\Payment application\\PA_201567_ShiftedYears.xes", true)
    //        reportDurationsForDays(loadStrippedLog("C:\\bpic2018_work\\Payment application\\PA_201567_ShiftedYears.xesstripped.xes"),
    //          "C:\\bpic2018_work\\Payment application\\psm\\PA_201567_ShiftedYears.xes",
    //          Duration.ofDays(1).toMillis,
    //          false)
    //
    ////        val sortedActivities2 = Source.fromFile("C:\\bpic2018_work\\sorting_order\\sorting_order_byOriginalActivities_v5_template_2015.txt").getLines.toList
    //        val efrMatrixPA201567 = MatrixOfRelations(
    //          loadStrippedLog("C:\\bpic2018_work\\Payment application\\PA_201567.xes").aggregate(EventRelations.efr((_, _) => false, _: List[UnifiedEvent])),
    //          MatrixOfRelations.sortByOrderedList(List(), _: Set[String]))
    //        MatrixOfRelationsExporter.csvExport(efrMatrixPA201567, "C:\\bpic2018_work\\Payment application\\efr_201567_sorting0_5perc.csv", 5.0, "EFR")

    //// Main activities v2
    //    step_10a()
    //    step_10b()
    //    step_10c()
    //    step_20a()
    //    step_20b()
    //    step_20c()
    //    step_20d()
    //    step_20dd()
    //    step_20ddd
    //    step_20e()
    //    step_20ee()
    //    step_20f_level_4_201567_NOTshiftedYears_mainActiv2_keyVariants_PSM()
    //    step_20ff_level_44_201567_NOTshiftedYears_mainActiv3_keyVariants_PSM()
    //


    ////recources per year v1
    //    val level_3_2015_mainActiv2_ResourcesStripped: UnifiedEventLog = XesReader.xesToUnifiedEventLog(spark,
    //      NumSlices,
    //      XesReader.readXes(new File(FileNames.level_3_2015_mainActiv2)).head,
    //      Some(Set()),
    //      Some(Set(EventAttributes.Resource)),
    //      XesReader.DefaultActivitySep, EventAttributes.Resource)
    ////    exportExt(level_3_2015_mainActiv2_ResourcesStripped, s"${FileNames.rootYear("2015")}/level_3_2015_mainActiv2_ResourcesStripped.xes", addStripped = false)
    //    val level_3_2016_mainActiv2_ResourcesStripped: UnifiedEventLog = XesReader.xesToUnifiedEventLog(spark,
    //      NumSlices,
    //      XesReader.readXes(new File(FileNames.level_3_2016_mainActiv2)).head,
    //      Some(Set()),
    //      Some(Set(EventAttributes.Resource)),
    //      XesReader.DefaultActivitySep, EventAttributes.Resource)
    //    val level_3_2017_mainActiv2_ResourcesStripped: UnifiedEventLog = XesReader.xesToUnifiedEventLog(spark,
    //      NumSlices,
    //      XesReader.readXes(new File(FileNames.level_3_2017_mainActiv2)).head,
    //      Some(Set()),
    //      Some(Set(EventAttributes.Resource)),
    //      XesReader.DefaultActivitySep, EventAttributes.Resource)
    //    val logMerged = level_3_2015_mainActiv2_ResourcesStripped.fullOuterJoin(level_3_2016_mainActiv2_ResourcesStripped).fullOuterJoin(level_3_2017_mainActiv2_ResourcesStripped)
    ////    reportDurationsForDays(logMerged, FileNames.level_3_201567_mainActiv2_Resources_PSM,
    ////      Duration.ofDays(1).toMillis, false)
    //
    //    val shiftedLog2015 = shiftYears(level_3_2015_mainActiv2_ResourcesStripped, "2015", 0)
    //    val shiftedLog2016 = shiftYears(level_3_2016_mainActiv2_ResourcesStripped, "2016", 1)
    //    val shiftedLog2017 = shiftYears(level_3_2017_mainActiv2_ResourcesStripped, "2017", 2)
    //    val logMerged2 = shiftedLog2015.fullOuterJoin(shiftedLog2016).fullOuterJoin(shiftedLog2017)
    //    reportDurationsForDays(logMerged2, FileNames.level_3_201567_shiftedYears_mainActiv2_Resources_PSM,
    //      Duration.ofDays(1).toMillis, false)

    ////departments per year v1
    //    val shiftedLog2015 = shiftYears(loadLogWithDepartmentStripped(FileNames.level_1_2015), "2015", 0)
    //    val shiftedLog2016 = shiftYears(loadLogWithDepartmentStripped(FileNames.level_1_2016), "2016", 1)
    //    val shiftedLog2017 = shiftYears(loadLogWithDepartmentStripped(FileNames.level_1_2017), "2017", 2)
    //    val logMerged2 = shiftedLog2015.fullOuterJoin(shiftedLog2016).fullOuterJoin(shiftedLog2017)
    //    reportDurationsForDays(logMerged2, FileNames.level_3_201567_shiftedYears_mainActiv2_Resources_PSM,
    //      Duration.ofDays(1).toMillis, false)


    //resources
    //        reportAttribute(logCompleteResourcesStripped,  s"${FileNames.rootYear("allYears")}/psm/resources", Duration.ofDays(1).toMillis, EventAttributes.Resource,  "")
    //    createLogWithResourceDepartmentClassifier(FileNames.OriginalLogOnlyMainAttrs, FileNames.level_1_201567_ResourceDepartment) // RpcTimeoutException
    //    lazy val step100 = shiftYears_LogWithResourceDepartmentClassifier( // ObjectAggregationIterator: Aggregation hash map reaches threshold capacity (128 entries), spilling and falling back to sort based aggregation. You may change the threshold by adjust option spark.sql.objectHashAggregate.so
    //      FileNames.level_1_2015,
    //      FileNames.level_1_2016,
    //      FileNames.level_1_2017,
    //      FileNames.level_1_201567_ResourceDepartment_PSM)

    //    val step101 = shiftYears_LogWithResourceDepartmentClassifier(
    //      FileNames.level_3_2015_mainActiv2,
    //      FileNames.level_3_2015_mainActiv2,
    //      FileNames.level_3_2015_mainActiv2,
    //      FileNames.level_3_201567_ResourceDepartment_PSM)


    //создание матрицы efr для определения порядка сортировки

    //    def step_40() = {
    //      val logNames = Seq(FileNames.level_3_2015_mainActiv2, FileNames.level_3_2016_mainActiv2, FileNames.level_3_2017_mainActiv2)
    //      val sortedOrderPath = FileNames.sorting_order_template_byOriginalActivities_v8
    //      val newFileName = "level_3_201567_mainActiv2_sorting8_5perc"
    //      exportEfrDfrFootprint(logNames, false, sortedOrderPath, newFileName)
    //      exportEfrDfrFootprint(logNames, true, sortedOrderPath, newFileName)
    //      exportEfrDfr(logNames, true, sortedOrderPath, newFileName)
    //      exportEfrDfr(logNames, false, sortedOrderPath, newFileName)
    //    }
    //          val efrMatrix2015Medians = MatrixOfRelations(dsMatrix,
    //            MatrixOfRelations.sortByOrderedList(sortedActivities8, _: Set[String]),
    //            EmptyDescriptiveStatisticsEntry)


    //    step_50a_level_5_201567_keyVariants() // level_5_***_keyVariants -- create log
    //        step_50b_level_5_201567_keyVariants_PSM_NOTshiftedYears() // level_5_***_keyVariants -- PSM
    //    step_50c_level_5_201567_keyVariants_PSM_shiftedYears() // level_5_***_keyVariants -- shift years and to PSM
    //    step_60a_level_6_201567_keyVariants_GPD_PSM_shiftedYears() // НО НЕТ ОСОБОГО СМЫСЛА тк можно отфильтровать и в Диско и в ПСМ
    //    step_70a_level_7_201567_keyVariants_decide_PSM_shiftedYears() //
    //    step_70b_level_7_201567_keyVariants_validParcel_PSM_shiftedYears()
    //    step_70c_level_7_201567_keyVariants_RAperformedPAinitialize_PSM_shiftedYears()
    //    step_80a
    //    step_80b
    //    step_80c //OLD
    //        step_90a
    //                step_90aa
    //        step_90b
    //            step_90c
    //    step_90d
    //    step_90e
    //        step_100
    //        step_100a
    //        step_101a
    //        step_101b
    //        step_103a
    //    step_103a2
    //        step_103b
    //        step_103c
    //    step_104
    //    step_104b
    //    step_104b2
    //    step_104d
    //    step_104e
    //    step_104f
    //    step_104c
    //    step_104a
    //    step_105
    //    step_105a
    //    step_105b
    //    step_105c
    //    step_105c2
    //    step_106a
    //    step_106b
    //    step_107
    //    step_108
    //    step_109
    //    step_110
    //    step_111
    //    step_112a
    //    step_112b
    //    step_113a
    //    step_114a
    //    step_115a
    //    step_115b
    //////////////////
//    step_1001_create_log_L2_sealed
    //    step_1002_create_log_L3_activ17_cases100
    //    step_1003_create_log_L4_activ13_cases100_sealed

  }


}

