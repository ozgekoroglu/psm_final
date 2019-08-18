package org.processmining.scala.applications.bp.bpic2018

import java.io.File
import java.util.regex.Pattern

import org.processmining.scala.applications.common.spark.EvaluationHelper
import org.processmining.scala.log.common.filtering.expressions.events.regex.impl.EventEx
import org.processmining.scala.log.common.unified.event.UnifiedEvent
import org.processmining.scala.log.common.unified.log.spark.UnifiedEventLog
import org.processmining.scala.log.common.unified.log.common.UnifiedEventLog.UnifiedTrace
import org.processmining.scala.log.common.xes.parallel.{XesReader, XesWriter}


object FileNames {
  private val Root = "C:/bpic2018_work"

  def rootYear(year: String) = s"$Root/$year" // year: allYears, 2015, 2016, 2017
  def rootYearPsm(year: String) = s"$Root/$year/psm"

  def rootFollowings() = s"$Root/followings"

  val OriginalLog = s"$Root/input/BPI Challenge 2018.xes"
  val OriginalLogOnlyMainAttrs = s"$Root/allYears/level_0_allYears_onlyMainAttrs.xes"
  val OriginalLogOnlyMainAttrStripped = s"$Root/allYears/level_0_allYears_onlyMainAttrs.xesstripped.xes"
  val OriginalLogOnlyMinAttrs = s"$Root/allYears/level_0_allYears_onlyMinAttrs.xes"
  val OriginalLogOnlyMinAttrsPSM = s"$Root/allYears/psm/level_0_allYears_onlyMinAttrs.xes"
  val completePsmActivities = s"$Root/allYears/psm/complete_activities.4q"
  val completePsmActivities_1week = s"$Root/allYears/psm/complete_activities_1week.4q"
  val completePsmResources = s"$Root/allYears/psm/complete_resources.4q"
  val TestLog1_fromDisco = s"$Root/allYears/TestLog1_fromDisco.xes"
  val testLog2_sample01 = s"$Root/allYears/testLog2_sample01.xes"
  val testLog2_sample01_PSM = s"$Root/allYears/psm//testLog2_sample01.xes"
  val testLog3_sample001 = s"$Root/allYears/testLog3_sample001.xes"
  val testLog3_sample001_PSM = s"$Root/allYears/psm//testLog3_sample001.xes"
  val OriginalLogDoctypeSubprocess = s"$Root/allYears/level_0_allYears_onlyMainAttrs_DoctypeSubprocess.xes"
  val OriginalLogShiftedYears = s"$Root/allYears/level_0_allYears_ShiftedYears.xes"
  val OriginalLogShiftedYearsMainActiv1 = s"$Root/allYears/level_3_allYears_ShiftedYears_mainActiv1.xes"
  val OriginalLogShiftedYearsPSM = s"$Root/allYears/psm/level_0_allYears_ShiftedYears.xes"
  val level_1_2015 = s"$Root/2015/level_1_2015.xes"
  val level_1_2016 = s"$Root/2016/level_1_2016.xes"
  val level_1_2017 = s"$Root/2017/level_1_2017.xes"
  val level_3_2015_mainActiv2 = s"$Root/2015/level_3_2015_mainActiv2.xes"
  val level_3_2016_mainActiv2 = s"$Root/2016/level_3_2016_mainActiv2.xes"
  val level_3_2017_mainActiv2 = s"$Root/2017/level_3_2017_mainActiv2.xes"
  val level_3_201567_shiftedYears_mainActiv2 = s"$Root/allYears/level_3_201567_shiftedYears_mainActiv2.xes"
  val level_3_201567_shiftedYears_mainActiv2_PSM = s"$Root/allYears/psm/level_3_201567_shiftedYears_mainActiv2.xes"
  val level_3_201567_mainActiv2_PSM = s"$Root/allYears/psm/level_3_201567_mainActiv2.xes"
  val level_3_201567_mainActiv2_Resources_PSM = s"$Root/allYears/psm/level_3_201567_mainActiv2_Resources.xes"
  val level_3_201567_shiftedYears_mainActiv2_Resources_PSM = s"$Root/allYears/psm/level_3_201567_shiftedYears_mainActiv2_Resources.xes"
  val level_1_201567_shiftedYears_Departments_PSM = s"$Root/allYears/psm/level_3_201567_shiftedYears_mainActiv2_Resources.xes"
  val level_1_201567_ResourceDepartment = s"$Root/allYears/level_1_201567_ResourceDepartment.xes"
  val level_1_201567_ResourceDepartment_PSM = s"$Root/allYears/psm/level_1_201567_ResourceDepartment.xes"
  val level_3_201567_ResourceDepartment = s"$Root/allYears/level_3_201567_ResourceDepartment.xes"
  val level_3_201567_ResourceDepartment_PSM = s"$Root/allYears/psm/level_3_201567_ResourceDepartment.xes"
  val level_4_2015_mainActiv2_keyVariants = s"$Root/2015/level_4_2015_mainActiv2_keyVariants.xes"
  val level_4_2016_mainActiv2_keyVariants = s"$Root/2016/level_4_2016_mainActiv2_keyVariants.xes"
  val level_4_2017_mainActiv2_keyVariants = s"$Root/2017/level_4_2017_mainActiv2_keyVariants.xes"
  val level_4_201567_mainActiv2_keyVariants = s"$Root/allYears/level_4_201567_mainActiv2_keyVariants.xes"
  val level_44_201567_mainActiv3_keyVariants = s"$Root/allYears/level_44_201567_mainActiv3_keyVariants.xes"
  val level_4_201567_shiftedYears_mainActiv2_keyVariants = s"$Root/allYears/level_4_201567_shiftedYears_mainActiv2_keyVariants.xes"
  val level_4_201567_NOTshiftedYears_mainActiv2_keyVariants_PSM = s"$Root/allYears/psm/level_4_201567_NOTshiftedYears_mainActiv2_keyVariants_PSM.xes"
  val level_44_201567_NOTshiftedYears_mainActiv3_keyVariants_PSM = s"$Root/allYears/psm/level_44_201567_NOTshiftedYears_mainActiv3_keyVariants_PSM.xes"
  val level_4_201567_shiftedYears_mainActiv2_keyVariants_PSM = s"$Root/allYears/psm/level_4_201567_shiftedYears_mainActiv2_keyVariants_PSM.xes"
  val level_5_2015_keyVariants = s"$Root/2015/level_5_2015_keyVariants.xes"
  val level_5_2016_keyVariants = s"$Root/2016/level_5_2016_keyVariants.xes"
  val level_5_2017_keyVariants = s"$Root/2017/level_5_2017_keyVariants.xes"
  val level_5_201567_keyVariants = s"$Root/allYears/level_5_201567_keyVariants.xes"
  val level_5_201567_keyVariants_PSM_NOTshiftedYears = s"$Root/allYears/psm/level_5_201567_keyVariants_PSM_NOTshiftedYears.xes"
  val level_5_201567_keyVariants_PSM_shiftedYears = s"$Root/allYears/psm/level_5_201567_keyVariants_PSM_shiftedYears.xes"
  val level_6_2015_keyVariants_GPD = s"$Root/2015/level_6_2015_keyVariants_GPD.xes"
  val level_6_2016_keyVariants_GPD = s"$Root/2016/level_6_2016_keyVariants_GPD.xes"
  val level_6_2017_keyVariants_GPD = s"$Root/2017/level_6_2017_keyVariants_GPD.xes"
  val level_6_201567_keyVariants_GPD = s"$Root/allYears/level_6_201567_keyVariants_GPD.xes"
  val level_6_201567_keyVariants_GPD_PSM_shiftedYears = s"$Root/allYears/psm/level_6_201567_keyVariants_GPD_PSM_shiftedYears.xes"
  val level_6_201567_keyVariants_PA = s"$Root/allYears/level_6_201567_keyVariants_PA.xes"
  val level_6_201567_keyVariants_PA_PSM_shiftedYears = s"$Root/allYears/psm/level_6_201567_keyVariants_PA_PSM_shiftedYears.xes"
  val level_7_201567_keyVariants_decide = s"$Root/allYears/level_7_201567_keyVariants_decide.xes"
  val level_7_201567_keyVariants_decide_PSM_shiftedYears = s"$Root/allYears/psm/level_7_201567_keyVariants_decide_PSM_shiftedYears.xes"
  val level_7_201567_keyVariants_validParcel = s"$Root/allYears/level_7_201567_keyVariants_validParcel.xes"
  val level_7_201567_keyVariants_validParcel_PSM_shiftedYears = s"$Root/allYears/psm/level_7_201567_keyVariants_validParcel_PSM_shiftedYears.xes"
  val level_7_201567_keyVariants_RAperformedPAinitialize = s"$Root/allYears/level_7_201567_keyVariants_RAperformedPAinitialize.xes"
  val level_7_201567_keyVariants_RAperformedPAinitialize_PSM_shiftedYears = s"$Root/allYears/psm/level_7_201567_keyVariants_RAperformedPAinitialize_PSM_shiftedYears.xes"
  val level_10_201567_activAll_casesAll_sealed = s"$Root/allYears/level_10_201567_activAll_casesAll_sealed.xes"
  val level_10_201567_activAll_casesAll_sealed_PSM = s"$Root/allYears/psm/level_10_201567_activAll_casesAll_sealed_PSM.xes"
  val level_10_2015_activAll_casesAll_sealed = s"$Root/allYears/level_10_2015_activAll_casesAll_sealed.xes"
  val level_10_2016_activAll_casesAll_sealed = s"$Root/allYears/level_10_2016_activAll_casesAll_sealed.xes"
  val level_10_2017_activAll_casesAll_sealed = s"$Root/allYears/level_10_2017_activAll_casesAll_sealed.xes"
  val level_11_201567_activMilestone4_casesAll_sealed_stripped = s"$Root/allYears/level_11_201567_activMilestone4_casesAll_sealed_stripped.xes"
  val level_11_2015_activMilestone4_casesAll_sealed_stripped = s"$Root/allYears/level_11_2015_activMilestone4_casesAll_sealed_stripped.xes"
  val level_11_2016_activMilestone4_casesAll_sealed_stripped = s"$Root/allYears/level_11_2016_activMilestone4_casesAll_sealed_stripped.xes"
  val level_11_2017_activMilestone4_casesAll_sealed_stripped = s"$Root/allYears/level_11_2017_activMilestone4_casesAll_sealed_stripped.xes"
  val level_11_201567_activMilestone4_casesAll_sealed_Shifted_1w_PSM = s"$Root/allYears/psm/level_11_201567_activMilestone4_casesAll_sealed_Shifted_1w_PSM.xes"
  val level_12_201567_activAll_casesMilestone4_sealed = s"$Root/allYears/level_12_201567_activAll_casesMilestone4_sealed.xes"
  val level_12_201567_activAll_casesMilestone4_sealed_PSM = s"$Root/allYears/psm/level_12_201567_activAll_casesMilestone4_sealed_PSM.xes"
  val level_12_201567_activAll_casesMilestone4_sealed_PSM_1w = s"$Root/allYears/psm/level_12_201567_activAll_casesMilestone4_sealed_PSM_1w.xes"
  val level_12_201567_activAll_casesMilestone4_sealed_firstActiv_PSM_1w = s"$Root/allYears/psm/level_12_201567_activAll_casesMilestone4_sealed_firstActiv_PSM_1w.xes"
  val level_12_201567_activAll_casesMilestone4_sealed_Shifted_1w = s"$Root/allYears/psm/level_12_201567_activAll_casesMilestone4_sealed_Shifted_1w.xes"
  val level_12_201567_activAll_casesMilestone4_sealed_Shifted_1d = s"$Root/allYears/psm/level_12_201567_activAll_casesMilestone4_sealed_Shifted_1d.xes"
  val level_12_2015_activAll_casesMilestone4_sealed = s"$Root/allYears/level_12_2015_activAll_casesMilestone4_sealed.xes"
  val level_12_2016_activAll_casesMilestone4_sealed = s"$Root/allYears/level_12_2016_activAll_casesMilestone4_sealed.xes"
  val level_12_2017_activAll_casesMilestone4_sealed = s"$Root/allYears/level_12_2017_activAll_casesMilestone4_sealed.xes"
  val level_14_201567_activAll_casesMilestone4_sealed_PAinitPAsdecide = s"$Root/allYears/level_14_201567_activAll_casesMilestone4_sealed_PAinitPAsdecide.xes"
  val level_14_201567_activAll_casesMilestone4_sealed_PAinitPAsdecide_1d = s"$Root/allYears/psm/level_14_201567_activAll_casesMilestone4_sealed_PAinitPAsdecide_1d.xes"
  val level_14_201567_activAll_casesMilestone4_sealed_PAinitPAsdecide_1w = s"$Root/allYears/psm/level_14_201567_activAll_casesMilestone4_sealed_PAinitPAsdecide_1w.xes"
  val level_14_201567_activAll_casesMilestone4_sealed_mailValidGeoParcel = s"$Root/allYears/level_14_201567_activAll_casesMilestone4_sealed_mailValidGeoParcel.xes"
  val level_14_201567_activAll_casesMilestone4_sealed_mailValidGeoParcel_1d = s"$Root/allYears/psm/level_14_201567_activAll_casesMilestone4_sealed_mailValidGeoParcel_1d.xes"
  val level_14_201567_activAll_casesMilestone4_sealed_mailValidGeoParcel_1w = s"$Root/allYears/psm/level_14_201567_activAll_casesMilestone4_sealed_mailValidGeoParcel_1w.xes"
  val level_14_201567_activAll_casesMilestone4_sealed_RefinitPAinit = s"$Root/allYears/level_14_201567_activAll_casesMilestone4_sealed_RefinitPAinit.xes"
  val level_14_201567_activAll_casesMilestone4_sealed_RefinitPAinit_1d = s"$Root/allYears/psm/level_14_201567_activAll_casesMilestone4_sealed_RefinitPAinit_1d.xes"
  val level_14_201567_activAll_casesMilestone4_sealed_RefinitPAinit_1w = s"$Root/allYears/psm/level_14_201567_activAll_casesMilestone4_sealed_RefinitPAinit_1w.xes"
  val level_13_201567_activMilestone4_casesMilestone4_sealed = s"$Root/allYears/level_13_201567_activMilestone4_casesMilestone4_sealed.xes"
  val level_13_2015_activMilestone4_casesMilestone4_sealed = s"$Root/allYears/level_13_2015_activMilestone4_casesMilestone4_sealed.xes"
  val level_13_2016_activMilestone4_casesMilestone4_sealed = s"$Root/allYears/level_13_2016_activMilestone4_casesMilestone4_sealed.xes"
  val level_13_2017_activMilestone4_casesMilestone4_sealed = s"$Root/allYears/level_13_2017_activMilestone4_casesMilestone4_sealed.xes"
  val level_13_201567_activMilestone4_casesMilestone4_sealed_PSM = s"$Root/allYears/psm/level_13_201567_activMilestone4_casesMilestone4_sealed_PSM.xes"
  val level_13_201567_activMilestone4_casesMilestone4_sealed_1w_PSM = s"$Root/allYears/psm/level_13_201567_activMilestone4_casesMilestone4_sealed_1w_PSM.xes"
  val level_13_201567_activMilestone4_casesMilestone4_sealed_Shifted_1w_PSM = s"$Root/allYears/psm/level_13_201567_activMilestone4_casesMilestone4_sealed_Shifted_1w_PSM.xes"
  val level_13_201567_activMilestone4_casesMilestone4_sealed_Shifted_1d_PSM = s"$Root/allYears/psm/level_13_201567_activMilestone4_casesMilestone4_sealed_Shifted_1d_PSM.xes"
  val level_13_201567_activMilestone4_casesMilestone4_sealed_firstActiv_Shifted_1d_PSM = s"$Root/allYears/psm/level_13_201567_activMilestone4_casesMilestone4_sealed_firstActiv_Shifted_1d_PSM.xes"
  val level_13_201567_activMilestone4_casesMilestone4_sealed_firstActiv_PSM = s"$Root/allYears/psm/level_13_201567_activMilestone4_casesMilestone4_sealed_firstActiv_PSM.xes"
  val level_13_201567_activMilestone4_casesMilestone4_sealed_firstActiv_1w_PSM = s"$Root/allYears/psm/level_13_201567_activMilestone4_casesMilestone4_sealed_firstActiv_1w_PSM.xes"
  val level_15_201567_activAll_casesMilestone4_sealed_RecDep = s"$Root/allYears/level_15_201567_activAll_casesMilestone4_sealed_RecDep.xes"
  val level_15_201567_activAll_casesMilestone4_sealed_RecDep_1w = s"$Root/allYears/psm/level_15_201567_activAll_casesMilestone4_sealed_RecDep_1w.xes"
  val level_16_201567_activAll_casesMilestone4_sealed_Dep = s"$Root/allYears/level_16_201567_activAll_casesMilestone4_sealed_Dep.xes"
  val level_16_201567_activAll_casesMilestone4_sealed_Dep_1w = s"$Root/allYears/psm/level_16_201567_activAll_casesMilestone4_sealed_Dep_1w.xes"
  val level_17_201567_activAll_casesMilestone4_sealed_ActDep = s"$Root/allYears/level_17_201567_activAll_casesMilestone4_sealed_ActDep.xes"
  val level_17_201567_activAll_casesMilestone4_sealed_ActDep_firstActiv_1w_PSM = s"$Root/allYears/psm/level_17_201567_activAll_casesMilestone4_sealed_ActDep_firstActiv_1w_PSM.xes"

  val log_L2_activ187_cases100_sealed = s"F:/bpic2018_F/logs/log_L2_201567_activAll_casesAll_sealed.xes"
  val log_L3_activ17_cases100_sealed = s"F:/bpic2018_F/logs/log_L3_activ17_cases100_sealed.xes"
  val log_L4_activ13_cases100_sealed = s"F:/bpic2018_F/logs/log_L4_activ13_cases100_sealed.xes"
  val log_L5_activ13_cases80_sealed = s"F:/bpic2018_F/logs/log_L5_activ13_cases80_sealed.xes"
  val log_L6_activ187_cases80_sealed = s"F:/bpic2018_F/logs/log_L6_activ187_cases80_sealed.xes"

  val sorting_order_template_byOriginalActivities_v8 = s"$Root/sorting_order/sorting_order_template_byOriginalActivities_v8.txt"
  val sorting_order_template_byOriginalActivities_v9 = s"$Root/sorting_order/sorting_order_template_byOriginalActivities_v9.txt"
  val efr_201567_mainActiv2_sorting8_5perc = s"$Root/followings/efr_201567_mainActiv2_sorting8_5perc.csv"
  val dfr_201567_mainActiv2_sorting8_5perc = s"$Root/followings/dfr_201567_mainActiv2_sorting8_5perc.csv"
  val efr_2015_mainActiv2_sorting8_5perc = s"$Root/followings/efr_2015_mainActiv2_sorting8_5perc.csv"
  val efr_2016_mainActiv2_sorting8_5perc = s"$Root/followings/efr_2016_mainActiv2_sorting8_5perc.csv"
  val efr_2017_mainActiv2_sorting8_5perc = s"$Root/followings/efr_2017_mainActiv2_sorting8_5perc.csv"

  val efr_ds = s"$Root/followings/efr_mainActiv2_sorting8_5perc_ds.csv" //delete

  //  def efr_dse_html(title: String) = s"$Root/followings/efr_mainActiv2_sorting8_dse_$title.html"
  //  def dfr_dse_html(title: String) = s"$Root/followings/dfr_mainActiv2_sorting8_dse_$title.html"

  def efr_dse_html(title: String) = s"$Root/followings/level_4_201567_mainActiv2_keyVariants_sorting8_5perc/efr_level_4_201567_mainActiv2_keyVariants_sorting8_5perc_dse_$title.html" // TODO
  def dfr_dse_html(title: String) = s"$Root/followings/level_4_201567_mainActiv2_keyVariants_sorting8_5perc/dfr_level_4_201567_mainActiv2_keyVariants_sorting8_5perc_dse_$title.html" // TODO

  val efr_img = "efr_img/"
  //  val efr_ds_html = s"$efr_ds_html_root/efr_mainActiv2_sorting8_ds_distr.html" // OLD

  //  val dfr_ds_html = s"$dfr_ds_html_root/dfr_mainActiv2_sorting8_ds_distr.html" // OLD
  val dfr_img = "dfr_img/"


  val efr_html = s"$Root/followings/efr/efr_mainActiv2_sorting8.html"
  //delete
  val dfr_html = s"$Root/followings/dfr/dfr_mainActiv2_sorting8.html" //delete


  val MergedActivities2015 = s"$Root/merged_activities_2015.csv"
  //delete
  val MergedActivities = s"$Root/merged_activities.csv" //delete

  def completeLogForYear(year: String) = s"$Root/$year/level_1_$year.xes"

  def sortingOrderPaths(filename: String, version: String): (String, String) = {
    (s"$Root/sorting_order/sorting_order_template_${filename}_v${version}.txt",
      s"$Root/sorting_order/sorting_order_result_${filename}_v${version}.txt")
  }

  def sortingOrderPaths_withYear(filename: String, version: String): (String, String) = {
    (s"$Root/sorting_order/sorting_order_template_${filename}_v${version}.txt",
      s"$Root/sorting_order/sorting_order_result_${filename}_v${version}_withYear.txt")
  }

  def sortingOrderResultPathByMax(logName: String, logPSMPath: String): (String, String) = {
    (s"${logPSMPath}/duration/max.csv",
      s"${logPSMPath}/duration/sorting_order_result_byCountOfSegments_${logName}.txt")
  }

  //  def logForYear(year: String, folderInRoot: String, newLogName: String) = s"$Root/$folderInRoot/${newLogName}_$year.xes" // old
  def logForYear(year: String, folderInRoot: String, newLogName: String) = s"$Root/${year}/${folderInRoot}_${year}_${newLogName}.xes"

  def completeLogForYearStripped(year: String) = completeLogForYear(year) + "stripped.xes"

  def completeLogForYear(year: String, level: Int, note: String) = s"$Root/$year/level_${level.toString}_${year}_$note.xes"

  def completeLogForAllYears(newLogName: String) = s"$Root/allYears/allYears_$newLogName.xes"

  def completePsmDurationForYear(year: String) = s"$Root/$year/psm/level_1_$year.4q"

  def q1UndesiredOutcom(year: String, outcomeNumber: Int) = s"$Root/$year/level_2_${year}_q1_undesired_outcome_$outcomeNumber.xes"

  def q1UndesiredOutcomIds(year: String, outcomeNumber: Int) = s"$Root/$year/level_2_${year}_q1_undesired_outcome_IDs_$outcomeNumber.txt"

  def q1DesiredOutcom(year: String) = s"$Root/$year/level_2_${year}_q1_desired_outcome.xes"
}

class Bpic2018Template extends EvaluationHelper("BPIC'2018") {

  val DefaultClassifier = Array(EventAttributes.Doctype, EventAttributes.Subprocess, EventAttributes.Activity)
  val DefaultYearClassifier = Array(TraceAttributes.Year, EventAttributes.Doctype, EventAttributes.Subprocess, EventAttributes.Activity)
  val DoctypeSubprocessClassifier = Array(EventAttributes.Doctype, EventAttributes.Subprocess)
  val ResourceDepartmentClassifier = Array(EventAttributes.Resource, TraceAttributes.Department)
  val ActivDepartmentClassifier = Array(EventAttributes.Doctype, EventAttributes.Subprocess, EventAttributes.Activity, TraceAttributes.Department)
  val DepartmentClassifier = Array(TraceAttributes.Department)
  val NumSlices = 2500
  val t = org.processmining.scala.log.common.filtering.expressions.traces.impl.TraceExpression()


  lazy val logCompleteActivitiesStripped: UnifiedEventLog = org.processmining.scala.log.common.xes.spark.XesReader.xesToUnifiedEventLog(spark,
    NumSlices,
    XesReader.readXes(new File(FileNames.OriginalLog)).head,
    Some(Set()),
    Some(DefaultClassifier.toSet),
    XesReader.DefaultActivitySep, DefaultClassifier: _*)

  lazy val logCompleteResourcesStripped: UnifiedEventLog = org.processmining.scala.log.common.xes.spark.XesReader.xesToUnifiedEventLog(spark,
    NumSlices,
    XesReader.readXes(new File(FileNames.OriginalLog)).head,
    Some(Set()),
    Some(Set(EventAttributes.Resource)),
    XesReader.DefaultActivitySep, EventAttributes.Resource)

  lazy val log_level_0_allYears_onlyMainAttrs: UnifiedEventLog = org.processmining.scala.log.common.xes.spark.XesReader.xesToUnifiedEventLog(spark,
    NumSlices,
    XesReader.readXes(new File(FileNames.OriginalLogOnlyMainAttrs)).head,
    Some(TraceAttributes.SetOfMainAttributes),
    Some(EventAttributes.SetOfMainAttributes),
    XesReader.DefaultActivitySep, DefaultClassifier: _*)

  lazy val log_level_0_allYears_onlyMinAttrs_DoctypeSubprocess: UnifiedEventLog = org.processmining.scala.log.common.xes.spark.XesReader.xesToUnifiedEventLog(spark,
    NumSlices,
    XesReader.readXes(new File(FileNames.OriginalLogOnlyMinAttrs)).head,
    Some(Set()),
    Some(DoctypeSubprocessClassifier.toSet),
    XesReader.DefaultActivitySep, DoctypeSubprocessClassifier: _*)

  lazy val logShifted: UnifiedEventLog = org.processmining.scala.log.common.xes.spark.XesReader.xesToUnifiedEventLog(spark,
    NumSlices,
    XesReader.readXes(new File(FileNames.OriginalLogShiftedYears)).head,
    Some(Set()),
    Some(EventAttributes.SetOfMinAttributes),
    XesReader.DefaultActivitySep, DefaultYearClassifier: _*)

  lazy val logShiftedStripped: UnifiedEventLog = org.processmining.scala.log.common.xes.spark.XesReader.xesToUnifiedEventLog(spark,
    NumSlices,
    XesReader.readXes(new File(FileNames.OriginalLogShiftedYears)).head,
    Some(Set()),
    Some(Set()),
    XesReader.DefaultTraceAttrNamePrefix, XesReader.DefaultActivitySep)

  def loadLog(filename: String): UnifiedEventLog = org.processmining.scala.log.common.xes.spark.XesReader.xesToUnifiedEventLog(spark,
    NumSlices,
    XesReader.readXes(new File(filename)).head,
    None,
    None,
    XesReader.DefaultActivitySep, DefaultClassifier: _*)

  def loadStrippedLog(filename: String): UnifiedEventLog = org.processmining.scala.log.common.xes.spark.XesReader.xesToUnifiedEventLog(spark,
    NumSlices,
    XesReader.readXes(new File(filename)).head,
    Some(Set(TraceAttributesWithoutPrefix.Year)),
    Some(DefaultClassifier.toSet),
    XesReader.DefaultActivitySep, DefaultClassifier: _*)

  def loadShiftedStrippedLog(filename: String): UnifiedEventLog = org.processmining.scala.log.common.xes.spark.XesReader.xesToUnifiedEventLog(spark,
    NumSlices,
    XesReader.readXes(new File(filename)).head,
    Some(Set(TraceAttributesWithoutPrefix.Year)),
    Some(DefaultClassifier.toSet),
    XesReader.DefaultTraceAttrNamePrefix, DefaultYearClassifier: _*)

  def loadLogWithYear(filename: String): UnifiedEventLog = org.processmining.scala.log.common.xes.spark.XesReader.xesToUnifiedEventLog(spark,
    NumSlices,
    XesReader.readXes(new File(filename)).head,
    None,
    None,
    XesReader.DefaultActivitySep, DefaultYearClassifier: _*)

  def loadStrippedLogWithYear(filename: String): UnifiedEventLog = org.processmining.scala.log.common.xes.spark.XesReader.xesToUnifiedEventLog(spark,
    NumSlices,
    XesReader.readXes(new File(filename)).head,
    Some(Set(TraceAttributesWithoutPrefix.Year)),
    Some(DefaultClassifier.toSet),
    XesReader.DefaultActivitySep, DefaultYearClassifier: _*)

  def loadLogResourceDepartment(filename: String): UnifiedEventLog = org.processmining.scala.log.common.xes.spark.XesReader.xesToUnifiedEventLog(spark,
    NumSlices,
    XesReader.readXes(new File(filename)).head,
    None,
    None,
    XesReader.DefaultActivitySep, ResourceDepartmentClassifier: _*)

  def loadLogResourceDepartmentStripped(filename: String): UnifiedEventLog = org.processmining.scala.log.common.xes.spark.XesReader.xesToUnifiedEventLog(spark,
    NumSlices,
    XesReader.readXes(new File(filename)).head,
    Some(Set(TraceAttributesWithoutPrefix.Department)),
    Some(Set(EventAttributes.Resource)),
    XesReader.DefaultActivitySep, ResourceDepartmentClassifier: _*)

  def loadLogDepartmentStripped(filename: String): UnifiedEventLog = org.processmining.scala.log.common.xes.spark.XesReader.xesToUnifiedEventLog(spark,
    NumSlices,
    XesReader.readXes(new File(filename)).head,
    Some(Set(TraceAttributesWithoutPrefix.Department)),
    Some(Set(EventAttributes.Doctype, EventAttributes.Subprocess, EventAttributes.Activity, EventAttributes.Resource)),
    XesReader.DefaultActivitySep, DepartmentClassifier: _*)

  def loadLogActivDepartment(filename: String): UnifiedEventLog = org.processmining.scala.log.common.xes.spark.XesReader.xesToUnifiedEventLog(spark,
    NumSlices,
    XesReader.readXes(new File(filename)).head,
    None,
    None,
    XesReader.DefaultActivitySep, ActivDepartmentClassifier: _*)

  def loadLogActivDepartmentStripped(filename: String): UnifiedEventLog = org.processmining.scala.log.common.xes.spark.XesReader.xesToUnifiedEventLog(spark,
    NumSlices,
    XesReader.readXes(new File(filename)).head,
    Some(Set(TraceAttributesWithoutPrefix.Department)),
    Some(Set(EventAttributes.Doctype, EventAttributes.Subprocess, EventAttributes.Activity)),
    XesReader.DefaultActivitySep, ActivDepartmentClassifier: _*)

  def logWithoutChangeDepartment1(log: UnifiedEventLog): UnifiedEventLog = {
    def traceWithoutChangeDepartment(trace: List[UnifiedEvent]): List[UnifiedEvent] = trace match {
      case Nil => Nil
      case x :: xTail =>
        if (x.activity.contains("change department")) Nil
        else x :: traceWithoutChangeDepartment(xTail)
    }

    UnifiedEventLog.fromTraces(
      log.traces().map(trace => (trace._1, traceWithoutChangeDepartment(trace._2)
      )))
  }

  //  def logWithoutChangeDepartment2(log: UnifiedEventLog): UnifiedEventLog = {
  //    val exWithoutChangeDepartment = t contains EventEx(".+").attrs.withValue(EventAttributes.Activity, "change department")
  //    val log =
  //      log.filter(exWithoutChangeDepartment)
  //  }

  lazy val logComplete: UnifiedEventLog = loadLog(FileNames.OriginalLog)
  lazy val logOnlyMainAttrs: UnifiedEventLog = loadLog(FileNames.OriginalLogOnlyMainAttrs)
  lazy val logTestFromDisco: UnifiedEventLog = loadLog(FileNames.TestLog1_fromDisco)
  lazy val testLog2_sample01: UnifiedEventLog = loadLog(FileNames.testLog2_sample01)
  lazy val testLog3_sample001: UnifiedEventLog = loadLog(FileNames.testLog3_sample001)
  lazy val log1_2015: UnifiedEventLog = loadLog(FileNames.completeLogForYear("2015"))
  lazy val log1_2016: UnifiedEventLog = loadLog(FileNames.completeLogForYear("2016"))
  lazy val log1_2017: UnifiedEventLog = loadLog(FileNames.completeLogForYear("2017"))
  lazy val log1_2015Stripped: UnifiedEventLog = loadLog(FileNames.completeLogForYearStripped("2015"))

  def getLogByYear(year: Int) = year match {
    case 2015 => log1_2015
    case 2016 => log1_2016
    case 2017 => log1_2017
    case _ => ???
  }

  def exportExt(log: org.processmining.scala.log.common.unified.log.parallel.UnifiedEventLog, filename: String, addStripped: Boolean): Unit = {
    logger.info(s"Exporting $filename")
    XesWriter.write(log, filename, XesReader.DefaultTraceAttrNamePrefix)
    if (addStripped) {
      logger.info(s"Exporting stripped $filename")
      val strippedLog = log.projectAttributes((EventAttributes.Timestamp :: EventAttributes.Success :: EventAttributes.Resource :: EventAttributes.ConceptName :: DefaultClassifier.toList).toSet)
      XesWriter.write(strippedLog, filename + "stripped.xes", XesReader.DefaultTraceAttrNamePrefix)
    }
  }

  def exportExt(log: UnifiedEventLog, filename: String, addStripped: Boolean): Unit =
    exportExt(org.processmining.scala.log.common.unified.log.spark.UnifiedEventLog.create(log), filename, addStripped)

  def renameActivity(name: String) = s"sealed $name"

  def renameLastEventIf(ex: EventEx, isNotFollowedBy: EventEx, newName: String => String = renameActivity)(trace: UnifiedTrace): UnifiedTrace = {
    val patternEx = Pattern.compile(ex.translate())
    val patternIsNotFollowedBy = Pattern.compile(isNotFollowedBy.translate())
    val z0 = (false, List[UnifiedEvent]())

    (trace._1, trace._2.foldRight(z0) { (e, z) =>
      if (!z._1 && patternIsNotFollowedBy.matcher(e.regexpableForm()).matches()) (true, e :: z._2)
      else if (z._1) (true, e :: z._2)
      else if (patternEx.matcher(e.regexpableForm()).matches()) (true, e.copy(newName(e.activity)).copy(EventAttributes.Activity, newName(e.attributes(EventAttributes.Activity).asInstanceOf[String])) :: z._2) else (false, e :: z._2)
    }._2)
  }


}