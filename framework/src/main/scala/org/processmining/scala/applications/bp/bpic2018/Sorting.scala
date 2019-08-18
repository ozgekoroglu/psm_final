package org.processmining.scala.applications.bp.bpic2018

import java.io.{File, PrintWriter}

import org.processmining.scala.log.common.csv.parallel.CsvReader

import scala.io.Source

object Sorting {

  private def maxSegmentsCounts(filename: String): Map[String, Long] = {
    def factory(keyIndex: Int, maxIndex: Int)(row: Array[String]): (String, Long) =
      (row(keyIndex), row(maxIndex).toLong)

    val csvReader = new CsvReader()
    val (header, lines) = csvReader.read(filename)
    csvReader.parse(lines, factory(header.indexOf("key"), header.indexOf("max")))
      .seq.toMap
  }

  private def sortingByOriginalActivities(templatePath: String, resultPath: String): Unit = {
    val lines = "any" :: Source.fromFile(templatePath).getLines.toList
    val newLines = lines.flatMap(x => lines.map(y => x + ":" + y))
    val pw = new PrintWriter(new File(resultPath))
    pw.println(resultPath)
    newLines.foreach(pw.println)
    pw.close
  }

  private def sortingByOriginalActivitiesWithYear(templatePath: String, resultPath: String): Unit = { // TODO не добавлять к any год
    val lines = "any" :: Source.fromFile(templatePath).getLines.toList
    val newLines = lines.flatMap(x => lines.map(y => List(s"2015-$x:2015-$y", s"2016-$x:2016-$y", s"2017-$x:2017-$y"))).flatten
    val pw = new PrintWriter(new File(resultPath))
    pw.println(resultPath)
    newLines.foreach(pw.println)
    pw.close
  }

  private def sortingByCountOfSegments(templatePath: String, resultPath: String): Unit = {
    val linesExample: Map[String, Long] = maxSegmentsCounts(templatePath)
    val newLines = linesExample.toList.sortWith(_._2 > _._2)
    val pw = new PrintWriter(new File(resultPath))
    pw.println(resultPath)
    newLines.map(x => pw.println(x._1))
    pw.close
  }

  def step_1a_byOriginalActivities_2() = {
    val (templatePath, resultPath) = FileNames.sortingOrderPaths("byOriginalActivities", "2")
    sortingByOriginalActivities(templatePath, resultPath)
  }

  def step_1b_byOriginalActivities_withYear_2() = {
    val (templatePath, resultPath) = FileNames.sortingOrderPaths_withYear("byOriginalActivities", "2")
    sortingByOriginalActivitiesWithYear(templatePath, resultPath)
  }

  def step_2a_byOriginalActivities_8() = {
    val (templatePath, resultPath) = FileNames.sortingOrderPaths("byOriginalActivities", "8")
    sortingByOriginalActivities(templatePath, resultPath)
  }

  def step_2b_byOriginalActivities_withYear_8() = {
    val (templatePath, resultPath) = FileNames.sortingOrderPaths_withYear("byOriginalActivities", "8")
    sortingByOriginalActivitiesWithYear(templatePath, resultPath)
  }

  def step_3a_byResorces_1() = {
    val (templatePath, resultPath) = FileNames.sortingOrderPaths("byResorces", "1")
    sortingByOriginalActivities(templatePath, resultPath)
  }

  def step_3b_byResorces_withYear_1() = {
    val (templatePath, resultPath) = FileNames.sortingOrderPaths_withYear("byResorces", "1")
    sortingByOriginalActivitiesWithYear(templatePath, resultPath)
  }

  def step_4_byCountOfSegments_completePsmActivities() = {
    val (templatePath, resultPath) = FileNames.sortingOrderResultPathByMax("completePsmActivities", FileNames.completePsmActivities)
    sortingByCountOfSegments(templatePath, resultPath)
  }

  def step_5_byCountOfSegments_level_0_allYears_ShiftedYears() = {
    val (templatePath, resultPath) = FileNames.sortingOrderResultPathByMax("level_0_allYears_ShiftedYears", FileNames.OriginalLogShiftedYearsPSM)
    sortingByCountOfSegments(templatePath, resultPath)
  }

  def step_6_byCountOfSegments_level_5_201567_keyVariants_PSM_shiftedYears() = {
    val (templatePath, resultPath) = FileNames.sortingOrderResultPathByMax("level_5_201567_keyVariants_PSM_shiftedYears", FileNames.level_5_201567_keyVariants_PSM_shiftedYears)
    sortingByCountOfSegments(templatePath, resultPath)
  }

  def step_7_byOriginalActivities_9() = {
    val (templatePath, resultPath) = FileNames.sortingOrderPaths("byOriginalActivities", "9")
    sortingByOriginalActivities(templatePath, resultPath)
  }

  def step_8_byCountOfSegments_level_14_201567_activAll_casesMilestone4_sealed_PAinitPAsdecide_1w() = {
    val (templatePath, resultPath) = FileNames.sortingOrderResultPathByMax("level_14_201567_activAll_casesMilestone4_sealed_PAinitPAsdecide_1w", FileNames.level_14_201567_activAll_casesMilestone4_sealed_PAinitPAsdecide_1w)
    sortingByCountOfSegments(templatePath, resultPath)
  }

  def step_9_byCountOfSegments_level_14_201567_activAll_casesMilestone4_sealed_mailValidGeoParcel_1w() = {
    val (templatePath, resultPath) = FileNames.sortingOrderResultPathByMax("level_14_201567_activAll_casesMilestone4_sealed_mailValidGeoParcel_1w", FileNames.level_14_201567_activAll_casesMilestone4_sealed_mailValidGeoParcel_1w)
    sortingByCountOfSegments(templatePath, resultPath)
  }

  def step_10_byCountOfSegments_level_15_201567_activAll_casesMilestone4_sealed_RecDep_1w() = {
    val (templatePath, resultPath) = FileNames.sortingOrderResultPathByMax("level_15_201567_activAll_casesMilestone4_sealed_RecDep_1w",
      FileNames.level_15_201567_activAll_casesMilestone4_sealed_RecDep_1w)
    sortingByCountOfSegments(templatePath, resultPath)
  }

  def step_11_byOriginalActivities_withYear_9() = {
    val (templatePath, resultPath) = FileNames.sortingOrderPaths_withYear("byOriginalActivities", "9")
    sortingByOriginalActivitiesWithYear(templatePath, resultPath)
  }

  def step_12_byOriginalActivities_withYear_12() = {
    val (templatePath, resultPath) = FileNames.sortingOrderPaths_withYear("byOriginalActivities", "12")
    sortingByOriginalActivitiesWithYear(templatePath, resultPath)
  }

  def step_13_byOriginalActivities_12() = {
    val (templatePath, resultPath) = FileNames.sortingOrderPaths("byOriginalActivities", "12")
    sortingByOriginalActivities(templatePath, resultPath)
  }

  def main(args: Array[String]): Unit = {

    //    step_1a_byOriginalActivities_2
    //    step_1b_byOriginalActivities_withYear_2
    //    step_2a_byOriginalActivities_8
    //    step_2b_byOriginalActivities_withYear_8
    //    step_3a_byResorces_1
    //    step_3b_byResorces_withYear_1
    //    step_4_byCountOfSegments_completePsmActivities
    //    step_5_byCountOfSegments_level_0_allYears_ShiftedYears
    //    step_6_byCountOfSegments_level_5_201567_keyVariants_PSM_shiftedYears
    //    step_7_byOriginalActivities_9
    //    step_8_byCountOfSegments_level_14_201567_activAll_casesMilestone4_sealed_PAinitPAsdecide_1w
    //    step_9_byCountOfSegments_level_14_201567_activAll_casesMilestone4_sealed_mailValidGeoParcel_1w
    //    step_10_byCountOfSegments_level_15_201567_activAll_casesMilestone4_sealed_RecDep_1w
    //    step_11_byOriginalActivities_withYear_9
    //    step_12_byOriginalActivities_withYear_12
    //    step_13_byOriginalActivities_12

    //    sortingByOriginalActivities(
    //      "F:\\psm\\Road_Traffic_Fine_Management_Process\\outputV9Q\\30d\\Road_Traffic_Fine_Management_Process.xes.gz\\duration\\sorting_order_template.txt",
    //      "F:\\psm\\Road_Traffic_Fine_Management_Process\\outputV9Q\\30d\\Road_Traffic_Fine_Management_Process.xes.gz\\duration\\sorting_order_result.txt")

  }
}
