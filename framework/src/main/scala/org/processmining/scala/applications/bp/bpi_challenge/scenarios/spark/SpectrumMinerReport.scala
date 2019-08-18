package org.processmining.scala.applications.bp.bpi_challenge.scenarios.spark

import java.util.Date

import org.processmining.scala.log.common.csv.spark.CsvWriter
import org.processmining.scala.log.common.enhancment.segments.common.InventoryAggregation
import org.processmining.scala.log.common.enhancment.segments.spark.{DurationSegmentProcessor, SegmentProcessor}

private object SpectrumMinerReport extends TemplateForBpi2017CaseStudy {

  def main(args: Array[String]): Unit = {


    segmentDf.cache()
    statDf.cache()

    // Writing the complete segment log to disk
    CsvWriter.dataframeToLocalCsv(segmentDf, caseStudyConfig.outDir + "segments.csv")

    //Joining descriptive stat with capacity (here we just add a dummy field, to provide compatibility with older versions)
    CsvWriter.dataframeToLocalCsv(spark.sql("""SELECT stat.*, 0 as capacity FROM stat""")
      .toDF("key", "median", "mean", "std", "capacity"),
      caseStudyConfig.outDir + "/segmentsStatisticsCapacity.csv")

    val segLoadTime = new Date(); // for time measuring

    SegmentProcessor.toCsvV2(
      durationProcessor,
      caseStudyConfig.outDir + "/duration/",
      durationProcessor.adc.legend)
      //.unpersist()
    segmentDf.unpersist()
    statDf.unpersist()

    val durationTime = new Date(); // for time measuring

    SegmentProcessor.toCsvV2(offeredAmountClazzBasedSegmentProcessor,
      caseStudyConfig.outDir + "/offeredAmount/",
      "Offered Amount%0-5000%5000-10000%10000-15000%15000-20000%20000-25000%25000-30000%30000-35000%35000-45000%45000+")
      //.unpersist()

    val totalTime = new Date(); // for time measuring
    println(s"Loading and segments constructing took ${(segLoadTime.getTime - appStartTime.getTime) / 1000} seconds.")
    println(s"Duration analysis took ${(durationTime.getTime - segLoadTime.getTime) / 1000} seconds.")
    println(s"Offers analysis took ${(totalTime.getTime - durationTime.getTime) / 1000} seconds.")
    println(s"Pre-processing took ${(totalTime.getTime - appStartTime.getTime) / 1000} seconds.")
  }
}
