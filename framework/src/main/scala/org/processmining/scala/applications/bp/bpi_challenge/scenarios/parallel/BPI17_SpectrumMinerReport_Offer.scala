package org.processmining.scala.applications.bp.bpi_challenge.scenarios.parallel

import java.util.Date

import org.processmining.scala.log.common.enhancment.segments.common.InventoryAggregation
import org.processmining.scala.log.common.enhancment.segments.parallel.SegmentProcessor

private object BPI17_SpectrumMinerReport_Offer extends TemplateForBpi2017CaseStudy {
  def main(args: Array[String]): Unit = {
    SegmentProcessor.toCsvV2(
      durationProcessor,
      s"${caseStudyConfig.outDir}/duration/",
      durationProcessor.adc.legend)

    SegmentProcessor.toCsvV2(offeredAmountClazzBasedSegmentProcessor,
      caseStudyConfig.outDir + "/offeredAmount/",
      "Offered Amount%0-5000%5000-10000%10000-15000%15000-20000%20000-25000%25000-30000%30000-35000%35000-45000%45000+")
    println(s"Pre-processing took ${(new Date().getTime - appStartTime.getTime) / 1000} seconds.")
  }

}
