package org.processmining.scala.applications.bp.bpi_challenge.scenarios.parallel

import java.util.Date

import org.processmining.scala.applications.bp.bpi_challenge.types.ApplicationEvent
import org.processmining.scala.log.common.enhancment.segments.common.InventoryAggregation
import org.processmining.scala.log.common.enhancment.segments.parallel.{ClazzBasedSegmentProcessor, DurationSegmentProcessor, SegmentProcessor, SegmentProcessorConfig}
import org.processmining.scala.log.common.unified.event.UnifiedEvent

private object BPI17_SpectrumMinerReport_Application extends TemplateForBpi2017CaseStudyApplication {
  def main(args: Array[String]): Unit = {

    val processorConfigApplications = SegmentProcessorConfig(logApplicationsDfrEventsWithArtificialStartsStops,
      caseStudyConfig.timestamp1Ms,
      caseStudyConfig.timestamp2Ms,
      caseStudyConfig.twSize)

    //Map offered amounts to DFR events
    val offeredAmountClazzBasedSegmentProcessor = new ClazzBasedSegmentProcessor[(String, UnifiedEvent)](
      processorConfigApplications,
      ApplicationEvent.ClazzCount,
      eventsApplication
        .filter(_._2.getAs[Byte]("offeredAmountClass") != 0)
        .distinct
        .toList,
      _._1,
      _._2.timestamp,
      _._2.getAs[Byte]("offeredAmountClass")
    )
    val (segments, stat, durationProcessor) = DurationSegmentProcessor(processorConfigApplications)

    SegmentProcessor.toCsvV2(
      durationProcessor,
      s"${caseStudyConfig.outDir}/duration_all/",
      durationProcessor.adc.legend)

    SegmentProcessor.toCsvV2(
      offeredAmountClazzBasedSegmentProcessor,
      caseStudyConfig.outDir + "/offeredAmount_all/",
      "Offered Amount%0-5000%5000-10000%10000-15000%15000-20000%20000-25000%25000-30000%30000-35000%35000-45000%45000+")
    println(s"Pre-processing took ${(new Date().getTime - appStartTime.getTime) / 1000} seconds.")
  }
}
