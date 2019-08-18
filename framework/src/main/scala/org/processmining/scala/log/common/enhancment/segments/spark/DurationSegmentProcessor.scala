package org.processmining.scala.log.common.enhancment.segments.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.processmining.scala.log.common.enhancment.segments.common.{AbstractDurationClassifier, FasterNormal23VerySlowDurationClassifier}
import org.processmining.scala.log.common.types.SegmentWithClazz
import org.processmining.scala.log.common.unified.event.CommonAttributeSchemas

/**
  * Classifies durations of segments using some hardcoded parameters (see SQL expression)
  *
  * @param config standard config of Segment Processors
  */
final class DurationSegmentProcessor(config: SegmentProcessorConfig, val adc: AbstractDurationClassifier) extends SegmentProcessor(config, 5) {

  def createClassifiedSegmentsDf(): DataFrame =
    config.spark.sql(adc.sparkSqlExpression(CommonAttributeSchemas.AttrNameDuration, CommonAttributeSchemas.AttrNameClazz))

  override def getVisualizationDatasetDf(): DataFrame =
    defaultPreProcessingForVisualization(createClassifiedSegmentsDf())

  override def getClassifiedSegments(): RDD[SegmentWithClazz] =
    SegmentProcessor.dfToRdd(createClassifiedSegmentsDf())
}

object DurationSegmentProcessor {

  /**
    * Builder
    * @param config typical config params
    * @param percentile percentile value for classification
    * @return triple (dataframe of classified segments, descriptive statistics, processor)
    */
  def apply(config: SegmentProcessorConfig,  percentile: Double, adc: AbstractDurationClassifier = new FasterNormal23VerySlowDurationClassifier())
  : (DataFrame, DataFrame, DurationSegmentProcessor) = {
    val segmentsDataframe = SegmentProcessor.createSegmentDataframe(config.segments, config.spark)
    segmentsDataframe.createOrReplaceTempView("segments") // registering segments 'table'
    val statDataFrame = SegmentUtils.getDescriptiveStatistics("segments", percentile, config.spark)
    statDataFrame.createOrReplaceTempView("stat") // registering stat 'table'
    (segmentsDataframe, statDataFrame, new DurationSegmentProcessor(config, adc))

  }
}
