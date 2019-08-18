package org.processmining.scala.log.common.enhancment.segments.spark

import org.apache.spark.sql.DataFrame

/**
  * Binary classifier: marks all segment of a case if the case ID is contained in a provided dataset
  * @param config standard config of Segment Processors
  * @param ids dataset of IDs (the field name must be 'id')
  */
final class IdBasedSegmentProcessor(config: SegmentProcessorConfig, ids: DataFrame)
  extends SegmentProcessor(config, 2) {

  def segmentsDf() : DataFrame = {
    ids.createOrReplaceTempView("rec")
    config.spark.sql(
      s"""SELECT segments.*,
         | CASE WHEN rec.id IS NULL THEN 0 ELSE 1 END AS clazz
         | FROM segments
         | LEFT JOIN rec ON rec.id = segments.id""".stripMargin)
  }


  override def getVisualizationDatasetDf() =  defaultPreProcessingForVisualization(segmentsDf())

  override def getClassifiedSegments() = SegmentProcessor.dfToRdd(segmentsDf())
}


object IdBasedSegmentProcessor{
  def apply(config: SegmentProcessorConfig, segmentClass: String, ids: DataFrame) = {
    val segmentsDataframe = SegmentProcessor.createSegmentDataframe(config.segments, config.spark)
    segmentsDataframe.createOrReplaceTempView("segments") // registering segments 'table'
    (segmentsDataframe, new IdBasedSegmentProcessor(config, ids))
  }
}
