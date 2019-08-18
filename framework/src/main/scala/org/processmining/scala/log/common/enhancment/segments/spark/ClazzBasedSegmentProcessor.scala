package org.processmining.scala.log.common.enhancment.segments.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.processmining.scala.log.common.enhancment.segments.common.ClazzBasedSegmentProcessorHelper2
import org.processmining.scala.log.common.types.{Clazz, Segment, SegmentWithClazz, Timestamp}

// Should be re-designed for functional approach
final class ClazzBasedSegmentProcessor[T](config: SegmentProcessorConfig,
                                          classCount: Int,
                                          attributeRdd: RDD[T],
                                          id: T => String,
                                          timestamp: T => Long,
                                          clazz: T => Int
                                         ) extends SegmentProcessor(config, classCount) {


  def getClassifiedSegments(): RDD[SegmentWithClazz] = {

    SegmentProcessor.getTracesWithSegments(config.segments)
      .leftOuterJoin(attributeRdd.groupBy(id(_)))
      .values
      .flatMap { x => ClazzBasedSegmentProcessorHelper2.addAttribute(x._1.sortBy(_.timestamp), x._2, id, timestamp, clazz) }
  }

  override def getVisualizationDatasetDf(): DataFrame = {
    import config.spark.implicits._
    defaultPreProcessingForVisualization(getClassifiedSegments().toDF())
  }
}


