package org.processmining.scala.log.common.enhancment.segments.spark

import org.apache.spark.rdd.RDD
import org.processmining.scala.log.common.enhancment.segments.common.NodesProcessorHelper2
import org.processmining.scala.log.common.types._

/**
  * The class classifies both normal segments and node segments that represent paths from a node to everywhere (or nowhere).
  * For example, failed direction errors can be processed by this class
  * The node segments have the following names: "Activity:" (nothing after ':')
  * It can be used in 2 ways: 1) segment processor 2) node processor
  * 1) Segment processor mode
  * * in config.segments 'normal' segments (like movement) are provided
  * * in attributeRdd attributes to be mapped are provided (like error codes)
  * LIMITATION: start times of segment must be equal to timestamps of attributes.
  * Otherwise they will not be mapped
  *
  * 2) Node processor mode
  * * in config.segments 'node' segments (like movement but just first parts of names, before ':') are provided
  * The rest see in p.1
  *
  * @param config       standard config of Segment Processors
  * @param attributeRdd events to be mapped
  */
class NodesProcessor[T](config: SegmentProcessorConfig,
                        classCount: Int,
                        attributeRdd: RDD[T],
                        id: T => String,
                        timestamp: T => Long,
                        clazz: T => Int
                       )
  extends SegmentProcessor(config, classCount) {

  def getClassifiedSegments(): RDD[SegmentWithClazz] = {
    SegmentProcessor.getTracesWithSegments(config.segments)
      .leftOuterJoin(attributeRdd.groupBy(id(_)))
      .values
      .flatMap { x => NodesProcessorHelper2.addAttribute[T](x._1, x._2, id, timestamp, clazz) }
  }


  override def getVisualizationDatasetDf() = {
    import config.spark.implicits._
    defaultPreProcessingForVisualization(getClassifiedSegments().toDF())
  }
}


