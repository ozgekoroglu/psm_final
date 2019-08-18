package org.processmining.scala.log.common.enhancment.segments.spark

import java.io.File

import org.apache.spark.rdd.RDD
import org.processmining.scala.log.common.csv.common.CsvWriter
import org.processmining.scala.log.common.enhancment.segments.parallel.SegmentVisualizationItem
import org.processmining.scala.log.common.types.SegmentWithClazz

object DataExporter {
  def toFile(groupedSegTw: Iterable[SegmentVisualizationItem], filename: String): Unit =
    CsvWriter.eventsToCsvLocalFilesystem[SegmentVisualizationItem](groupedSegTw, SegmentVisualizationItem.csvHeader, _.toCsv, filename)

  def toDirs(groupedSegTw: RDD[SegmentVisualizationItem], dirName: String): Unit = {
    new File(dirName).mkdirs()
    groupedSegTw
      .groupBy(_.index)
      .foreach(x => {
        new File(s"$dirName/${x._1}").mkdir()
        toFile(x._2, s"$dirName/${x._1}/${org.processmining.scala.log.common.enhancment.segments.parallel.SegmentProcessor.ProcessedDataFileName}")
      })
  }


  // Directories structure: twIndex -> Clazz
  def segmentsToDirs(dirName: String, segments: RDD[(Long, SegmentWithClazz)]): Unit = {
    new File(dirName).mkdirs()
    val mapping = segments
      .groupBy(_._1)
      .map { x => (x._1, x._2.map(_._2)) }
    mapping.foreach { twLevel => {
      val twDirName = s"$dirName/${twLevel._1}"
      new File(twDirName).mkdir()
      CsvWriter.eventsToCsvLocalFilesystem[SegmentWithClazz](
        twLevel._2.seq, SegmentWithClazz.CsvHeader, _.toCsv(_.toString), s"$twDirName/${org.processmining.scala.log.common.enhancment.segments.parallel.SegmentProcessor.SegmentsFileName}")
    }
    }
  }

}
