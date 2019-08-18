package org.processmining.scala.log.common.enhancment.segments.common

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.processmining.scala.log.common.types.Segment
import org.processmining.scala.log.common.unified.event.{CommonAttributeSchemas, UnifiedEvent}
import org.processmining.scala.log.common.unified.log.parallel.UnifiedEventLog

import scala.collection.immutable.SortedMap
import scala.collection.parallel.ParSeq

object TestExcelExport {


  def createEventsFromExcelExport(excelCopyPaste: String, firstEventTimestamp: Int => Long): ParSeq[Segment] = {

    val lines = excelCopyPaste
      .replace("|", "")
      .split(";")
      .filter(_.nonEmpty)
      .zipWithIndex

    val header = lines.filter(_._2 == 0).head._1.split("\\s").filter(_.nonEmpty).tail

    lines
      .filter(_._2 != 0)
      .map(_._1)
      .filter(_.replaceAll("\\s+", "").nonEmpty)
      .map(x => {

        val cells = x
          .split("\\s")
          .filter(_.nonEmpty)

        //println(cells.mkString("<", "; ", ">"))
        val id = cells.head
        val startTimestamp = firstEventTimestamp(id.toInt)
        val initialValue: (List[Segment], Long, Int) = (List[Segment](), startTimestamp, 0)
        cells
          .tail
          .foldLeft(initialValue)(
            (z: (List[Segment], Long, Int),
             e: String
            )
            => (Segment(id, header(z._3), z._2, e.toLong) :: z._1, z._2 + e.toLong, z._3 + 1)
          )
      }).flatMap(_._1).par

  }


  def createSegments(excelCopyPaste: String, firstEventTimestamp: Int => Long): ParSeq[(String, UnifiedEvent)] =
    createEventsFromExcelExport(excelCopyPaste, firstEventTimestamp)
      .map(x => (x.id, UnifiedEvent(
        x.timestamp,
        x.key,
        SortedMap(CommonAttributeSchemas.AttrNameDuration -> x.duration),
        None
      )
      ))


  def createParallelLog(excelCopyPaste: String, firstEventTimestamp: Int => Long): UnifiedEventLog =
    UnifiedEventLog.create(createSegments(excelCopyPaste, firstEventTimestamp))

  def createSparkLog(spark: SparkSession, excelCopyPaste: String, firstEventTimestamp: Int => Long): org.processmining.scala.log.common.unified.log.spark.UnifiedEventLog =
    org.processmining.scala.log.common.unified.log.spark.UnifiedEventLog.create(
      spark.sparkContext.parallelize(createSegments(excelCopyPaste, firstEventTimestamp).seq))


}
