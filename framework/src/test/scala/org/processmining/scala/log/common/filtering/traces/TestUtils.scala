package org.processmining.scala.log.common.filtering.traces

import org.processmining.scala.log.common.unified.event.UnifiedEvent
import org.processmining.scala.log.common.unified.log.common.UnifiedEventLog.UnifiedTrace
import org.processmining.scala.log.common.unified.trace.UnifiedTraceIdImpl
import org.processmining.scala.log.common.utils.common.types.Event

import scala.collection.immutable.SortedMap

object TestUtils {


//  val EventSchema = new StructType(Array(
//    StructField("index", IntegerType, false)
//  ))

  private def mapper(e: Event): UnifiedEvent = UnifiedEvent(e.timestamp, e.activity)

  private def mapper(e: Event, index: Int): UnifiedEvent =
    UnifiedEvent(e.timestamp,
      e.activity,
      SortedMap("index" -> index),
      None)


  def create(string: String, id: String): UnifiedTrace =
    (new UnifiedTraceIdImpl(id), string
      .zipWithIndex
      .map(x => Event(id, x._1.toString, x._2))
      .map(mapper)
      .toList)

  def createWithIndexAttr(string: String, id: String): UnifiedTrace =
    (new UnifiedTraceIdImpl(id), string
      .zipWithIndex
      .map(x => (Event(id, x._1.toString, x._2), x._2))
      .map(x =>  mapper(x._1, x._2))
      .toList)


  def create(list: List[(String, Int)], id: String): UnifiedTrace =
    (new UnifiedTraceIdImpl(id), list
      .map(x => Event(id, x._1, x._2))
      .map(mapper)
      .sortBy(_.timestamp)
    )

  def trace2String(t: UnifiedTrace): String =
    t
      ._2
      .map(_.activity)
      .mkString("")

  //  def trace2String(t: UnifiedTrace): String =
  //    t._2.map(_.event.asInstanceOf[Event])
  //      .map(_.activity)
  //      .mkString("")


}