package org.processmining.scala.log.common.enhancment.segments.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.processmining.scala.log.common.enhancment.segments.common.SegmentDescriptiveStatistics
import org.processmining.scala.log.common.types._
import org.processmining.scala.log.common.unified.event.{CommonAttributeSchemas, UnifiedEvent}
import org.processmining.scala.log.common.unified.trace.UnifiedTraceId

import scala.collection.immutable.SortedMap

/**
  * Created by nlvden on 30-06-2017.
  */
object SegmentUtils {

  val DefaultSeparator = org.processmining.scala.log.common.enhancment.segments.common.SegmentUtils.DefaultSeparator



  private def createEFSegmentsForEvent(t: List[UnifiedEvent]): List[List[(UnifiedEvent, UnifiedEvent)]] =
    t match {
      case Nil => Nil
      case _ :: Nil => Nil
      case x :: xs => xs.map((x, _)) :: createEFSegmentsForEvent(xs)
    }


  def convertToEFSegments(sep: String, tracePair: (UnifiedTraceId, List[UnifiedEvent])): (UnifiedTraceId, List[UnifiedEvent]) =
    tracePair match {
      case (unifiedTrace, unifiedEvents) =>
        (unifiedTrace, createEFSegmentsForEvent(unifiedEvents)
          .flatten
          .map(s => Segment(unifiedTrace.id,
            s._1.activity + sep + s._2.activity,
            s._1.timestamp,
            s._2.timestamp - s._1.timestamp))
          .map(s => UnifiedEvent(
            s.timestamp,
            s.key,
            SortedMap(CommonAttributeSchemas.AttrNameDuration -> s.duration),
            None)
          )
          .reverse)
    }


  def convertToOneActivitySegments(sep: String, tracePair: (UnifiedTraceId, List[UnifiedEvent])): (UnifiedTraceId, List[UnifiedEvent]) =
    tracePair match {
      case (unifiedTrace, unifiedEvents) =>
        (unifiedTrace, unifiedEvents
          .map(s => Segment(unifiedTrace.id,
            s.activity + sep,
            s.timestamp,
            0))
          .map(s => UnifiedEvent(
            s.timestamp,
            s.key,
            SortedMap(CommonAttributeSchemas.AttrNameDuration -> s.duration),
            None)
          )
        )
    }


  def addSegments(sep: String, tracePair: (UnifiedTraceId, List[UnifiedEvent])): (UnifiedTraceId, List[UnifiedEvent]) = {
    val (_, segments) = org.processmining.scala.log.common.enhancment.segments.common.SegmentUtils.convertToSegments(sep, tracePair)
    (tracePair._1, (tracePair._2 ::: segments).sortBy(_.timestamp))
  }

  // Creating Segments from traces
  //  def traces2segments[E](
  //                          traces: RDD[List[E]],
  //                          id: E => String,
  //                          timestamp: E => Long,
  //                          activity: E => String,
  //                          separator: String = DefaultSeparator)
  //  : RDD[Segment] =
  //    traces
  //      .map(
  //        // converting each trace into list of segments (E, E)
  //        _./:((List[(E, E)](), None: Option[E]))((l: (List[(E, E)], Option[E]), e: E) =>
  //          if (l._2.isEmpty) (l._1, Some(e)) else ((l._2.get, e) :: l._1, Some(e)))
  //          ._1
  //
  //      ).flatMap(l => l) // getting RDD of (E, E)
  //      .map(s => Segment(id(s._1), activity(s._1) + separator + activity(s._2), timestamp(s._1), timestamp(s._2) - timestamp(s._1))) // converting to Segment case class


  //  @deprecated
  //  def findCapacity(segments: RDD[Segment], spark: SparkSession): DataFrame = {
  //    import spark.implicits._
  //    segments
  //      .map(s => (s.key, s)) // pair RDD of segments
  //      .groupByKey() // segments grouped by key
  //      .mapValues {
  //      _.flatMap { s => (s.timestamp, 1) :: (s.timestamp + s.duration, -1) :: Nil }
  //        .toList
  //        .sortBy(_._1) // sort by time
  //        .map(_._2)
  //        ./:((0, 0)) { (z, f) => {
  //          val sum = z._2 + f
  //          (if (z._1 < sum) sum else z._1, sum)
  //        }
  //        }
  //    }
  //      .mapValues(_._1)
  //      .toDF("key", "capacity")
  //  }


  //TODO: rename median into percentile
  /**
    *
    * @param segmantTable   name of a table with segment events
    * @param percentileRate value between 0 and 1, e.g. 0.5 for getting medians
    * @param spark
    * @return
    */
  def getDescriptiveStatistics(segmantTable: String, percentileRate: Double, spark: SparkSession): DataFrame =
    spark.sql(
      s"""SELECT key,
         | PERCENTILE_APPROX(${CommonAttributeSchemas.AttrNameDuration}, 0.25) as q2,
         | PERCENTILE_APPROX(${CommonAttributeSchemas.AttrNameDuration}, 0.5) as median,
         | PERCENTILE_APPROX(${CommonAttributeSchemas.AttrNameDuration}, 0.75) as q4,
         | MEAN(${CommonAttributeSchemas.AttrNameDuration}) as mean,
         | STDDEV(${CommonAttributeSchemas.AttrNameDuration}) as std
         | FROM $segmantTable GROUP BY key """.stripMargin)

  def descriptiveStatisticsDfToRdd(df: DataFrame): RDD[SegmentDescriptiveStatistics] = {
    df
      .rdd
      .map { x =>
        SegmentDescriptiveStatistics(
          x.getAs[String]("key"),
          x.getAs[Double]("q2"),
          x.getAs[Double]("median"),
          x.getAs[Double]("q4"),
          x.getAs[Double]("mean"),
          x.getAs[Double]("std")
        )
      }
  }


}

