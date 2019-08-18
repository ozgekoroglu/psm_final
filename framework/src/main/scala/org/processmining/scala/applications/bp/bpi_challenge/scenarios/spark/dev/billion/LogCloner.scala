package org.processmining.scala.applications.bp.bpi_challenge.scenarios.spark.dev.billion

import java.time.Duration
import java.util.Date

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{ByteType, StringType, StructField, StructType}
import org.apache.spark.storage.StorageLevel
import org.processmining.scala.applications.bp.bpi_challenge.types.OfferEvent
import org.processmining.scala.log.common.enhancment.segments.spark.SegmentUtils
import org.processmining.scala.log.common.unified.event.{UnifiedEvent, UnifiedEventImpl}
import org.processmining.scala.log.common.unified.log.spark.UnifiedEventLog
import org.processmining.scala.log.common.unified.trace.{UnifiedTraceId, UnifiedTraceIdImpl}

import scala.collection.immutable.SortedMap
import scala.util.Random

private object LogCloner extends TemplateForBpi2017CaseStudy {
  //val scale = 1
  val daysDelta = 30
  val maxAmount = 50000

  val SourceAttributesSchema = new StructType(Array(StructField("offeredAmount", StringType, false), StructField("offeredAmountClass", ByteType, false)))

  val activityMap: Map[String, Int] = srcLogOffers
    .events()
    .map(_._2.activity)
    .distinct
    .zipWithIndex
    .map(x => x._1 -> x._2)
    .toMap
    .seq

  def generateEvent(e: UnifiedEvent, delta: Long, amount: Int, amountClazz: Byte): UnifiedEvent =
    UnifiedEvent(e.timestamp + delta,
      activityMap(e.activity).toString,
      SortedMap("offeredAmount" -> (amount.toString + ".0"), "offeredAmountClass" -> amountClazz),
      None)

  def getDelta(r: Random): Long =
    (r.nextInt(Duration.ofDays(daysDelta).toHours.toInt) * (if (r.nextInt(2) == 0) 1 else -1)).toLong * 3600000L

  def generateAndSaveTraces(scale: Int, id: Long, events: List[UnifiedEvent], index: Long): IndexedSeq[(UnifiedTraceId, List[UnifiedEvent])] = {
    val r = Random
    r.setSeed(id)
    (0 until scale)
      .map((_, r.nextInt(maxAmount)))
      .map(pair => (new UnifiedTraceIdImpl(s"${id}_${pair._1}"),
        events.map(generateEvent(_, getDelta(r), pair._2, OfferEvent.offeredAmountClassifier(pair._2.toDouble)))))
      .map(SegmentUtils.addSegments(":", _))
  }

  def createLog(scale: Int, partitionNumber: Int): (UnifiedEventLog, Date) = {
    val tr = spark.sparkContext.parallelize(srcLogOffers
      .traces()
      .zipWithIndex
      .map(t => (t._2, t._1._2))
      .zipWithIndex
      .seq)
      .repartition(partitionNumber)
      .persist(StorageLevel.MEMORY_ONLY)
    val log = UnifiedEventLog.fromTraces(tr.flatMap(t => generateAndSaveTraces(scale, t._1._1, t._1._2, t._2)))
    (log, new Date())
  }
}
