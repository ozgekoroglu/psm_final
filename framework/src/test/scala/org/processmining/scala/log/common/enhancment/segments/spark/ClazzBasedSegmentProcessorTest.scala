package org.processmining.scala.log.common.enhancment.segments.spark

import org.apache.spark.sql.SparkSession
import org.processmining.scala.log.common.enhancment.segments.common.TestExcelExport
import org.processmining.scala.log.common.utils.common.types.EventWithClazz
import org.scalatest.FunSuite

class ClazzBasedSegmentProcessorTest extends FunSuite {

  @transient
  protected val spark: SparkSession =
    SparkSession
      .builder()
      .appName("DurationSegmentProcessorTest")
      .config("spark.master", "local[*]")
      .getOrCreate()

  @transient
  protected val sc = spark.sparkContext

  val startTimestamp = 0L
  val endTimestamp = 1000L
  val twSize = 250
  val percentile = 0.5
  val log = TestExcelExport.createSparkLog(spark, DurationSegmentProcessorTestData.Sample1, x => startTimestamp)

  test("testGetClassifiedSegments_NoMatching") {
    val SegmentType = "D"
    val attributes = EventWithClazz("1", 1000000, 5) :: Nil

    val config = SegmentProcessorConfig(spark,
      log,
      startTimestamp,
      endTimestamp,
      twSize)

    val proc = new ClazzBasedSegmentProcessor[EventWithClazz](config, 10, spark.sparkContext.parallelize(attributes), _.id, _.timestamp, _.clazz)
    val segments = proc
      .getClassifiedSegments()
      .collect()
      .map(x => (x.id, x.key) -> x.clazz)
      .toMap


    assert(segments(("1", "A:B")) == 0)
    assert(segments(("1", "B:C")) == 0)
    assert(segments(("1", "C:D")) == 0)
  }

  test("testGetClassifiedSegments_MatchesBeforeTheBeginning") {
    val attributes = EventWithClazz("1", -1, 5) :: Nil
    val config = SegmentProcessorConfig(spark,
      log,
      startTimestamp,
      endTimestamp,
      twSize)

    val proc = new ClazzBasedSegmentProcessor[EventWithClazz](config, 10, spark.sparkContext.parallelize(attributes), _.id, _.timestamp, _.clazz)
    val segments = proc
      .getClassifiedSegments()
      .collect()
      .map(x => (x.id, x.key) -> x.clazz)
      .toMap

    assert(segments(("1", "A:B")) == 5)
    assert(segments(("1", "B:C")) == 5)
    assert(segments(("1", "C:D")) == 5)

    assert(segments(("2", "A:B")) == 0)
    assert(segments(("2", "B:C")) == 0)
    assert(segments(("2", "C:D")) == 0)
  }

  test("testGetClassifiedSegments_MatchesFromTheBeginning") {
    val attributes = EventWithClazz("1", 0, 5) :: Nil
    val config = SegmentProcessorConfig(spark,
      log,
      startTimestamp,
      endTimestamp,
      twSize)

    val proc = new ClazzBasedSegmentProcessor[EventWithClazz](config, 10, spark.sparkContext.parallelize(attributes), _.id, _.timestamp, _.clazz)
    val segments = proc
      .getClassifiedSegments()
      .collect()
      .map(x => (x.id, x.key) -> x.clazz)
      .toMap

    assert(segments(("1", "A:B")) == 5)
    assert(segments(("1", "B:C")) == 5)
    assert(segments(("1", "C:D")) == 5)

    assert(segments(("2", "A:B")) == 0)
    assert(segments(("2", "B:C")) == 0)
    assert(segments(("2", "C:D")) == 0)
  }

  test("testGetClassifiedSegments_MatchesFromTheEnd") {
    val attributes = EventWithClazz("1", 50, 5) :: Nil
    val config = SegmentProcessorConfig(spark,
      log,
      startTimestamp,
      endTimestamp,
      twSize)

    val proc = new ClazzBasedSegmentProcessor[EventWithClazz](config, 10, spark.sparkContext.parallelize(attributes), _.id, _.timestamp, _.clazz)
    val segments = proc
      .getClassifiedSegments()
      .collect()
      .map(x => (x.id, x.key) -> x.clazz)
      .toMap

    assert(segments(("1", "A:B")) == 0)
    assert(segments(("1", "B:C")) == 0)
    assert(segments(("1", "C:D")) == 5)

    assert(segments(("2", "A:B")) == 0)
    assert(segments(("2", "B:C")) == 0)
    assert(segments(("2", "C:D")) == 0)
  }

}
