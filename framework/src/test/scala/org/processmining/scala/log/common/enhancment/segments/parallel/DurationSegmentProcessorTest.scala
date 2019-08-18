package org.processmining.scala.log.common.enhancment.segments.parallel

import org.processmining.scala.log.common.enhancment.segments.common.TestExcelExport
import org.processmining.scala.log.common.enhancment.segments.spark.DurationSegmentProcessorTestData
import org.processmining.scala.log.common.types.Segment
import org.processmining.scala.log.common.unified.log.parallel.UnifiedEventLog
import org.scalactic.TolerantNumerics
import org.scalatest.FunSuite

class DurationSegmentProcessorTest extends FunSuite {

  test("testStat") {
    val startTimestamp = 0L
    val endTimestamp = 100000L
    val twSize = 1
    val log = TestExcelExport.createParallelLog(DurationSegmentProcessorTestData.Sample1, startTimestamp + _ * 10)
//    val SegmentType = "D"
//    val log = UnifiedEventLog.create[Segment](segmentEvents, SegmentType, _.id, _.timestamp, _.key, Segment.regexpableForm(SegmentType, _: Segment))

    val processorConfig = SegmentProcessorConfig(
      log,
      startTimestamp,
      endTimestamp,
      twSize)

    val (segmentDf, stat, durationProcessor) =
      DurationSegmentProcessor(processorConfig)

    implicit val doubleEq = TolerantNumerics.tolerantDoubleEquality(1e-4f)
    assert(stat("A:B").median === 70.0)
    assert(stat("A:B").mean === 116.6666667)
    assert(stat("A:B").stdev === 136.1022025)

    assert(stat("B:C").median === 150.0)
    assert(stat("B:C").mean === 947.3333333)
    assert(stat("B:C").stdev === 2538.496368)


    assert(stat("C:D").median === 110.0)
    assert(stat("C:D").mean === 4158.666667)
    assert(stat("C:D").stdev === 12822.7686)

  }


  test("testClassification") {
    val startTimestamp = 0L
    val endTimestamp = 100000L
    val twSize = 1
    val log = TestExcelExport.createParallelLog(DurationSegmentProcessorTestData.Sample1, startTimestamp + _ * 10)

    val processorConfig = SegmentProcessorConfig(
      log,
          startTimestamp,
      endTimestamp,
      twSize)

    val (segmentDf, stat, durationProcessor) =
      DurationSegmentProcessor(processorConfig)

    val s = durationProcessor.getClassifiedSegments()

      .map(x => (x.id, x.key) -> x.clazz)
      .toMap

    assert(s(("0", "A:B")) == 0)
    assert(s(("1", "A:B")) == 0)
    assert(s(("2", "A:B")) == 0)
    assert(s(("3", "A:B")) == 0)
    assert(s(("4", "A:B")) == 0)
    assert(s(("5", "A:B")) == 0)
    assert(s(("6", "A:B")) == 0)
    assert(s(("7", "A:B")) == 0)
    assert(s(("8", "A:B")) == 0)
    assert(s(("9", "A:B")) == 0)
    assert(s(("10", "A:B")) == 0)
    assert(s(("11", "A:B")) == 2)
    assert(s(("12", "A:B")) == 3)
    assert(s(("13", "A:B")) == 4)
    assert(s(("14", "A:B")) == 4)


    assert(s(("0", "B:C")) == 0)
    assert(s(("1", "B:C")) == 0)
    assert(s(("2", "B:C")) == 0)
    assert(s(("3", "B:C")) == 0)
    assert(s(("4", "B:C")) == 0)
    assert(s(("5", "B:C")) == 0)
    assert(s(("6", "B:C")) == 0)
    assert(s(("7", "B:C")) == 0)
    assert(s(("8", "B:C")) == 0)
    assert(s(("9", "B:C")) == 0)
    assert(s(("10", "B:C")) == 0)
    assert(s(("11", "B:C")) == 3)
    assert(s(("12", "B:C")) == 4)
    assert(s(("13", "B:C")) == 4)
    assert(s(("14", "B:C")) == 4)


    assert(s(("0", "C:D")) == 0)
    assert(s(("1", "C:D")) == 0)
    assert(s(("2", "C:D")) == 0)
    assert(s(("3", "C:D")) == 0)
    assert(s(("4", "C:D")) == 0)
    assert(s(("5", "C:D")) == 0)
    assert(s(("6", "C:D")) == 0)
    assert(s(("7", "C:D")) == 0)
    assert(s(("8", "C:D")) == 0)
    assert(s(("9", "C:D")) == 0)
    assert(s(("10", "C:D")) == 0)
    assert(s(("11", "C:D")) == 4)
    assert(s(("12", "C:D")) == 4)
    assert(s(("13", "C:D")) == 4)
    assert(s(("14", "C:D")) == 4)

  }

  test("testVisualization") {
    val startTimestamp = 0L
    val endTimestamp = 1000L
    val twSize = 250
    val log = TestExcelExport.createParallelLog(DurationSegmentProcessorTestData.Sample1, startTimestamp + _ * 10)

    val processorConfig = SegmentProcessorConfig(
      log,
      startTimestamp,
      endTimestamp,
      twSize)

    val (segmentDf, stat, durationProcessor) =
      DurationSegmentProcessor(processorConfig)

    val s = durationProcessor.getVisualizationDataset(_ => ())._1
      .map(x => (x.index, x.key, x.clazz) -> (x.clazz, x.count))
      .toMap

    assert(s((0, "A:B", 2)) == (2, 1))
    assert(s((0, "A:B", 4)) == (4, 2))
    assert(s((0, "A:B", 3)) == (3, 1))
    assert(s((0, "A:B", 0)) == (0, 10))
    assert(s((1, "A:B", 3)) == (3, 1))
    assert(s((1, "A:B", 4)) == (4, 2))
    assert(s((1, "A:B", 2)) == (2, 1))
    assert(s((2, "A:B", 4)) == (4, 1))


    assert(s((0, "B:C", 0)) == (0, 11))
    assert(s((1, "B:C", 0)) == (0, 4))
    assert(s((1, "B:C", 3)) == (3, 1))
    assert(s((1, "B:C", 4)) == (4, 2))
    assert(s((2, "B:C", 4)) == (4, 3))
    assert(s((2, "B:C", 3)) == (3, 1))
    assert(s((3, "B:C", 3)) == (3, 1))
    assert(s((3, "B:C", 4)) == (4, 3))

    assert(s((0, "C:D", 0)) == (0, 6))
    assert(s((1, "C:D", 0)) == (0, 6))
    assert(s((2, "C:D", 0)) == (0, 2))
    assert(s((3, "C:D", 4)) == (4, 1))


    val max = SegmentProcessor.getMax(durationProcessor.getVisualizationDataset(_ => ())._1)
      .map(r => r._1 -> r._2)

    assert( max("A:B") == 14)
    assert( max("B:C") == 11)
    assert( max("C:D") == 6)
  }
}
