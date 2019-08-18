package org.processmining.scala.log.common.enhancment.segments.spark

import org.apache.spark.sql.SparkSession
import org.processmining.scala.log.common.enhancment.segments.common.TestExcelExport
import org.processmining.scala.log.common.types.Segment
import org.processmining.scala.log.common.unified.log.spark.UnifiedEventLog
import org.scalactic.TolerantNumerics
import org.scalatest.FunSuite

class DurationSegmentProcessorTest extends FunSuite {

  // Spark initialization
  @transient
  protected val spark: SparkSession =
  SparkSession
    .builder()
    .appName("DurationSegmentProcessorTest")
    .config("spark.master", "local[*]")
    //.config("spark.master", "spark://131.155.68.43:7077")
    .getOrCreate()

  @transient
  protected val sc = spark.sparkContext


  test("testStat") {
    val startTimestamp = 0L
    val endTimestamp = 100000L
    val twSize = 1
    val percentile = 0.5
    val log = TestExcelExport.createSparkLog(spark,DurationSegmentProcessorTestData.Sample1, startTimestamp + _ * 10)


    val processorConfig = SegmentProcessorConfig(spark,
      log,
      startTimestamp,
      endTimestamp,
      twSize)

    val (segmentDf, statDf, durationProcessor) =
      DurationSegmentProcessor(processorConfig, percentile)

    val stat = SegmentUtils.descriptiveStatisticsDfToRdd(statDf)
      .collect()
      .map(x => x.key -> x)
      .toMap

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
    val percentile = 0.5
    val log = TestExcelExport.createSparkLog(spark,DurationSegmentProcessorTestData.Sample1, startTimestamp + _ * 10)



    val processorConfig = SegmentProcessorConfig(spark,
      log,
      startTimestamp,
      endTimestamp,
      twSize)

    val (segmentDf, statDf, durationProcessor) =
      DurationSegmentProcessor(processorConfig, percentile)


    val s = durationProcessor.getClassifiedSegments()
      .collect()
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
    val percentile = 0.5
    val log = TestExcelExport.createSparkLog(spark,DurationSegmentProcessorTestData.Sample1, startTimestamp + _ * 10)

    val processorConfig = SegmentProcessorConfig(spark,
      log,
      startTimestamp,
      endTimestamp,
      twSize)

    val (segmentDf, statDf, durationProcessor) =
      DurationSegmentProcessor(processorConfig, percentile)

    //    SegmentProcessor.toCsv(durationProcessor, "d:\\tmp\\ttt\\", "")



    val s = durationProcessor.getVisualizationDataset( _ => ())
      ._1
      .collect()
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



    val max = SegmentProcessor.getMax(durationProcessor.getVisualizationDatasetDf(), processorConfig)
      .rdd
      .collect()
      .map(r => (r.getAs[String]("key") -> r.getAs[Long]("max")))
      .toMap

    assert( max("A:B") == 14)
    assert( max("B:C") == 11)
    assert( max("C:D") == 6)

  }
}


object DurationSegmentProcessorTestData {
  val Sample1 =
    """ID	A:B	B:C	C:D	;
      |0	0	10	5	;
      |1	10	30	20	;
      |2	20	50	35	;
      |3	30	70	50	;
      |4	40	90	65	;
      |5	50	110	80	;
      |6	60	130	95	;
      |7	70	150	110	;
      |8	80	170	125	;
      |9	90	190	140	;
      |10	100	210	155	;
      |11	150	500	500	;
      |12	250	1000	5000	;
      |13	300	1500	6000	;
      |14	500	10000	50000	;
      |"""


}