package org.processmining.scala.log.common.filtering.traces

import org.processmining.scala.log.common.filtering.expressions.traces.impl.TraceExpression
import org.processmining.scala.log.common.unified.trace.{UnifiedEventAggregator, UnifiedEventRenaming}
import org.processmining.scala.log.common.utils.common.EventAggregator
import org.scalatest.FunSuite


class EventAggregatorL1 extends EventAggregator {
  override def aggregate(name: String): String =
    name match {
      case "a" => "A"
      case "b" => "A"
      case "c" => "A"
      case "d" => "A"
      case "e" => "A"

      case "f" => "F"
      case "g" => "F"
      case "h" => "F"
      case "i" => "F"
      case "j" => "F"

      case "k" => "M"
      case "l" => "M"
      case "m" => "M"
      case "n" => "M"
      case "o" => "M"

      case _ => name
    }

  override def aggregateSegmentKey(originalKey: String) = ???
}

class EventAggregatorL2 extends EventAggregator {
  override def aggregate(name: String): String =
    name match {
      case "A" => "X"
      case "F" => "X"
      case "M" => "Y"
      case _ => name
    }

  override def aggregateSegmentKey(originalKey: String) = ???
}

class TraceExpressionAggregateTest extends FunSuite {
  private val DefaultTraceId = "ID"
  private val t = TraceExpression()
  private val ea1 = new EventAggregatorL1
  private val ea2 = new EventAggregatorL2

  test("renaming") {
    val pattern = "aafkx"
    val originalTrace = TestUtils.create(pattern, DefaultTraceId)
    val aggregationEx = t aggregate UnifiedEventRenaming(ea1)
    val aggregatedTrace = aggregationEx.transform(originalTrace)._2
    assert(aggregatedTrace(0).activity == "A")
    assert(aggregatedTrace(1).activity == "A")
    assert(aggregatedTrace(2).activity == "F")
    assert(aggregatedTrace(3).activity == "M")
    assert(aggregatedTrace(4).activity == "x")
  }

  test("testAggregeteL1_Missed") {
    val pattern = "xyz"
    val originalTrace = TestUtils.create(pattern, DefaultTraceId)
    val aggregationEx = t aggregate UnifiedEventAggregator(ea1)
    val aggregatedTrace = aggregationEx.transform(originalTrace)
    assert(TestUtils.trace2String(aggregatedTrace) == pattern)
    val restoredEx = t.deaggregate
    val restoredTrace = restoredEx.transform(aggregatedTrace)
    assert(TestUtils.trace2String(restoredTrace) == pattern)
  }


  test("testAggregeteL1_OneFirst") {
    val pattern = "abcde"
    val originalTrace = TestUtils.create(pattern, DefaultTraceId)
    val aggregationEx = t aggregate UnifiedEventAggregator(ea1)
    val aggregatedTrace = aggregationEx.transform(originalTrace)
    assert(TestUtils.trace2String(aggregatedTrace) == "A")
    val restoredEx = t.deaggregate
    val restoredTrace = restoredEx.transform(aggregatedTrace)
    assert(TestUtils.trace2String(restoredTrace) == pattern)
  }

  test("testAggregeteL1_TwoFirst") {
    val pattern = "abcdefghij"
    val originalTrace = TestUtils.create(pattern, DefaultTraceId)
    val aggregationEx = t aggregate UnifiedEventAggregator(ea1)
    val aggregatedTrace = aggregationEx.transform(originalTrace)
    assert(TestUtils.trace2String(aggregatedTrace) == "AF")
    val restoredEx = t.deaggregate
    val restoredTrace = restoredEx.transform(aggregatedTrace)
    assert(TestUtils.trace2String(restoredTrace) == pattern)
  }

  test("testAggregeteL1_ThreeFirst") {
    val pattern = "abcdefghijklmno"
    val originalTrace = TestUtils.create(pattern, DefaultTraceId)
    val aggregationEx = t aggregate UnifiedEventAggregator(ea1)
    val aggregatedTrace = aggregationEx.transform(originalTrace)
    assert(TestUtils.trace2String(aggregatedTrace) == "AFM")
    val restoredEx = t.deaggregate
    val restoredTrace = restoredEx.transform(aggregatedTrace)
    assert(TestUtils.trace2String(restoredTrace) == pattern)
  }

  test("testAggregeteL1_MissedThenThreeFirst") {
    val pattern = "xyzabcdefghijklmno"
    val originalTrace = TestUtils.create(pattern, DefaultTraceId)
    val aggregationEx = t aggregate UnifiedEventAggregator(ea1)
    val aggregatedTrace = aggregationEx.transform(originalTrace)
    assert(TestUtils.trace2String(aggregatedTrace) == "xyzAFM")
    val restoredEx = t.deaggregate
    val restoredTrace = restoredEx.transform(aggregatedTrace)
    assert(TestUtils.trace2String(restoredTrace) == pattern)
  }


  test("testAggregeteL1_AFAMA") {
    val pattern = "abcdefghijaklmnod"
    val originalTrace = TestUtils.create(pattern, DefaultTraceId)
    val aggregationEx = t aggregate UnifiedEventAggregator(ea1)
    val aggregatedTrace = aggregationEx.transform(originalTrace)
    assert(TestUtils.trace2String(aggregatedTrace) == "AFAMA")
    val restoredEx = t.deaggregate
    val restoredTrace = restoredEx.transform(aggregatedTrace)
    assert(TestUtils.trace2String(restoredTrace) == pattern)
  }



  test("testAggregeteL1-2_Extra_AFAMA") {
    val pattern = "abcdefghijaklmnod"
    val originalTrace = TestUtils.create(pattern, DefaultTraceId)
    val aggregationEx = t aggregate UnifiedEventAggregator(ea1)
    val aggregatedTrace = aggregationEx.transform(originalTrace)
    assert(TestUtils.trace2String(aggregatedTrace) == "AFAMA")

    val aggregationEx2 = t aggregate UnifiedEventAggregator(ea2)
    val aggregatedTrace2 = aggregationEx2.transform(aggregatedTrace)
    assert(TestUtils.trace2String(aggregatedTrace2) == "XYX")

    val restoredEx2 = t.deaggregate
    val restoredTrace2 = restoredEx2.transform(aggregatedTrace2)
    assert(TestUtils.trace2String(restoredTrace2) == "AFAMA")

    val restoredEx = t.deaggregate
    val restoredTrace = restoredEx.transform(restoredTrace2)
    assert(TestUtils.trace2String(restoredTrace) == pattern)

    val restoredExExtra = t.deaggregate
    val restoredTraceExtra = restoredExExtra.transform(restoredTrace)
    assert(TestUtils.trace2String(restoredTraceExtra) == pattern)
  }


}
