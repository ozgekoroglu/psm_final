package org.processmining.scala.log.common.filtering.traces
import org.processmining.scala.log.common.filtering.expressions.traces.impl.TraceExpression
import org.scalatest.FunSuite
class TraceExpressionTrimToTimeframeTest extends FunSuite {

  private val DefaultTraceId = "ID"
  private val t = TraceExpression()

  test("trim_empty") {
    val trace = TestUtils.create("", DefaultTraceId)
    val ex = t trimToTimeframe (0, 100)
    assert(TestUtils.trace2String(ex.transform(trace)) == "")
  }

  test("trim_one") {
    val trace = TestUtils.create("0", DefaultTraceId)
    val ex = t trimToTimeframe (0, 0)
    assert(TestUtils.trace2String(ex.transform(trace)) == "0")
  }

  test("trim_exactly") {
    val trace = TestUtils.create("0123456789", DefaultTraceId)
    val ex = t trimToTimeframe (0, 9)
    assert(TestUtils.trace2String(ex.transform(trace)) == "0123456789")
  }

  test("trim_wider") {
    val trace = TestUtils.create("0123456789", DefaultTraceId)
    val ex = t trimToTimeframe (-1, 10)
    assert(TestUtils.trace2String(ex.transform(trace)) == "0123456789")
  }

  test("trim_outside") {
    val trace = TestUtils.create("0123456789", DefaultTraceId)
    val ex = t trimToTimeframe (100, 110)
    assert(TestUtils.trace2String(ex.transform(trace)) == "")
  }

  test("trim_beginning") {
    val trace = TestUtils.create("0123456789", DefaultTraceId)
    val ex = t trimToTimeframe (0, 3)
    assert(TestUtils.trace2String(ex.transform(trace)) == "0123")
  }

  test("trim_beginning_left_wider") {
    val trace = TestUtils.create("0123456789", DefaultTraceId)
    val ex = t trimToTimeframe (-3, 3)
    assert(TestUtils.trace2String(ex.transform(trace)) == "0123")
  }

  test("trim_middle") {
    val trace = TestUtils.create("0123456789", DefaultTraceId)
    val ex = t trimToTimeframe (3, 6)
    assert(TestUtils.trace2String(ex.transform(trace)) == "3456")
  }

  test("trim_end") {
    val trace = TestUtils.create("0123456789", DefaultTraceId)
    val ex = t trimToTimeframe (6, 9)
    assert(TestUtils.trace2String(ex.transform(trace)) == "6789")
  }

  test("trim_end_right_wider") {
    val trace = TestUtils.create("0123456789", DefaultTraceId)
    val ex = t trimToTimeframe (6, 100)
    assert(TestUtils.trace2String(ex.transform(trace)) == "6789")
  }


}
