package org.processmining.scala.log.common.filtering.traces

import org.processmining.scala.log.common.filtering.expressions.traces.impl.TraceExpression
import org.scalatest.FunSuite

//Covered
class TraceExpressionTakeTest extends FunSuite {
  private val DefaultTraceId = "ID"
  private val t = TraceExpression()

  test("testTake0") {
    val trace = TestUtils.create("12345", DefaultTraceId)
    val ex = t take 0
    assert(TestUtils.trace2String(ex.transform(trace)) == "")
  }

  test("testTakeOne") {
    val trace = TestUtils.create("12345", DefaultTraceId)
    val ex = t take 1
    assert(TestUtils.trace2String(ex.transform(trace)) == "1")
  }

  test("testTakeTwo") {
    val trace = TestUtils.create("12345", DefaultTraceId)
    val ex = t take 2
    assert(TestUtils.trace2String(ex.transform(trace)) == "12")
  }

  test("testTakeEverything") {
    val trace = TestUtils.create("12345", DefaultTraceId)
    val ex = t take 5
    assert(TestUtils.trace2String(ex.transform(trace)) == "12345")
  }

  test("testTakeMore") {
    val trace = TestUtils.create("12345", DefaultTraceId)
    val ex = t take 6
    assert(TestUtils.trace2String(ex.transform(trace)) == "12345")
  }

}
