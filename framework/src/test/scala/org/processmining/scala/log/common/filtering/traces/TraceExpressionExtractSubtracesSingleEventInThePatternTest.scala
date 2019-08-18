package org.processmining.scala.log.common.filtering.traces

import org.processmining.scala.log.common.filtering.expressions.events.regex.impl.EventEx
import org.scalatest.FunSuite
import org.processmining.scala.log.common.filtering.expressions.traces._
import org.processmining.scala.log.common.filtering.expressions.traces.impl.TraceExpression


class TraceExpressionExtractSubtracesSingleEventInThePatternTest extends FunSuite {
  private val DefaultTraceId = "ID"
  private val t = TraceExpression()


  test("testExtractSubtraces_OneFromStart") {
    val trace = TestUtils.create("12345", DefaultTraceId)
    val ex = t extractSubtraces  EventEx("1")
    assert(ex.transform(trace).size == 1)
    assert(TestUtils.trace2String(ex.transform(trace).head) == "1")
  }

  test("testExtractSubtraces_OneFromMiddle") {
    val trace = TestUtils.create("12345", DefaultTraceId)
    val ex = t extractSubtraces  EventEx("3")
    assert(ex.transform(trace).size == 1)
    assert(TestUtils.trace2String(ex.transform(trace).head) == "3")
  }

  test("testExtractSubtraces_OneFromEnd") {
    val trace = TestUtils.create("12345", DefaultTraceId)
    val ex = t extractSubtraces  EventEx("5")
    assert(ex.transform(trace).size == 1)
    assert(TestUtils.trace2String(ex.transform(trace).head) == "5")
  }

  test("testExtractSubtraces_Zero") {
    val trace = TestUtils.create("12345", DefaultTraceId)
    val ex = t extractSubtraces  EventEx("6")
    assert(ex.transform(trace).size == 0)
  }

  test("testExtractSubtraces_3separate_of_5") {
    val trace = TestUtils.create("12141", DefaultTraceId)
    val ex = t extractSubtraces  EventEx("1")
    assert(ex.transform(trace).size == 3)
    assert(TestUtils.trace2String(ex.transform(trace)(0)) == "1")
    assert(TestUtils.trace2String(ex.transform(trace)(1)) == "1")
    assert(TestUtils.trace2String(ex.transform(trace)(2)) == "1")
  }

  test("testExtractSubtraces_5_of_5") {
    val trace = TestUtils.create("11111", DefaultTraceId)
    val ex = t extractSubtraces  EventEx("1")
    assert(ex.transform(trace).size == 5)
    assert(TestUtils.trace2String(ex.transform(trace)(0)) == "1")
    assert(TestUtils.trace2String(ex.transform(trace)(1)) == "1")
    assert(TestUtils.trace2String(ex.transform(trace)(2)) == "1")
    assert(TestUtils.trace2String(ex.transform(trace)(3)) == "1")
    assert(TestUtils.trace2String(ex.transform(trace)(4)) == "1")
  }




}
