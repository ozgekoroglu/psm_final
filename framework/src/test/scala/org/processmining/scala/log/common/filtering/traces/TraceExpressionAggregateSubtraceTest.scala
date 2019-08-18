package org.processmining.scala.log.common.filtering.traces
import org.processmining.scala.log.common.filtering.expressions.events.regex.impl.EventEx
import org.scalatest.FunSuite
import org.processmining.scala.log.common.filtering.expressions.traces._
import org.processmining.scala.log.common.filtering.expressions.traces.impl.TraceExpression

class TraceExpressionAggregateSubtraceTest extends FunSuite {

  private val DefaultTraceId = "ID"
  private val t = TraceExpression()


  test("testSubtraceStart") {
    val trace = TestUtils.create("bcdefgabcdefgabcdefg", DefaultTraceId)
    val ex = t aggregateSubtraces (EventEx("b") >-> EventEx("e"), "X")
    val tt = ex.transform(trace)._2
    assert(TestUtils.trace2String(ex.transform(trace)) == "XfgaXfgaXfg")
  }

  test("testSubtraceMiddle") {
    val trace = TestUtils.create("abcdefgabcdefgabcdefg", DefaultTraceId)
    val ex = t aggregateSubtraces (EventEx("b") >-> EventEx("e"), "X")
    assert(TestUtils.trace2String(ex.transform(trace)) == "aXfgaXfgaXfg")
  }

  test("testSubtraceEnd") {
    val trace = TestUtils.create("abcdefgabcdefgabcde", DefaultTraceId)
    val ex = t aggregateSubtraces (EventEx("b") >-> EventEx("e"), "X")
    assert(TestUtils.trace2String(ex.transform(trace)) == "aXfgaXfgaX")
  }
}
