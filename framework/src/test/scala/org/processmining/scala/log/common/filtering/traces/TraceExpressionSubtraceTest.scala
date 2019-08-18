package org.processmining.scala.log.common.filtering.traces
import org.processmining.scala.log.common.filtering.expressions.events.regex.impl.EventEx
import org.scalatest.FunSuite
import org.processmining.scala.log.common.filtering.expressions.traces._
import org.processmining.scala.log.common.filtering.expressions.traces.impl.TraceExpression

class TraceExpressionSubtraceTest extends FunSuite {

  private val DefaultTraceId = "ID"
  private val t = TraceExpression()


  test("testSubtraceStart") {
    val trace = TestUtils.create("12345", DefaultTraceId)
    val ex = t subtrace EventEx("1")
    assert(TestUtils.trace2String(ex.transform(trace)) == "1")
  }

  test("testSubtraceMiddle") {
    val trace = TestUtils.create("12345", DefaultTraceId)
    val ex = t subtrace (EventEx("2") >> EventEx("3") >> EventEx("4"))
    assert(TestUtils.trace2String(ex.transform(trace)) == "234")
  }

  test("testSubtraceEnd") {
    val trace = TestUtils.create("12345", DefaultTraceId)
    val ex = t subtrace EventEx("5")
    assert(TestUtils.trace2String(ex.transform(trace)) == "5")
  }

  test("testSubtraceEmpty") {
    val trace = TestUtils.create("12345", DefaultTraceId)
    val ex = t subtrace EventEx("6")
    assert(TestUtils.trace2String(ex.transform(trace)).isEmpty)
  }


}


