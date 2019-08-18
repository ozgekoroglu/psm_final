package org.processmining.scala.log.common.filtering.traces
import org.processmining.scala.log.common.filtering.expressions.events.regex.impl.EventEx
import org.scalatest.FunSuite
import org.processmining.scala.log.common.filtering.expressions.traces._
import org.processmining.scala.log.common.filtering.expressions.traces.impl.TraceExpression


class TraceExpressionExtractSubtraces3EventsInThePatternTest extends FunSuite {
  private val DefaultTraceId = "ID"
  private val t = TraceExpression()

  test("testExtractSubtraces_1FromStart") {
    val trace = TestUtils.create("123456789", DefaultTraceId)
    val ex = t extractSubtraces  (EventEx("1") >> EventEx("2") >> EventEx("3"))
    assert(ex.transform(trace).size == 1)
    assert(TestUtils.trace2String(ex.transform(trace)(0)) == "123")

  }


  test("testExtractSubtraces_1FromMiddle") {
    val trace = TestUtils.create("123456789", DefaultTraceId)
    val ex = t extractSubtraces  (EventEx("4") >> EventEx("5") >> EventEx("6"))
    assert(ex.transform(trace).size == 1)
    assert(TestUtils.trace2String(ex.transform(trace)(0)) == "456")

  }

  test("testExtractSubtraces_1FromEnd") {
    val trace = TestUtils.create("123456789", DefaultTraceId)
    val ex = t extractSubtraces  (EventEx("7") >> EventEx("8") >> EventEx("9"))
    assert(ex.transform(trace).size == 1)
    assert(TestUtils.trace2String(ex.transform(trace)(0)) == "789")

  }


  test("testExtractSubtraces_5withEnds") {
    val trace = TestUtils.create("az1az23az45az6789az", DefaultTraceId)
    val ex = t extractSubtraces  (EventEx("a") >> EventEx("z"))
    assert(ex.transform(trace).size == 5)
    assert(TestUtils.trace2String(ex.transform(trace)(0)) == "az")
    assert(TestUtils.trace2String(ex.transform(trace)(1)) == "az")
    assert(TestUtils.trace2String(ex.transform(trace)(2)) == "az")
    assert(TestUtils.trace2String(ex.transform(trace)(3)) == "az")
    assert(TestUtils.trace2String(ex.transform(trace)(4)) == "az")

  }


  test("testExtractSubtraces_5withoutEnds") {
    val trace = TestUtils.create("0az1az23az45az6789az0", DefaultTraceId)
    val ex = t extractSubtraces  (EventEx("a") >> EventEx("z"))
    assert(ex.transform(trace).size == 5)
    assert(TestUtils.trace2String(ex.transform(trace)(0)) == "az")
    assert(TestUtils.trace2String(ex.transform(trace)(1)) == "az")
    assert(TestUtils.trace2String(ex.transform(trace)(2)) == "az")
    assert(TestUtils.trace2String(ex.transform(trace)(3)) == "az")
    assert(TestUtils.trace2String(ex.transform(trace)(4)) == "az")

  }


}
