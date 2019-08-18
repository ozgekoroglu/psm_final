package org.processmining.scala.log.common.filtering.traces

import org.processmining.scala.log.common.filtering.expressions.traces.impl.TraceExpression
import org.scalatest.FunSuite

class TraceExpressionLengthTest extends FunSuite {

  private val DefaultTraceId = "ID"
  private val t = TraceExpression()

  test("testEmpty1") {
    val trace = TestUtils.create("", DefaultTraceId)
    val ex = t length (0, 1)
    assert(ex.evaluate(trace))
  }

  test("testEmpty2") {
    val trace = TestUtils.create("", DefaultTraceId)
    val ex = t length (1, 5)
    assert(!ex.evaluate(trace))
  }

  test("testOne1") {
    val trace = TestUtils.create("a", DefaultTraceId)
    val ex = t length (0, 1)
    assert(!ex.evaluate(trace))
  }

  test("testOne2") {
    val trace = TestUtils.create("a", DefaultTraceId)
    val ex = t length (0, 2)
    assert(ex.evaluate(trace))
  }

  test("testManyExactly") {
    val trace = TestUtils.create("0123456789", DefaultTraceId)
    val ex = t length (10, 11)
    assert(ex.evaluate(trace))
  }

  test("testManyLeftWider") {
    val trace = TestUtils.create("0123456789", DefaultTraceId)
    val ex = t length (0, 11)
    assert(ex.evaluate(trace))
  }

  test("testManyRightWider") {
    val trace = TestUtils.create("0123456789", DefaultTraceId)
    val ex = t length (10, 20)
    assert(ex.evaluate(trace))
  }

}
