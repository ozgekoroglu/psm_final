package org.processmining.scala.log.common.filtering.expressions.events.regex.impl

import org.scalatest.FunSuite
import org.processmining.scala.log.common.filtering.expressions.traces._
import org.processmining.scala.log.common.filtering.expressions.traces.impl.TraceExpression
import org.processmining.scala.log.common.filtering.traces.TestUtils


class EventRegexExpression_ZeroOrOneTest extends FunSuite {

  private val DefaultTraceId = "ID"
  private val t = TraceExpression()

  test("testZeroOrOne_Zero") {
    val trace = TestUtils.create("0123456789", DefaultTraceId)
    val ex = t contains (EventEx("x") zeroOrOne)
    assert(ex.evaluate(trace))
  }

  test("testZeroOrOne_One_Beginning") {
    val trace = TestUtils.create("0123456789", DefaultTraceId)
    val ex = t contains (EventEx("0") zeroOrOne)
    assert(ex.evaluate(trace))
  }

  test("testZeroOrOne_One_Middle") {
    val trace = TestUtils.create("0123456789", DefaultTraceId)
    val ex = t contains (EventEx("5") zeroOrOne)
    assert(ex.evaluate(trace))
  }

  test("testZeroOrOne_One_End") {
    val trace = TestUtils.create("0123456789", DefaultTraceId)
    val ex = t contains (EventEx("9") zeroOrOne)
    assert(ex.evaluate(trace))
  }

  test("testZeroOrOne_Begining2ndZero") {
    val trace = TestUtils.create("0123456789", DefaultTraceId)
    val ex = t contains (EventEx("0") >> (EventEx("x") zeroOrOne))
    assert(ex.evaluate(trace))
  }

  test("testZeroOrOne_Begining2ndOne") {
    val trace = TestUtils.create("0123456789", DefaultTraceId)
    val ex = t contains (EventEx("0") >> (EventEx("1") zeroOrOne))
    assert(ex.evaluate(trace))
  }

  test("testZeroOrOne_Begining1stWrong2ndOne") {
    val trace = TestUtils.create("0123456789", DefaultTraceId)
    val ex = t contains (EventEx("x") >> (EventEx("1") zeroOrOne))
    assert(!ex.evaluate(trace))
  }



  test("testZeroOrOne_Middle2ndZero") {
    val trace = TestUtils.create("0123456789", DefaultTraceId)
    val ex = t contains (EventEx("3") >> (EventEx("x") zeroOrOne))
    assert(ex.evaluate(trace))
  }

  test("testZeroOrOne_Middle2ndOne") {
    val trace = TestUtils.create("0123456789", DefaultTraceId)
    val ex = t contains (EventEx("3") >> (EventEx("4") zeroOrOne))
    assert(ex.evaluate(trace))
  }

  test("testZeroOrOne_Middle1stWrong2ndOne") {
    val trace = TestUtils.create("0123456789", DefaultTraceId)
    val ex = t contains (EventEx("x") >> (EventEx("3") zeroOrOne))
    assert(!ex.evaluate(trace))
  }



  test("testZeroOrOne_End2ndZero") {
    val trace = TestUtils.create("0123456789", DefaultTraceId)
    val ex = t contains (EventEx("9") >> (EventEx("x") zeroOrOne))
    assert(ex.evaluate(trace))
  }

  test("testZeroOrOne_End2ndOne") {
    val trace = TestUtils.create("0123456789", DefaultTraceId)
    val ex = t contains (EventEx("8") >> (EventEx("9") zeroOrOne))
    assert(ex.evaluate(trace))
  }

  test("testZeroOrOne_End1stWrong2ndOne") {
    val trace = TestUtils.create("0123456789", DefaultTraceId)
    val ex = t contains (EventEx("x") >> (EventEx("9") zeroOrOne))
    assert(!ex.evaluate(trace))
  }


}
