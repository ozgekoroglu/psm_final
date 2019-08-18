package org.processmining.scala.log.common.filtering.traces

import org.processmining.scala.log.common.filtering.expressions.events.regex.impl.EventEx
import org.scalatest.FunSuite
import org.processmining.scala.log.common.filtering.expressions.traces._
import org.processmining.scala.log.common.filtering.expressions.traces.impl.TraceExpression


//Covered
class TraceExpressionContainsTest extends FunSuite {

  private val DefaultTraceId = "ID"
  private val t = TraceExpression()

  test("testEmptyMustNotContainAnything") {
    val trace = TestUtils.create("", DefaultTraceId)
    val ex = t contains EventEx(".")
    assert(!ex.evaluate(trace))
  }

  test("testContainsStart") {
    val trace = TestUtils.create("12345", DefaultTraceId)
    val ex = t contains EventEx("1")
    assert(ex.evaluate(trace))
  }

  test("testContainsMiddle") {
    val trace = TestUtils.create("12345", DefaultTraceId)
    val ex = t contains (EventEx("2") >> EventEx("3"))
    assert(ex.evaluate(trace))
  }

  test("testContainsEnd") {
    val trace = TestUtils.create("12345", DefaultTraceId)
    val ex = t contains EventEx("5")
    assert(ex.evaluate(trace))
  }

  test("testContainsRepRep") {
    val trace = TestUtils.create("12225", DefaultTraceId)
    val ex = t contains  (EventEx("2") >> EventEx("2") >> EventEx("2"))
    assert(ex.evaluate(trace))
  }

}


