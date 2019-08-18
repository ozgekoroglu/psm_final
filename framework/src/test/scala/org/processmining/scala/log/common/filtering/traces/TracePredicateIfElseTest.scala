package org.processmining.scala.log.common.filtering.traces

import org.processmining.scala.log.common.filtering.expressions.events.regex.impl.EventEx
import org.scalatest.FunSuite
import org.processmining.scala.log.common.filtering.expressions.traces._
import org.processmining.scala.log.common.filtering.expressions.traces.impl.TraceExpression

class TracePredicateIfElseTest extends FunSuite {

  private val DefaultTraceId = "ID"
  private val t = TraceExpression()


  test("testIfElse_Then") {
    val trace = TestUtils.create("12345", DefaultTraceId)
    val ex = (t contains EventEx("1")).ifElse(t take 1, t take 2)
    assert(TestUtils.trace2String(ex.transform(trace)) == "1")
  }

  test("testIfElse_Else") {
    val trace = TestUtils.create("12345", DefaultTraceId)
    val ex = (t contains EventEx("x")).ifElse(t take 1, t take 2)
    assert(TestUtils.trace2String(ex.transform(trace)) == "12")
  }

  test("testAnd11") {
    val trace = TestUtils.create("12345", DefaultTraceId)
    val ex = (t contains EventEx("1")) and (t contains EventEx("2"))
    assert(ex.evaluate(trace))
  }

  test("testAnd1x") {
    val trace = TestUtils.create("12345", DefaultTraceId)
    val ex = (t contains EventEx("1")) and (t contains EventEx("x"))
    assert(!ex.evaluate(trace))
  }

  test("testAndx1") {
    val trace = TestUtils.create("12345", DefaultTraceId)
    val ex = (t contains EventEx("x")) and (t contains EventEx("1"))
    assert(!ex.evaluate(trace))
  }

  test("testAndxy") {
    val trace = TestUtils.create("12345", DefaultTraceId)
    val ex = (t contains EventEx("x")) and (t contains EventEx("y"))
    assert(!ex.evaluate(trace))
  }

  test("testOr11") {
    val trace = TestUtils.create("12345", DefaultTraceId)
    val ex = (t contains EventEx("1")) or (t contains EventEx("2"))
    assert(ex.evaluate(trace))
  }

  test("testOr1x") {
    val trace = TestUtils.create("12345", DefaultTraceId)
    val ex = (t contains EventEx("1")) or (t contains EventEx("x"))
    assert(ex.evaluate(trace))
  }

  test("testOrx1") {
    val trace = TestUtils.create("12345", DefaultTraceId)
    val ex = (t contains EventEx("x")) or (t contains EventEx("1"))
    assert(ex.evaluate(trace))
  }

  test("testOrxy") {
    val trace = TestUtils.create("12345", DefaultTraceId)
    val ex = (t contains EventEx("x")) or (t contains EventEx("y"))
    assert(!ex.evaluate(trace))
  }

  test("testNot1") {
    val trace = TestUtils.create("12345", DefaultTraceId)
    val ex = !(t contains EventEx("1"))
    assert(!ex.evaluate(trace))
  }

  test("testNotx") {
    val trace = TestUtils.create("12345", DefaultTraceId)
    val ex = !(t contains EventEx("x"))
    assert(ex.evaluate(trace))
  }

}
