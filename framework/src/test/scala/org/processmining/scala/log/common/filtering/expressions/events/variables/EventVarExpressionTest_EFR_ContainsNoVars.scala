package org.processmining.scala.log.common.filtering.expressions.events.variables

import org.processmining.scala.log.common.filtering.traces.TestUtils
import org.scalatest.FunSuite

class EventVarExpressionTest_EFR_ContainsNoVars extends FunSuite {
  private val DefaultTraceId = "ID"

  test("testContains_AtTheBeginningDirectly") {
    val trace = TestUtils.create("0123456789", DefaultTraceId)
    val ex = EventVar("0") >|> EventVar("1")
    assert(ex.contains(trace))

  }

  test("testContains_AtTheBeginningEventually") {
    val trace = TestUtils.create("0123456789", DefaultTraceId)
    val ex = EventVar("0") >|> EventVar("3")
    assert(ex.contains(trace))

  }

  test("testContains_AtTheBeginningEventuallyAtTHeEnd") {
    val trace = TestUtils.create("0123456789", DefaultTraceId)
    val ex = EventVar("0") >|> EventVar("9")
    assert(ex.contains(trace))

  }

  test("testContains_AtTheBeginningThenNoMatching") {
    val trace = TestUtils.create("0123456789", DefaultTraceId)
    val ex = EventVar("0") >|> EventVar("x")
    assert(!ex.contains(trace))

  }


  test("testContains_InTheMiddleDirectly") {
    val trace = TestUtils.create("0123456789", DefaultTraceId)
    val ex = EventVar("4") >|> EventVar("5")
    assert(ex.contains(trace))

  }

  test("testContains_InTheMiddleEventually") {
    val trace = TestUtils.create("0123456789", DefaultTraceId)
    val ex = EventVar("4") >|> EventVar("8")
    assert(ex.contains(trace))

  }

  test("testContains_InTheMiddleNoMatching") {
    val trace = TestUtils.create("0123456789", DefaultTraceId)
    val ex = EventVar("4") >|> EventVar("x")
    assert(!ex.contains(trace))
  }

}
