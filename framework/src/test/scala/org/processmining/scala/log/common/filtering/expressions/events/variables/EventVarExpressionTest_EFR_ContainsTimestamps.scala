package org.processmining.scala.log.common.filtering.expressions.events.variables

import org.processmining.scala.log.common.filtering.traces.TestUtils
import org.scalatest.FunSuite

class EventVarExpressionTest_EFR_ContainsTimestamps extends FunSuite {
  private val DefaultTraceId = "ID"
  //val attr = TU.EventSchema.apply("index")

  test("testContains_AtTheBeginningDirectly") {
    val trace = TestUtils.createWithIndexAttr("0123456789", DefaultTraceId)
    val ex = (EventVar("0").defineTimestamp("x") >|> EventVar("1").defineTimestamp("y")).where(
      (_, t, _) => {
        val x = t.get("x")
        val y = t.get("y")
        x.isDefined && y.isDefined && x.get == 0L && y.get == 1L
      }
    )
    assert(ex.contains(trace))
  }

  test("testContains_AtTheBeginningEventually") {
    val trace = TestUtils.createWithIndexAttr("0123456789", DefaultTraceId)
    val ex = (EventVar("0").defineTimestamp("x") >|> EventVar("3").defineTimestamp("y")).where(
      (m, t, _) => {
        val x = t.get("x")
        val y = t.get("y")
        x.isDefined && y.isDefined && x.get == 0 && y.get == 3
      }
    )
    assert(ex.contains(trace))
  }

  test("testContains_AtTheBeginningAndAtTheEnd") {
    val trace = TestUtils.createWithIndexAttr("0123456789", DefaultTraceId)
    val ex = (EventVar("0").defineTimestamp("x") >|> EventVar("9").defineTimestamp("y")).where(
      (m, t, _) => {
        val x = t.get("x")
        val y = t.get("y")
        x.isDefined && y.isDefined && x.get == 0 && y.get == 9
      }
    )
    assert(ex.contains(trace))
  }

  test("testContains_AtTheBeginningNoMatching") {
    val trace = TestUtils.createWithIndexAttr("0123456789", DefaultTraceId)
    val ex = (EventVar("0").defineTimestamp("x") >|> EventVar("9").defineTimestamp("y")).where(
      (m, t, _) => {
        val x = t.get("x")
        val y = t.get("y")
        x.isDefined && y.isDefined && x.get == 0 && y.get == 10
      }
    )
    assert(!ex.contains(trace))
  }

  test("testContains_InTheMiddleDirectly") {
    val trace = TestUtils.createWithIndexAttr("0123456789", DefaultTraceId)
    val ex = (EventVar("3").defineTimestamp("x") >|> EventVar("4").defineTimestamp("y")).where(
      (m, t, _) => {
        val x = t.get("x")
        val y = t.get("y")
        x.isDefined && y.isDefined && x.get == 3 && y.get == 4
      }
    )
    assert(ex.contains(trace))
  }

  test("testContains_InTheMiddleEventually") {
    val trace = TestUtils.createWithIndexAttr("0123456789", DefaultTraceId)
    val ex = (EventVar("3").defineTimestamp("x") >|> EventVar("7").defineTimestamp("y")).where(
      (m, t, _) => {
        val x = t.get("x")
        val y = t.get("y")
        x.isDefined && y.isDefined && x.get == 3 && y.get == 7
      }
    )
    assert(ex.contains(trace))
  }

  test("testContains_InTheMiddleNoMatching") {
    val trace = TestUtils.createWithIndexAttr("0123456789", DefaultTraceId)
    val ex = (EventVar("3").defineTimestamp("x") >|> EventVar("7").defineTimestamp("y")).where(
      (m, t, _) => {
        val x = t.get("x")
        val y = t.get("y")
        x.isDefined && y.isDefined && x.get == 3 && y.get == 8
      }
    )
    assert(!ex.contains(trace))
  }

  test("testContains_eventually") {
    val trace = TestUtils.createWithIndexAttr("0000011111", DefaultTraceId)
    val ex = (EventVar("0").defineTimestamp("x") >|> EventVar("1").defineTimestamp("y")).where(
      (m, t, _) => {
        val x = t.get("x")
        val y = t.get("y")
        x.isDefined && y.isDefined && x.get == 4 && y.get == 9
      }
    )
    assert(ex.contains(trace))
  }

  test("testContains_eventuallyNot") {
    val trace = TestUtils.createWithIndexAttr("0000011111", DefaultTraceId)
    val ex = (EventVar("0").defineTimestamp("x") >|> EventVar("1").defineTimestamp("y")).where(
      (m, t, _) => {
        val x = t.get("x")
        val y = t.get("y")
        x.isDefined && y.isDefined && x.get == 4 && y.get == 10
      }
    )
    assert(!ex.contains(trace))
  }

  test("testContains_eventually3") {
    val trace = TestUtils.createWithIndexAttr("q0000q1111e2", DefaultTraceId)
    val ex = (EventVar("0").defineTimestamp("x") >|> EventVar("1").defineTimestamp("y") >|> EventVar("2").defineTimestamp("z")).where(
      (m, t, _) => {
        val x = t.get("x")
        val y = t.get("y")
        val z = t.get("z")
        x.isDefined && y.isDefined && z.isDefined && x.get == 4 && y.get == 9 && z.get == 11
      }
    )
    assert(ex.contains(trace))
  }
}
