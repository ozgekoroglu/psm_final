package org.processmining.scala.log.common.filtering.expressions.events.variables

import org.processmining.scala.log.common.filtering.traces.TestUtils
import org.scalatest.FunSuite

class EventVarExpressionTest_EFR_ContainsVars extends FunSuite {
  private val DefaultTraceId = "ID"
  private val attr = "index"

  test("testContains_AtTheBeginningDirectly") {
    val trace = TestUtils.createWithIndexAttr("0123456789", DefaultTraceId)
    val ex = (EventVar("0").defineVar("x", attr) >|> EventVar("1").defineVar("y", attr)).where(
      (m, _, _) => {
        val x = m.get("x")
        val y = m.get("y")
        x.isDefined && y.isDefined && x.get.asInstanceOf[Int] == 0 && y.get.asInstanceOf[Int] == 1
      }
    )
    assert(ex.contains(trace))
  }

  test("testContains_AtTheBeginningEventually") {
    val trace = TestUtils.createWithIndexAttr("0123456789", DefaultTraceId)
    val ex = (EventVar("0").defineVar("x", attr) >|> EventVar("3").defineVar("y", attr)).where(
      (m, t, _) => {
        val x = m.get("x")
        val y = m.get("y")
        x.isDefined && y.isDefined && x.get.asInstanceOf[Int] == 0 && y.get.asInstanceOf[Int] == 3
      }
    )
    assert(ex.contains(trace))
  }

  test("testContains_AtTheBeginningAndAtTheEnd") {
    val trace = TestUtils.createWithIndexAttr("0123456789", DefaultTraceId)
    val ex = (EventVar("0").defineVar("x", attr) >|> EventVar("9").defineVar("y", attr)).where(
      (m, t, _) => {
        val x = m.get("x")
        val y = m.get("y")
        x.isDefined && y.isDefined && x.get.asInstanceOf[Int] == 0 && y.get.asInstanceOf[Int] == 9
      }
    )
    assert(ex.contains(trace))
  }

  test("testContains_AtTheBeginningNoMatching") {
    val trace = TestUtils.createWithIndexAttr("0123456789", DefaultTraceId)
    val ex = (EventVar("0").defineVar("x", attr) >|> EventVar("9").defineVar("y", attr)).where(
      (m, t, _) => {
        val x = m.get("x")
        val y = m.get("y")
        x.isDefined && y.isDefined && x.get.asInstanceOf[Int] == 0 && y.get.asInstanceOf[Int] == 10
      }
    )
    assert(!ex.contains(trace))
  }

  test("testContains_InTheMiddleDirectly") {
    val trace = TestUtils.createWithIndexAttr("0123456789", DefaultTraceId)
    val ex = (EventVar("3").defineVar("x", attr) >|> EventVar("4").defineVar("y", attr)).where(
      (m, t, _) => {
        val x = m.get("x")
        val y = m.get("y")
        x.isDefined && y.isDefined && x.get.asInstanceOf[Int] == 3 && y.get.asInstanceOf[Int] == 4
      }
    )
    assert(ex.contains(trace))
  }

  test("testContains_InTheMiddleEventually") {
    val trace = TestUtils.createWithIndexAttr("0123456789", DefaultTraceId)
    val ex = (EventVar("3").defineVar("x", attr) >|> EventVar("7").defineVar("y", attr)).where(
      (m, t, _) => {
        val x = m.get("x")
        val y = m.get("y")
        x.isDefined && y.isDefined && x.get.asInstanceOf[Int] == 3 && y.get.asInstanceOf[Int] == 7
      }
    )
    assert(ex.contains(trace))
  }

  test("testContains_InTheMiddleNoMatching") {
    val trace = TestUtils.createWithIndexAttr("0123456789", DefaultTraceId)
    val ex = (EventVar("3").defineVar("x", attr) >|> EventVar("7").defineVar("y", attr)).where(
      (m, t, _) => {
        val x = m.get("x")
        val y = m.get("y")
        x.isDefined && y.isDefined && x.get.asInstanceOf[Int] == 3 && y.get.asInstanceOf[Int] == 8
      }
    )
    assert(!ex.contains(trace))
  }


  test("testContains_eventually") {
    val trace = TestUtils.createWithIndexAttr("0000011111", DefaultTraceId)
    val ex = (EventVar("0").defineVar("x", attr) >|> EventVar("1").defineVar("y", attr)).where(
      (m, t, _) => {
        val x = m.get("x")
        val y = m.get("y")
        x.isDefined && y.isDefined && x.get.asInstanceOf[Int] == 4 && y.get.asInstanceOf[Int] == 9
      }
    )
    assert(ex.contains(trace))
  }

  test("testContains_eventuallyNot") {
    val trace = TestUtils.createWithIndexAttr("0000011111", DefaultTraceId)
    val ex = (EventVar("0").defineVar("x", attr) >|> EventVar("1").defineVar("y", attr)).where(
      (m, t, _) => {
        val x = m.get("x")
        val y = m.get("y")
        x.isDefined && y.isDefined && x.get.asInstanceOf[Int] == 4 && y.get.asInstanceOf[Int] == 10
      }
    )
    assert(!ex.contains(trace))
  }

  test("testContains_eventually3") {
    val trace = TestUtils.createWithIndexAttr("q0000q1111e2", DefaultTraceId)
    val ex = (EventVar("0").defineVar("x", attr) >|> EventVar("1").defineVar("y", attr)>|> EventVar("2").defineVar("z", attr)).where(
      (m, t, _) => {
        val x = m.get("x")
        val y = m.get("y")
        val z = m.get("z")
        x.isDefined && y.isDefined && z.isDefined && x.get.asInstanceOf[Int] == 4 && y.get.asInstanceOf[Int] == 9 && z.get.asInstanceOf[Int] == 11
      }
    )
    assert(ex.contains(trace))
  }


}
