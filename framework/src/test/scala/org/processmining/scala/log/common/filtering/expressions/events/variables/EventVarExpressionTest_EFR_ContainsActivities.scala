package org.processmining.scala.log.common.filtering.expressions.events.variables
import org.processmining.scala.log.common.filtering.traces.TestUtils
import org.scalatest.FunSuite

class EventVarExpressionTest_EFR_ContainsActivities extends FunSuite {
  private val DefaultTraceId = "ID"
  //val attr = TU.EventSchema.apply("index")

  test("testContains_ContainsLoop") {
    val trace = TestUtils.createWithIndexAttr("01234567819", DefaultTraceId)
    val ex = (EventVar("1").defineActivity("x") >|> EventVar(".").defineActivity("y")).where(
      (_, _, a) => {
        val x = a.get("x")
        val y = a.get("y")
        x.isDefined && y.isDefined && x.get == y.get
      }
    )
    assert(ex.contains(trace))
  }

  test("testContains_NotContainsLoop") {
    val trace = TestUtils.createWithIndexAttr("0123456789", DefaultTraceId)
    val ex = (EventVar("0").defineActivity("x") >|> EventVar(".").defineActivity("y")).where(
      (_, _, a) => {
        val x = a.get("x")
        val y = a.get("y")
        x.isDefined && y.isDefined && x.get == y.get
      }
    )
    assert(!ex.contains(trace))
  }

}
