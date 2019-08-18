package org.processmining.scala.log.common.unified.trace

import org.processmining.scala.log.common.filtering.expressions.events.regex.impl.EventEx
import org.processmining.scala.log.common.filtering.traces.TestUtils
import org.scalatest.FunSuite
import org.processmining.scala.log.common.filtering.expressions.traces._
import org.processmining.scala.log.common.filtering.expressions.traces.impl.TraceExpression
import org.processmining.scala.log.common.utils.common.EventAggregator



class UnifiedEventAggregatorTest extends FunSuite {

  private val DefaultTraceId = "ID"
  private val t = TraceExpression()


  test("testAggregate") {
    val trace = TestUtils.create("", DefaultTraceId)
    val ex = t contains EventEx(".")
    assert(!ex.evaluate(trace))
  }

  test("testDeaggregate") {

  }

}
