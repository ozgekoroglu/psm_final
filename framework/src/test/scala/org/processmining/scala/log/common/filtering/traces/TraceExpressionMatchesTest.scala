package org.processmining.scala.log.common.filtering.traces

import org.processmining.scala.log.common.filtering.expressions.events.regex.impl.EventEx
import org.scalatest.FunSuite
import org.processmining.scala.log.common.filtering.expressions.traces._
import org.processmining.scala.log.common.filtering.expressions.traces.impl.TraceExpression


class TraceExpressionMatchesTest  extends FunSuite {
  private val DefaultTraceId = "ID"
  private val t = TraceExpression()
  test("testMatches") {
    val trace = TestUtils.create("12345", DefaultTraceId)
    val pattern = (EventEx("1") >> EventEx("2") >> EventEx("3") >> EventEx("4") >> EventEx("5"))
    //println(s"ex = '${pattern.translate()}'")
    val ex = t matches pattern
    assert(ex.evaluate(trace))
  }

  //  test("testIfMatches") {
  //    val trace = create("12345", DefaultTraceId)
  //    val ex = t ifMatches (EventEx("1") >> EventEx("2") >> EventEx("3") >> EventEx("4") >> EventEx("5"))
  //    assert(trace2String(ex.transform(trace)) == "12345")
  //  }
  //
  //  test("testIfNotMatches") {
  //    val trace = create("12345", DefaultTraceId)
  //    val ex = t ifMatches (EventEx("1") >> EventEx("2") >> EventEx("3") >> EventEx("4"))
  //    assert(trace2String(ex.transform(trace)) == "")
  //  }


  test("testNotMatchesStart") {
    val trace = TestUtils.create("12345", DefaultTraceId)

    val pattern = EventEx("1")
    println(s"ex = '${pattern.translate()}'")

    val ex = t matches pattern
    assert(!ex.evaluate(trace))
  }

  test("testNotMatchesMiddle") {
    val trace = TestUtils.create("12345", DefaultTraceId)
    val ex = t matches (EventEx("2") >> EventEx("3"))
    assert(!ex.evaluate(trace))
  }

  test("testNotMatchesEnd") {
    val trace = TestUtils.create("12345", DefaultTraceId)
    val ex = t matches EventEx("5")
    assert(!ex.evaluate(trace))
  }

  test("testMatchesRepEntire") {
    val trace = TestUtils.create("12225", DefaultTraceId)
    val ex = t matches (EventEx("1") >> EventEx("2") >> EventEx("2") >> EventEx("2") >> EventEx("5"))
    assert(ex.evaluate(trace))
  }

  test("testNotMatchesRepRep") {
    val trace = TestUtils.create("12225", DefaultTraceId)
    val ex = t matches (EventEx("2") >> EventEx("2") >> EventEx("2"))
    assert(!ex.evaluate(trace))
  }



}
