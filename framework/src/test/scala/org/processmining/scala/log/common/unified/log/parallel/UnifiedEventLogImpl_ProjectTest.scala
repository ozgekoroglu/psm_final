package org.processmining.scala.log.common.unified.log.parallel

import org.processmining.scala.log.common.filtering.expressions.events.regex.impl.EventEx
import org.processmining.scala.log.common.filtering.traces.TestUtils
import org.scalatest.FunSuite

class UnifiedEventLogImpl_ProjectTest extends FunSuite {

  test("Project_emptyLog") {
    val log = UnifiedEventLog.createEmpty()
    assert(log.project(EventEx("A")).traces().isEmpty)
  }

  test("Project_none") {
    val trace0 = TestUtils.create(("A", 0) :: ("B", 10) :: ("C", 20) :: Nil, "id0")
    val trace1 = TestUtils.create(("x", 5) :: ("y", 15) :: ("z", 25) :: Nil, "id1")
    val trace2 = TestUtils.create(("t", 5) :: ("u", 15) :: ("e", 25) :: Nil, "id2")
    val log = UnifiedEventLog.fromTraces(List(trace0, trace1, trace2))
    val projectionOfTraces = log.project(EventEx("f")).traces().map(_._2)
    assert(projectionOfTraces.isEmpty)
  }

  test("Project_one") {
    val trace0 = TestUtils.create(("A", 0) :: ("B", 10) :: ("C", 20) :: Nil, "id0")
    val trace1 = TestUtils.create(("x", 5) :: ("y", 15) :: ("z", 25) :: Nil, "id1")
    val trace2 = TestUtils.create(("t", 5) :: ("u", 15) :: ("e", 25) :: Nil, "id2")
    val log = UnifiedEventLog.fromTraces(List(trace0, trace1, trace2))
    val projectionOfTraces = log.project(EventEx("t"))

    assert(projectionOfTraces.traces.size === 1)

    val optNewTrace0 = projectionOfTraces.find("id2")
    assert(optNewTrace0.isDefined)
    val eventsOfTrace0 = optNewTrace0.get._2
    assert(eventsOfTrace0.size === 1)
    assert(eventsOfTrace0(0).activity === "t")
  }

  test("Project_all") {
    val trace0 = TestUtils.create(("A", 0) :: ("B", 10) :: ("C", 20) :: Nil, "id0")
    val trace1 = TestUtils.create(("x", 5) :: ("y", 15) :: ("z", 25) :: Nil, "id1")
    val trace2 = TestUtils.create(("t", 5) :: ("u", 15) :: ("e", 25) :: Nil, "id2")
    val log = UnifiedEventLog.fromTraces(List(trace0, trace1, trace2))
    val projectionOfTraces = log.project("ABCxyztue".map(x => EventEx(x.toString)): _*)

    assert(projectionOfTraces.traces().size == 3)

    val newTrace0 = projectionOfTraces.find("id0")
    assert(newTrace0.isDefined && newTrace0.get == trace0)

    val newTrace1 = projectionOfTraces.find("id1")
    assert(newTrace1.isDefined && newTrace1.get == trace1)

    val newTrace2 = projectionOfTraces.find("id2")
    assert(newTrace2.isDefined && newTrace2.get == trace2)
  }

  test("Project_several") {
    val trace0 = TestUtils.create(("A", 0) :: ("B", 10) :: ("C", 20) :: Nil, "id0")
    val trace1 = TestUtils.create(("x", 5) :: ("y", 15) :: ("z", 25) :: Nil, "id1")
    val trace2 = TestUtils.create(("t", 5) :: ("u", 15) :: ("e", 25) :: Nil, "id2")
    val log = UnifiedEventLog.fromTraces(List(trace0, trace1, trace2))
    val projectionOfTraces = log.project(EventEx("A"), EventEx("x"), EventEx("y"))

    assert(projectionOfTraces.traces().size === 2)

    val optNewTrace0 = projectionOfTraces.find("id0")
    assert(optNewTrace0.isDefined)
    val eventsOfTrace0 = optNewTrace0.get._2
    assert(eventsOfTrace0.size === 1)
    assert(eventsOfTrace0.head.activity === "A")

    val optNewTrace1 = projectionOfTraces.find("id1")
    assert(optNewTrace1.isDefined)
    val eventsOfTrace1 = optNewTrace1.get._2
    assert(eventsOfTrace1.size === 2)
    assert(eventsOfTrace1(0).activity === "x")
    assert(eventsOfTrace1(1).activity === "y")
  }

}

