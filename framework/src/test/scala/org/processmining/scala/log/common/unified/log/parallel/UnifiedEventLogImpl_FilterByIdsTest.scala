package org.processmining.scala.log.common.unified.log.parallel

import org.processmining.scala.log.common.filtering.traces.TestUtils
import org.scalatest.FunSuite

class UnifiedEventLogImpl_FilterByIdsTest extends FunSuite {
  test("filterByTraceIds_emptyLog") {
    val log = UnifiedEventLog.createEmpty()
    assert(log.filterByTraceIds("id0").traces().isEmpty)
  }

  test("filterByTraceIds_none") {
    val trace0 = TestUtils.create(("A", 0) :: ("B", 10) :: ("C", 20) :: Nil, "id0")
    val trace1 = TestUtils.create(("x", 5) :: ("y", 15) :: ("z", 25) :: Nil, "id1")
    val trace2 = TestUtils.create(("t", 5) :: ("u", 15) :: ("e", 25) :: Nil, "id2")
    val log = UnifiedEventLog.fromTraces(List(trace0, trace1, trace2))
    val ids = log.filterByTraceIds("id3")
    assert(ids.traces().size === 0)
  }

  test("filterByTraceIds_one") {
    val trace0 = TestUtils.create(("A", 0) :: ("B", 10) :: ("C", 20) :: Nil, "id0")
    val trace1 = TestUtils.create(("x", 5) :: ("y", 15) :: ("z", 25) :: Nil, "id1")
    val trace2 = TestUtils.create(("t", 5) :: ("u", 15) :: ("e", 25) :: Nil, "id2")
    val log = UnifiedEventLog.fromTraces(List(trace0, trace1, trace2))
    val ids = log.filterByTraceIds("id0".split(";"): _*)

    assert(ids.traces().size === 1)

    assert(ids.find("id0").isDefined)
  }

  test("filterByTraceIds_all") {
    val trace0 = TestUtils.create(("A", 0) :: ("B", 10) :: ("C", 20) :: Nil, "id0")
    val trace1 = TestUtils.create(("x", 5) :: ("y", 15) :: ("z", 25) :: Nil, "id1")
    val trace2 = TestUtils.create(("t", 5) :: ("u", 15) :: ("e", 25) :: Nil, "id2")
    val log = UnifiedEventLog.fromTraces(List(trace0, trace1, trace2))
    val ids = log.filterByTraceIds("id0;id1;id2".split(";"): _*)

    assert(ids.traces().size === 3)

    assert(ids.find("id0").isDefined)
    assert(ids.find("id1").isDefined)
    assert(ids.find("id2").isDefined)
  }

  test("filterByTraceIds_several") {
    val trace0 = TestUtils.create(("A", 0) :: ("B", 10) :: ("C", 20) :: Nil, "id0")
    val trace1 = TestUtils.create(("x", 5) :: ("y", 15) :: ("z", 25) :: Nil, "id1")
    val trace2 = TestUtils.create(("t", 5) :: ("u", 15) :: ("e", 25) :: Nil, "id2")
    val log = UnifiedEventLog.fromTraces(List(trace0, trace1, trace2))
    val ids = log.filterByTraceIds("id1;id2".split(";"): _*)

    assert(ids.traces().size === 2)

    assert(ids.find("id1").isDefined)
    assert(ids.find("id2").isDefined)
  }

}
