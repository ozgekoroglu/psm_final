package org.processmining.scala.log.common.unified.log.parallel

import org.processmining.scala.log.common.filtering.traces.TestUtils
import org.scalatest.FunSuite

class UnifiedEventLogImpl_FullOuterJoin_Test extends FunSuite {
  test("testFullOuterJoin_interleaving") {
    val trace1 = TestUtils.create(("A", 0) :: ("B", 10) :: ("C", 20) :: Nil, "id0")
    val trace2 = TestUtils.create(("x", 5) :: ("y", 15) :: ("z", 25) :: Nil, "id0")
    val log1 = UnifiedEventLog.fromTraces(List(trace1))
    val log2 = UnifiedEventLog.fromTraces(List(trace2))
    val log = log1 fullOuterJoin log2
    val events = log.traces().head._2

    assert(events(0).activity == "A")
    assert(events(1).activity == "x")
    assert(events(2).activity == "B")
    assert(events(3).activity == "y")
    assert(events(4).activity == "C")
    assert(events(5).activity == "z")
  }


  test("testFullOuterJoin_duplicates") {
    val trace1 = TestUtils.create(("A", 0) :: ("B", 10) :: ("C", 20) :: Nil, "id0")
    val trace2 = TestUtils.create(("A", 0) :: ("B", 10) :: ("C", 20) :: Nil, "id0")
    val log1 = UnifiedEventLog.fromTraces(List(trace1))
    val log2 = UnifiedEventLog.fromTraces(List(trace2))
    val log = log1 fullOuterJoin log2
    val events = log.traces().head._2

    assert(events(0).activity == "A")
    assert(events(1).activity == "B")
    assert(events(2).activity == "C")

  }


  test("testFullOuterJoin_partialInterleavingDuplicates") {
    val trace1 = TestUtils.create(("A", 0) :: ("B", 10) :: ("C", 20) :: Nil, "id0")
    val trace2 = TestUtils.create(("A", 0) :: ("y", 15) :: ("z", 25) :: Nil, "id0")
    val log1 = UnifiedEventLog.fromTraces(List(trace1))
    val log2 = UnifiedEventLog.fromTraces(List(trace2))
    val log = log1 fullOuterJoin log2
    val events = log.traces().head._2

    assert(events(0).activity == "A")
    assert(events(1).activity == "B")
    assert(events(2).activity == "y")
    assert(events(3).activity == "C")
    assert(events(4).activity == "z")
  }


  test("testFullOuterJoin_SimilarActivitiesDifferentTimestamps") {
    val trace1 = TestUtils.create(("A", 0) :: ("B", 10) :: ("C", 20) :: Nil, "id0")
    val trace2 = TestUtils.create(("A", 5) :: ("B", 15) :: ("C", 25) :: Nil, "id0")
    val log1 = UnifiedEventLog.fromTraces(List(trace1))
    val log2 = UnifiedEventLog.fromTraces(List(trace2))
    val log = log1 fullOuterJoin log2
    val events = log.traces().head._2

    assert(events(0).activity == "A")
    assert(events(1).activity == "A")
    assert(events(2).activity == "B")
    assert(events(3).activity == "B")
    assert(events(4).activity == "C")
    assert(events(5).activity == "C")

  }


  test("testFullOuterJoin_1st_empty") {
    val trace1 = TestUtils.create(("A", 0) :: ("B", 10) :: ("C", 20) :: Nil, "id0")
    val trace2 = TestUtils.create(List[(String, Int)](), "id0")
    val log1 = UnifiedEventLog.fromTraces(List(trace1))
    val log2 = UnifiedEventLog.fromTraces(List(trace2))
    val log = log1 fullOuterJoin log2
    val events = log.traces().head._2

    assert(events(0).activity == "A")
    assert(events(1).activity == "B")
    assert(events(2).activity == "C")

  }

  test("testFullOuterJoin_2nd_empty") {
    val trace1 = TestUtils.create(List[(String, Int)](), "id0")
    val trace2 = TestUtils.create(("A", 0) :: ("B", 10) :: ("C", 20) :: Nil, "id0")
    val log1 = UnifiedEventLog.fromTraces(List(trace1))
    val log2 = UnifiedEventLog.fromTraces(List(trace2))
    val log = log1 fullOuterJoin log2
    val events = log.traces().head._2

    assert(events(0).activity == "A")
    assert(events(1).activity == "B")
    assert(events(2).activity == "C")

  }

}

