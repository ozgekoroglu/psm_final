package org.processmining.scala.log.common.enhancment.segments.spark

import org.processmining.scala.log.common.filtering.traces.TestUtils
import org.processmining.scala.log.common.types.Segment
import org.processmining.scala.log.common.unified.event.CommonAttributeSchemas
import org.scalatest.FunSuite

class SegmentUtilsTest extends FunSuite {

  test("testConvertToSegments") {
    val id = "id0"
    val trace1 = TestUtils.create(("A", 0) :: ("B", 10) :: ("C", 25) :: ("D", 30) :: Nil, id)
    val eventType = "SEGMENT"
    val segments = org.processmining.scala.log.common.enhancment.segments.common.SegmentUtils.convertToSegments(":", trace1)._2

    assert(segments(0).activity == "A:B")
    assert(segments(0).timestamp == 0)
    assert(segments(0).getAs[Long](CommonAttributeSchemas.AttrNameDuration) == 10)


    assert(segments(1).activity == "B:C")
    assert(segments(1).timestamp == 10)
    assert(segments(1).getAs[Long](CommonAttributeSchemas.AttrNameDuration) == 15)

    assert(segments(2).activity == "C:D")
    assert(segments(2).timestamp == 25)
    assert(segments(2).getAs[Long](CommonAttributeSchemas.AttrNameDuration) == 5)


  }


  test("testConvertToSegmentsOneEvent") {
    val id = "id0"
    val trace1 = TestUtils.create(("A", 0) :: Nil, id)
    val eventType = "SEGMENT"
    val segments = org.processmining.scala.log.common.enhancment.segments.common.SegmentUtils.convertToSegments(":", trace1)._2
    assert(segments.isEmpty)

  }

  test("testConvertToSegmentsEmpty") {
    val id = "id0"
    val trace1 = TestUtils.create(List[(String, Int)](), "id0")
    val eventType = "SEGMENT"
    val segments = org.processmining.scala.log.common.enhancment.segments.common.SegmentUtils.convertToSegments( ":", trace1)._2
    assert(segments.isEmpty)

  }

}
