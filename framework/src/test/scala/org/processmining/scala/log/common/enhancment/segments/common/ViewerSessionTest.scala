package org.processmining.scala.log.common.enhancment.segments.common

import org.scalatest.FunSuite

class ViewerSessionTest extends FunSuite {

  test("testApply") {
    val s = PreprocessingSession(1, 2, 3, 4, "StartAggregation", "Median proportional")
    assert(s.startMs == 1)
    assert(s.endMs == 2)
    assert(s.twSizeMs == 3)
    assert(s.classCount == 4)
    val e = PreprocessingSession.commit(s)

  }

  test("testDisk") {
    val s = PreprocessingSession(1, 2, 3, 4, InventoryAggregation.getClass.getSimpleName, "Median proportional")
    PreprocessingSession.toDisk(s, "xx.xml", true)
    val x = PreprocessingSession("xx.xml")



  }


}
