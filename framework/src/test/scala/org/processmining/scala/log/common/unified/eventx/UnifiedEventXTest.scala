package org.processmining.scala.log.common.unified.event

import org.scalatest.FunSuite

import scala.collection.immutable.SortedMap

//TODO: complete
class UnifiedEventTest extends FunSuite {

  test("testCtor") {
    val e1 = UnifiedEvent(1, "e1", SortedMap(), None)
    val e2 = UnifiedEvent(2, "e2", SortedMap(), None)
    val a = UnifiedEvent(1, "a", SortedMap(), Some(Seq(e1, e2)))
    assert(a.timestamp == 1)
    assert(a.activity == "a")
    assert(a.aggregatedEvents.get == Seq(e1, e2))
  }

  test("testEqualAndHashCode") {
    val e1_1 = UnifiedEvent(1, "e1", SortedMap(), None)
    val e2_1 = UnifiedEvent(2, "e2", SortedMap(), None)
    val a1 = UnifiedEvent(1, "a", SortedMap("x" -> 1, "y" -> 2L, "z" -> "text", "byte" -> Byte.MaxValue), Some(Seq(e1_1, e2_1)))

    val e1_2 = UnifiedEvent(1, "e1", SortedMap(), None)
    val e2_2 = UnifiedEvent(2, "e2", SortedMap(), None)
    val a2 = UnifiedEvent(1, "a", SortedMap("y" -> 2L, "x" -> 1, "z" -> "text", "byte" -> Byte.MaxValue), Some(Seq(e1_2, e2_2)))

    assert(a1 == a2)
    assert(a1.hashCode() == a2.hashCode())
  }


  test("testHasAttribute") {
    val a = UnifiedEvent(1, "a", SortedMap("x" -> 1, "y" -> 2L, "z" -> "text", "byte" -> Byte.MaxValue), None)
    assert(a.hasAttribute("x"))
    assert(a.hasAttribute("y"))
    assert(a.hasAttribute("z"))
    assert(a.hasAttribute("byte"))
    assert(!a.hasAttribute("e"))
  }


  test("testRegexpableForm") {
    val a = UnifiedEvent(5, "a", SortedMap("x" -> 1, "y" -> 2L, "z" -> "text", "byte" -> Byte.MaxValue), None)
    assert(a.regexpableForm() == "<a@0000000000000000005#&127%&0000000001%&0000000000000000002%&text%>")
  }

  test("testGetAs") {
    val a = UnifiedEvent(1, "a", SortedMap("x" -> 1, "y" -> 2L, "z" -> "text", "byte" -> Byte.MaxValue), None)
    assert(a.getAs[Int]("x") == 1)
    assert(a.getAs[Long]("y") == 2L)
    assert(a.getAs[String]("z") == "text")
    assert(a.getAs[Byte]("byte") == Byte.MaxValue)
    assertThrows[ClassCastException](a.getAs[Long]("x"))
    assertThrows[ClassCastException](a.getAs[Byte]("x"))
    assertThrows[ClassCastException](a.getAs[String]("x"))
  }

  test("testToCsvWithoutAttributes") {

  }


  test("testAggregatedEvents") {

  }

  test("testToCsv") {

  }

}

