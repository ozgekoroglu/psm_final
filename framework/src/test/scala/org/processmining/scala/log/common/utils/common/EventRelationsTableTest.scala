package org.processmining.scala.log.common.utils.common

import org.scalatest.FunSuite
import org.processmining.scala.log.common.utils.common.export._

class EventRelationsTableTest extends FunSuite {

  test("testEfrTableFinish") {
    val pairs: List[((String, String), Int)] = List(
      (("D", "D"), 1),
      (("D", "B"), 2),
      (("C", "D"), 3),
      (("C", "B"), 4),
      (("B", "D"), 5),
      (("B", "B"), 4),
      (("B", "A"), 3),
      (("A", "C"), 2),
      (("A", "B"), 1))
//    val expectedResultIntermed_table: Array[Array[Boolean]] = Array(
//      Array(false, true, true, false),
//      Array(true, true, false, true),
//      Array(false, true, false, true),
//      Array(false, true, false, true)
//    )
    val expectedResult_table: Array[Array[Byte]] = Array(
      Array(4, 3, 1, 4),
      Array(3, 5, 2, 3),
      Array(2, 1, 4, 1),
      Array(4, 3, 2, 5))
    val expectedResult_keys: Map[String, Int] = Map(
      "A" -> 0,
      "B" -> 1,
      "C" -> 2,
      "D" -> 3)
    val actualResult = EventRelationsTable.efrTableFinish(pairs)
    val actualResult_table = actualResult._1
    val actualResult_keys = actualResult._2
//    println (expectedResult_table.deep.mkString("\n"))
//    println
    println(actualResult_table.deep.mkString("\n"))
    assert(actualResult_table.flatten.toList == expectedResult_table.flatten.toList)
    assert(actualResult_table.flatten.toList.size == 16)
    assert(actualResult_keys == expectedResult_keys)
    assert(actualResult_keys.size == expectedResult_keys.size)
  }

  test("testEfrTableIntermediate") {
    val pairs: List[((String, String), Int)] = List(
      (("D", "D"), 2),
      (("C", "A"), 6),
      (("B", "C"), 1),
      (("B", "A"), 3),
      (("B", "A"), 1))
    val keys: Map[String, Int] = Map(
      "A" -> 0,
      "B" -> 1,
      "C" -> 2,
      "D" -> 3)
    val expectedResult: Array[Array[Boolean]] = Array(
      Array(false, false, false, false),
      Array(true, false, true, false),
      Array(true, false, false, false),
      Array(false, false, false, true)
    )
    val actualResult = EventRelationsTable.efrTableIntermediate(pairs, keys)
    println(expectedResult.deep.mkString("\n")) // List(...
    println
    println(actualResult.deep.mkString("\n"))
    assert(actualResult.flatten.toList == expectedResult.flatten.toList)
    assert(actualResult.flatten.toList.size == 16)
  }

  test("testEfrTableFromIntermediateToFinish") {
    val followingsArray: Array[Array[Boolean]] = Array(
      Array(true, false, true, false),
      Array(true, false, true, false),
      Array(true, false, true, false),
      Array(true, false, true, false)
    )
    val expectedResult: Array[Array[Byte]] = Array(
      Array(5, 2, 3, 2),
      Array(1, 4, 1, 4),
      Array(3, 2, 5, 2),
      Array(1, 4, 1, 4)
    )
    val actualResult = EventRelationsTable.efrTableFromIntermediateToFinish(followingsArray)
    //    println(actualResult.flatten.toList) // List(5, 2, 3, 2, 1, 4, 1, 4, 3, 2, 5, 2, 1, 4, 1, 4)
    assert(actualResult.flatten.toList == expectedResult.flatten.toList)
    assert(actualResult.flatten.toList.size == 16)
  }

  test("testUniqueActivitiesKeys") {
    val pairs: List[((String, String), Int)] = List(
      (("B", "D"), 2),
      (("D", "A"), 6),
      (("B", "c"), 1),
      (("a", "b"), 3),
      (("B", "A"), 1))
    val expectedResult: Map[String, Int] = Map(
      "A" -> 0,
      "a" -> 1,
      "b" -> 2,
      "B" -> 3,
      "c" -> 4,
      "D" -> 5)
    val actualResult = EventRelationsTable.uniqueActivitiesKeys(pairs)
    println(actualResult)
    assert(actualResult == expectedResult)
    assert(actualResult.size == expectedResult.size)
  }

}
