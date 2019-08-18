package org.processmining.scala.applications.bp.bpic2018

import org.scalatest.FunSuite

class StatisticsTest extends FunSuite {

  test("testCountOccurrences") {
    val list = Array(
      ("A", "1"),
      ("A", "1"),
      ("A", "1"),
      ("A", "2"),
      ("A", "3"),
      ("A", "3"),
      ("B", "2"),
      ("B", "2"),
      ("B", "3"))
    val absolutOccur = list.groupBy(_._1).mapValues(_.size) // expected A-6, B-3
    val caseOccur = list.groupBy(_._1).map(x => x._2.groupBy(_._2)) //.distinct) // expected A-3, B-2
    //    res.map(x => x._2.map((println(_))))
    println(absolutOccur)
    println(caseOccur)
  }

  test("testAttr1attr2table1") {

  }

  test("testAttr1attr2table2") {

  }

  test("testAllTracesAttrs") {

  }

}
