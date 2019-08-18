package org.processmining.scala.log.common.utils.common

import java.awt.Component.BaselineResizeBehavior

import org.apache.spark.sql.SparkSession
import org.processmining.scala.log.common.filtering.traces.TestUtils
import org.processmining.scala.log.common.unified.event.UnifiedEvent
import org.processmining.scala.log.common.unified.log.spark.UnifiedEventLog
import org.processmining.scala.log.common.utils.common.export._
import org.scalatest.FunSuite

class EventRelationsTest extends FunSuite {

  def findEFRamount(groups: List[List[String]]): Int = {
    val sizesOfGroups: List[Int] = groups.map(_.size)
    val total = sizesOfGroups.sum

    def formula(totalRest: Int, groupsRest: List[Int]): Int =
      groupsRest match {
        case Nil => 0
        case x :: xs =>
          (x * (totalRest - x)) +
            formula((totalRest - x), xs) +
            (x * (x - 1))
      }

    formula(total, sizesOfGroups)
  }

  @transient
  protected val spark: SparkSession =
    SparkSession
      .builder()
      .appName("DurationSegmentProcessorTest")
      .config("spark.master", "local[*]")
      .getOrCreate()

  test("efr from LOG") {
    val trace0 = TestUtils.create(("f", 0) :: ("e", 0) :: ("d", 10) :: ("c", 20) :: ("b", 20) :: ("a", 20) :: Nil, "id0")
    val trace1 = TestUtils.create(("F", 5) :: ("E", 5) :: ("D", 5) :: ("C", 10) :: ("B", 10) :: ("A", 25) :: Nil, "id1")
    val trace2 = TestUtils.create(("e", 5) :: ("f", 5) :: ("f", 5) :: ("d", 20) :: ("c", 30) :: ("e", 30) :: Nil, "id2")
    val log = UnifiedEventLog.fromTraces(List(trace0, trace1, trace2), spark)


    //        trace0:
    //        fe
    //        d
    //        cba
    //        > fe fd fc fb fa ef ed ec eb ea dc db da cb ca bc ba ac ab
    //        trace1:
    //        FED
    //        CB
    //        A
    //        > FE FD FC FB FA EF ED EC EB EA DF DE DC DB DA CB CA BC BA
    //        trace3:
    //        eff
    //        d
    //        ce
    //        > ef ef ed ec ee fe ff fd fc fe fe ff fd fc fe dc de ce ec

    val expectedResult = List(
      "fe fd fc fb fa ef ed ec eb ea dc db da cb ca bc ba ac ab",
      "FE FD FC FB FA EF ED EC EB EA DF DE DC DB DA CB CA BC BA",
      "ef ef ed ec ee fe ff fd fc fe fe ff fd fc fe dc de ce ec")
      .flatMap(_.split("\\s+"))
      .map(_.split(""))
      .map(m => ((m(0), m(1))))
      .groupBy(g => (g._1, g._2))
      .mapValues(_.size)
      .toArray
      .sortBy(_._1)
    // Array(((B,A),1), ((B,C),1), ((C,A),1), ((C,B),1), ((D,A),1), ((D,B),1), ((D,C),1), ((D,E),1), ((D,F),1), ((E,A),1), ((E,B),1), ((E,C),1), ((E,D),1), ((E,F),1), ((F,A),1), ((F,B),1), ((F,C),1), ((F,D),1), ((F,E),1), ((a,b),1), ((a,c),1), ((b,a),1), ((b,c),1), ((c,a),1), ((c,b),1), ((c,e),1), ((d,a),1), ((d,b),1), ((d,c),2), ((d,e),1), ((e,a),1), ((e,b),1), ((e,c),3), ((e,d),2), ((e,e),1), ((e,f),3), ((f,a),1), ((f,b),1), ((f,c),3), ((f,d),3), ((f,e),5), ((f,f),2))
    val realResult = log
      .combineAndReduce[Int, Int, Int](x => EventRelations.efr((_.timestamp == _.timestamp), x, EventRelations.count),
      x => x, _ + _, _ + _, (_, x) => x
    )
      .sortBy(_._1)
    assert(realResult.size == expectedResult.size)
    assert(realResult === expectedResult)
    realResult.foreach(println(_) + " ")
  }

  test("efr from trace") {
    val events = TestUtils.create(
      ("J", 0) :: ("I", 0) :: ("H", 0) ::
        ("G", 5) ::
        ("F", 10) :: ("E", 10) ::
        ("D", 15) :: ("C", 15) :: ("B", 15) ::
        ("A", 20) :: Nil, "ID")._2
    val expectedResult = List(
      "JI JH JG JF JE JD JC JB JA",
      "IJ IH IG IF IE ID IC IB IA",
      "HJ HI HG HF HE HD HC HB HA",
      "GF GE GD GC GB GA",
      "FE FD FC FB FA",
      "EF ED EC EB EA",
      "DC DB DA",
      "CD CB CA",
      "BD BC BA").flatMap(_.split("\\s+")).sorted // List(BA, BC, BD, CA, CB, CD, DA, DB, DC, EA, EB, EC, ED, EF, FA, FB, FC, FD, FE, GA, GB, GC, GD, GE, GF, HA, HB, HC, HD, HE, HF, HG, HI, HJ, IA, IB, IC, ID, IE, IF, IG, IH, IJ, JA, JB, JC, JD, JE, JF, JG, JH, JI)
    val realResult = EventRelations.efr((_.timestamp == _.timestamp), events, EventRelations.count)
      .map(x => x._1._1 + x._1._2)
      .sorted
    //.map { tuple => tuple.productIterator.mkString("") } // from List[(String, String)]
    assert(realResult === expectedResult)
    assert(realResult.size == 52)
    assert(realResult.size == findEFRamount(EventRelations.groupsOfEventsToGroupsOfActivities(EventRelations.groupByUdf(events, (_.timestamp == _.timestamp))))) //52
  }

  test("dfr from LOG") {
    val trace0 = TestUtils.create(("f", 0) :: ("e", 0) :: ("d", 10) :: ("c", 20) :: ("b", 20) :: ("a", 20) :: Nil, "id0")
    val trace1 = TestUtils.create(("F", 5) :: ("E", 5) :: ("D", 5) :: ("C", 10) :: ("B", 10) :: ("A", 25) :: Nil, "id1")
    val trace2 = TestUtils.create(("e", 5) :: ("f", 5) :: ("f", 5) :: ("d", 20) :: ("c", 30) :: ("e", 30) :: Nil, "id2")
    val log = UnifiedEventLog.fromTraces(List(trace0, trace1, trace2), spark)


    //        trace0:
    //        fe
    //        d
    //        cba
    //        > fe fd ef ed dc db da cb ca bc ba ac ab
    //        trace1:
    //        FED
    //        CB
    //        A
    //        > FE FD FC FB EF ED EC EB DF DE DC DB CB CA BC BA
    //        trace3:
    //        eff
    //        d
    //        ce
    //        > ef ef ed fe ff fd fe ff fd dc de ce ec

    val expectedResult = List(
      "fe fd ef ed dc db da cb ca bc ba ac ab",
      "FE FD FC FB EF ED EC EB DF DE DC DB CB CA BC BA",
      "ef ef ed fe ff fd fe ff fd dc de ce ec")
      .flatMap(_.split("\\s+"))
      .map(_.split(""))
      .map(m => ((m(0), m(1))))
      .groupBy(g => (g._1, g._2))
      .mapValues(_.size)
      .toArray
      .sortBy(_._1)
    // Array(((B,A),1), ((B,C),1), ((C,A),1), ((C,B),1), ((D,B),1), ((D,C),1), ((D,E),1), ((D,F),1), ((E,B),1), ((E,C),1), ((E,D),1), ((E,F),1), ((F,B),1), ((F,C),1), ((F,D),1), ((F,E),1), ((a,b),1), ((a,c),1), ((b,a),1), ((b,c),1), ((c,a),1), ((c,b),1), ((c,e),1), ((d,a),1), ((d,b),1), ((d,c),2), ((d,e),1), ((e,c),1), ((e,d),2), ((e,f),3), ((f,d),3), ((f,e),3), ((f,f),2))
    val realResult = log
      .combineAndReduce[Int, Int, Int](x => EventRelations.dfr((_.timestamp == _.timestamp), x, EventRelations.count),
      x => x, _ + _, _ + _, (_, x) => x)
      .sorted
    realResult.foreach(print(_))
    println()
    expectedResult.foreach(print(_))
    println()
    assert(realResult.size == expectedResult.size)
    assert(realResult === expectedResult)

  }

  test("groupByTimestamps_timestamps are equal") {
    val events = TestUtils.create(
      ("J", 0) :: ("I", 0) :: ("H", 0) ::
        ("G", 5) ::
        ("F", 10) :: ("E", 10) ::
        ("D", 15) :: ("C", 15) :: ("B", 15) ::
        ("A", 20) :: Nil, "ID")._2
    val expectedResult = List(
      List("J", "I", "H"),
      List("G"),
      List("F", "E"),
      List("D", "C", "B"),
      List("A"))
    val realResult = EventRelations.groupsOfEventsToGroupsOfActivities((EventRelations.groupByUdf(events, (_.timestamp == _.timestamp))))
    assert(realResult === expectedResult)
  }

  //  test("groupByTimestamps_timestamps are NOT equal") //TODO

  test("findEFR") {
    val groups = List(
      List("J", "G", "H"),
      List("F", "E", "I"),
      List("D", "C", "K"),
      List("B", "A", "L"))
    val expectedResult = List(
      "JG JH JF JE JI JD JC JK JB JA JL",
      "GJ GH GF GE GI GD GC GK GB GA GL",
      "HJ HG HF HE HI HD HC HK HB HA HL",
      "FE FI FD FC FK FB FA FL",
      "EF EI ED EC EK EB EA EL",
      "IF IE ID IC IK IB IA IL",
      "DC DK DB DA DL",
      "CD CK CB CA CL",
      "KD KC KB KA KL",
      "BA BL",
      "AB AL",
      "LA LB").flatMap(_.split("\\s+")).sorted
    val realResult = EventRelations.findEFR(groups).sorted.map { tuple => tuple.productIterator.mkString("") } // from List[(String, String)]
    assert(realResult == expectedResult)
    assert(realResult.size == 78) //expectedResult.size)
    assert(realResult.size == findEFRamount(groups)) //78
  }

  test("findEFR_2") {
    val groups = List(
      List("J", "G"),
      List("F", "E"),
      List("D", "C"),
      List("B", "A"))
    val expectedResult = List(
      "JG JF JE JD JC JB JA",
      "GJ GF GE GD GC GB GA",
      "FE FD FC FB FA",
      "EF ED EC EB EA",
      "DC DB DA",
      "CD CB CA",
      "BA",
      "AB").flatMap(_.split("\\s+")).sorted // List(AB, BA, CA, CB, CD, DA, DB, DC, EA, EB, EC, ED, EF, FA, FB, FC, FD, FE, GA, GB, GC, GD, GE, GF, GJ, JA, JB, JC, JD, JE, JF, JG)
    val realResult = EventRelations.findEFR(groups).sorted.map { tuple => tuple.productIterator.mkString("") } // from List[(String, String)]
    assert(realResult == expectedResult)
    assert(realResult.size == 32) //expectedResult.size)
    assert(realResult.size == findEFRamount(groups)) // 32
  }

  test("findEFR_list with two lists with repetitions") {
    val groups = List(
      List("B", "D", "B"),
      List("C", "C", "B"),
      List("D", "C", "D"))
    val expectedResult = List(
      "BD BB BC BC BB BD BC BD",
      "DB DB DC DC DB DD DC DD",
      "BB BD BC BC BB BD BC BD",
      "CC CB CD CC CD",
      "CC CB CD CC CD",
      "BC BC BD BC BD",
      "DC DD",
      "CD CD",
      "DD DC").flatMap(_.split("\\s+")).sorted
    val realResult = EventRelations.findEFR(groups).sorted.map { tuple => tuple.productIterator.mkString("") } // from List[(String, String)]
    assert(realResult == expectedResult)
    assert(realResult.size == 45)
    assert(realResult.size == findEFRamount(groups)) //45
  }

  test("findEFR_list with lists with different sizes") {
    val groups = List(
      List("B", "F", "H"),
      List("B", "F"),
      List("B"))
    val expectedResult = List(
      "BF BH BB BF BB",
      "FB FH FB FF FB",
      "HB HF HB HF HB",
      "BF BB",
      "FB FB").flatMap(_.split("\\s+")).sorted // List(BB, BB, BB, BF, BF, BF, BH, FB, FB, FB, FB, FB, FF, FH, HB, HB, HB, HF, HF)
    val realResult = EventRelations.findEFR(groups).sorted.map { tuple => tuple.productIterator.mkString("") } // from List[(String, String)]
    assert(realResult == expectedResult)
    assert(realResult.size == 19)
    assert(realResult.size == findEFRamount(groups)) //45
  }

  test("findEFR_empty list") {
    val groups = List()
    val expectedResult = List()
    val realResult = EventRelations.findEFR(groups)
    assert(realResult == expectedResult)
  }

  test("findEFR_list with the only non-empty list") {
    val groups = List(List("D", "C", "B", "A"))
    val expectedResult = List(
      "DC DB DA",
      "CD CB CA",
      "BD BC BA",
      "AD AC AB").flatMap(_.split("\\s+")).sorted // List(AB, AC, AD, BA, BC, BD, CA, CB, CD, DA, DB, DC)
    val realResult = EventRelations.findEFR(groups).sorted.map { tuple => tuple.productIterator.mkString("") } // from List[(String, String)]
    assert(realResult == expectedResult)
    assert(realResult.size == 12)
    assert(realResult.size == findEFRamount(groups)) //12
  }

  test("findEFR_list with the only empty list") {
    val groups = List(List())
    val expectedResult = List()
    val realResult = EventRelations.findEFR(groups)
    assert(realResult == expectedResult)
  }

  test("crossJoinInListAll") {
    val list = List("A", "B", "C", "D")
    val expectedResult = List(
      "AB AC AD",
      "BA BC BD",
      "CA CB CD",
      "DA DB DC").flatMap(_.split("\\s+")).sorted // List(AB, AC, AD, BA, BC, BD, CA, CB, CD, DA, DB, DC)
    val realResult = EventRelations.crossJoinInListAll(list).sorted.map { tuple => tuple.productIterator.mkString("") } // from List[(String, String)]
    assert(realResult == expectedResult)
  }

  test("crossJoinInListAll_list with repetitions") {
    val list = List("A", "A", "B", "C", "B")
    val expectedResult = List(
      "AA AB AC AB",
      "AA AB AC AB",
      "BA BA BC BB",
      "CA CA CB CB",
      "BA BA BB BC").flatMap(_.split("\\s+")).sorted // List(AA, AA, AB, AB, AB, AB, AC, AC, BA, BA, BA, BA, BB, BB, BC, BC, CA, CA, CB, CB)
    val realResult = EventRelations.crossJoinInListAll(list).sorted.map { tuple => tuple.productIterator.mkString("") } // from List[(String, String)]
    assert(realResult == expectedResult)
  }

  test("crossJoinInListAll_empty list") {
    val list = List[String]()
    val expectedResult = List[String]()
    val realResult = EventRelations.crossJoinInListAll(list).sorted.map { tuple => tuple.productIterator.mkString("") } // from List[(String, String)]
    assert(realResult == expectedResult)
  }

  test("crossJoinInListAll_list with one element") {
    val list = List("A")
    val expectedResult = List()
    val realResult = EventRelations.crossJoinInListAll(list).sorted.map { tuple => tuple.productIterator.mkString("") } // from List[(String, String)]
    assert(realResult == expectedResult)
  }

  //  test("ccrossJoinInListFollowed") = ??? // optional

  test("crossJoinOfSeqs_T=String_list with two lists") {
    val groups = List(
      List("A", "B"),
      List("C", "D"))
    val expectedResult = List(
      "AC AD",
      "BC BD").flatMap(_.split("\\s+")).sorted // List(AC, AD, BC, BD)
    val realResult = EventRelations.crossJoinOfSeqs(groups).map(_.mkString("")).sorted // from List[List[T]]
    assert(realResult == expectedResult)
  }

  test("crossJoinOfSeqs_T=String_list with two lists with repetitions") {
    val groups = List(
      List("B", "D", "B"),
      List("D", "C", "D"))
    val expectedResult = List(
      "BD BC BD",
      "DD DC DD",
      "BD BC BD").flatMap(_.split("\\s+")).sorted // List(BC, BC, BD, BD, BD, BD, DC, DD, DD)
    val realResult = EventRelations.crossJoinOfSeqs(groups).map(_.mkString("")).sorted // from List[List[T]]
    assert(realResult == expectedResult)
  }

  test("crossJoinOfSeqs_T=String_list with two lists with different sizes_1") {
    val groups = List(
      List("D", "C", "B", "A"),
      List("K"))
    val expectedResult = List(
      "DK CK BK AK").flatMap(_.split("\\s+")).sorted // List(AK, BK, CK, DK)
    val realResult = EventRelations.crossJoinOfSeqs(groups).map(_.mkString("")).sorted // from List[List[T]]
    assert(realResult == expectedResult)
  }

  test("crossJoinOfSeqs_T=String_list with two lists with different sizes_2") {
    val groups = List(
      List("K"),
      List("D", "C", "B", "A"))
    val expectedResult = List(
      "KD KC KB KA").flatMap(_.split("\\s+")).sorted // List(KA, KB, KC, KD)
    val realResult = EventRelations.crossJoinOfSeqs(groups).map(_.mkString("")).sorted // from List[List[T]]
    assert(realResult == expectedResult)
  }

  test("crossJoinOfSeqs_T=String_list with more then two lists") {
    val groups = List(
      List("D", "C", "B", "A"),
      List("G", "F", "E"),
      List("I", "J"),
      List("K"))
    val expectedResult = List(
      "DGIK CGIK BGIK AGIK",
      "DFIK CFIK BFIK AFIK",
      "DEIK CEIK BEIK AEIK",
      "DGJK CGJK BGJK AGJK",
      "DFJK CFJK BFJK AFJK",
      "DEJK CEJK BEJK AEJK").flatMap(_.split("\\s+")).sorted // List(AEIK, AEJK, AFIK, AFJK, AGIK, AGJK, BEIK, BEJK, BFIK, BFJK, BGIK, BGJK, CEIK, CEJK, CFIK, CFJK, CGIK, CGJK, DEIK, DEJK, DFIK, DFJK, DGIK, DGJK)
    val realResult = EventRelations.crossJoinOfSeqs(groups).map(_.mkString("")).sorted // from List[List[T]]
    assert(realResult == expectedResult)
  }

  test("crossJoinOfSeqs_empty list") {
    val groups = List()
    val expectedResult = List()
    val realResult = EventRelations.crossJoinOfSeqs(groups)
    assert(realResult == expectedResult)
  }

  test("crossJoinOfSeqs_list with the only non-empty list") {
    val groups = List(List("D", "C", "B", "A"))
    val expectedResult = List(List("D"), List("C"), List("B"), List("A"))
    val realResult = EventRelations.crossJoinOfSeqs(groups)
    assert(realResult == expectedResult)
  }

  test("crossJoinOfSeqs_list with the only empty list") {
    val groups = List(List())
    val expectedResult = List()
    val realResult = EventRelations.crossJoinOfSeqs(groups)
    assert(realResult == expectedResult)
  }

  test("crossJoinOfSeqs_list with non-empty and empty lists") {
    val groups = List(List("D", "C", "B", "A"), List())
    val expectedResult = List()
    val realResult = EventRelations.crossJoinOfSeqs(groups)
    assert(realResult == expectedResult)
  }

  test("crossJoinOfSeqs_list with empty and non-empty lists") {
    val groups = List(List(), List("D", "C", "B", "A"))
    val expectedResult = List()
    val realResult = EventRelations.crossJoinOfSeqs(groups)
    assert(realResult == expectedResult)
  }

  test("crossJoinOfSeqs_T=Int") {
    val groups = List(
      List(4, 3),
      List(2, 1))
    val expectedResult = List(
      "42 41",
      "32 31").flatMap(_.split("\\s+")).sorted // List(31, 32, 41, 42)
    val realResult = EventRelations.crossJoinOfSeqs(groups).map(_.mkString("")).sorted // from List[List[T]]
    assert(realResult == expectedResult)
  }
}
