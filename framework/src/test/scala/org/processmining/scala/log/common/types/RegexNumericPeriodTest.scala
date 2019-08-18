package org.processmining.scala.log.common.types

import java.util.regex.Pattern

import org.scalatest.FunSuite


class RegexNumericPeriodTest extends FunSuite {
  val LongWidth = Long.MaxValue.toString.length

  //Remove for debug print
  private def println(args: Object) = {}

  //  test("testApplyLongPar(from,to)_amountCheckedValues > Int.MaxValue_?????????") {
  //    val (from, to): (Long, Long) = (Int.MaxValue, Long.MaxValue)
  //    val start: Long = from // inclusive
  //    val end: Long = start + 100000L*Int.MaxValue - 1L // inclusive
  //    val amountCheckedValues: Long = end - start + 1L
  //
  //
  //    val period = new RegexNumericPeriod(LongWidth)
  //    val ex = period.apply(from, to)
  //    val pattern = Pattern.compile(ex)
  //
  //    println(s"from = $from, to = $to")
  //    println(s"start = $start, end = $end")
  //    println(s"amountCheckedValues = $amountCheckedValues")
  //    println(ex)
  //
  //    (1L to 100000L)
  //      .par
  //      .foreach { seed =>
  //        val iStart: Long = seed * Int.MaxValue
  //        var i: Long = iStart
  //        while (i < iStart + 1.00001 * Int.MaxValue) {
  //          val valueWithLeadingZeros = s"%0${LongWidth}d".format(i) // ?????????????????????????????????????????????
  //          assert(pattern.matcher(valueWithLeadingZeros).matches(), valueWithLeadingZeros)
  //          i = i + 1
  //        }
  //      }
  //  }

  test("testApplyLong(from,to)small values, in, must be true") {
    val (from, to): (Long, Long) = (5L, 10L)
    val (start, end): (Long, Long) = (from, to) // inclusive
    val amountCheckedValues: Long = end - start + 1L

    val period = new RegexNumericPeriod(LongWidth)
    val ex = period.apply(from, to)
    val pattern = Pattern.compile(ex)

    println(s"from = $from, to = $to")
    println(s"start = $start, end = $end")
    println(s"amountCheckedValues = $amountCheckedValues")
    println(ex)

    require(amountCheckedValues <= Int.MaxValue, "amountCheckedValues must be <= Int.MaxValue")
    (0L until amountCheckedValues)
      //      .par
      .foreach { i =>
      val value: Long = start + i
      val valueWithLeadingZeros = s"%0${LongWidth}d".format(value) // ?????????????????????????????????????????????
      //        val valueWithLeadingZeros = s"${"%019d".format(value)}"
      assert(pattern.matcher(valueWithLeadingZeros).matches(), valueWithLeadingZeros)
      println(valueWithLeadingZeros)
    }
  }

  test("testApplyLong(from,to) small values, before, must be false") {
    val (from, to): (Long, Long) = (5L, 10L)
    val (start, end): (Long, Long) = (0L, from - 1) // inclusive
    val amountCheckedValues: Long = end - start + 1L

    val period = new RegexNumericPeriod(LongWidth)
    val ex = period.apply(from, to)
    val pattern = Pattern.compile(ex)

    println(s"from = $from, to = $to")
    println(s"start = $start, end = $end")
    println(s"amountCheckedValues = $amountCheckedValues")
    println(ex)

    require(amountCheckedValues <= Int.MaxValue, "amountCheckedValues must be <= Int.MaxValue")
    (0L until amountCheckedValues)
      //      .par
      .foreach { i =>
      val value: Long = start + i
      val valueWithLeadingZeros = s"%0${LongWidth}d".format(value)
      assert(!pattern.matcher(valueWithLeadingZeros).matches(), valueWithLeadingZeros)
      println(valueWithLeadingZeros)
    }
  }

  test("testApplyLong(from,to)small values, after, must be false") {
    val (from, to): (Long, Long) = (5L, 10L)
    val (start, end): (Long, Long) = (to + 1, 20L) // inclusive
    val amountCheckedValues: Long = end - start + 1L

    val period = new RegexNumericPeriod(LongWidth)
    val ex = period.apply(from, to)
    val pattern = Pattern.compile(ex)

    println(s"from = $from, to = $to")
    println(s"start = $start, end = $end")
    println(s"amountCheckedValues = $amountCheckedValues")
    println(ex)

    require(amountCheckedValues <= Int.MaxValue, "amountCheckedValues must be <= Int.MaxValue")
    (0L until amountCheckedValues)
      //      .par
      .foreach { i =>
      val value: Long = start + i
      val valueWithLeadingZeros = s"%0${LongWidth}d".format(value)
      assert(!pattern.matcher(valueWithLeadingZeros).matches(), valueWithLeadingZeros)
      println(valueWithLeadingZeros)
    }
  }

  test("testApplyLong(from,to) 0, Long.MaxValue_1") {
    val (from, to): (Long, Long) = (0L, Long.MaxValue)
    val (start, end): (Long, Long) = (from, 100L) // inclusive
    val amountCheckedValues: Long = end - start + 1L

    val period = new RegexNumericPeriod(LongWidth)
    val ex = period.apply(from, to)
    val pattern = Pattern.compile(ex)

    println(s"from = $from, to = $to")
    println(s"start = $start, end = $end")
    println(s"amountCheckedValues = $amountCheckedValues")
    //    println(ex)

    require(amountCheckedValues <= Int.MaxValue, "amountCheckedValues must be <= Int.MaxValue")
    (0L until amountCheckedValues)
      //      .par
      .foreach { i =>
      val value: Long = start + i
      val valueWithLeadingZeros = s"%0${LongWidth}d".format(value) // ?????????????????????????????????????????????
      assert(pattern.matcher(valueWithLeadingZeros).matches(), valueWithLeadingZeros)
      //        println(valueWithLeadingZeros)
    }
  }

  test("testApplyLong(from,to) 0, Long.MaxValue_2") {
    val (from, to): (Long, Long) = (0L, Long.MaxValue)
    val (start, end): (Long, Long) = (to - 100L, to) // inclusive
    val amountCheckedValues: Long = end - start + 1L

    val period = new RegexNumericPeriod(LongWidth)
    val ex = period.apply(from, to)
    val pattern = Pattern.compile(ex)

    println(s"from = $from, to = $to")
    println(s"start = $start, end = $end")
    println(s"amountCheckedValues = $amountCheckedValues")
    //    println(ex)

    require(amountCheckedValues <= Int.MaxValue, "amountCheckedValues must be <= Int.MaxValue")
    (0L until amountCheckedValues)
      //      .par
      .foreach { i =>
      val value: Long = start + i
      val valueWithLeadingZeros = s"%0${LongWidth}d".format(value) // ?????????????????????????????????????????????
      assert(pattern.matcher(valueWithLeadingZeros).matches(), valueWithLeadingZeros)
      println(valueWithLeadingZeros)
    }
  }

  test("testApplyLong(from,to) middle, in, must be true") {
    val (from, to): (Long, Long) = (Long.MaxValue - 2L * Int.MaxValue, Long.MaxValue - Int.MaxValue - 1L)
    val (start, end): (Long, Long) = (from, to) // inclusive
    val amountCheckedValues: Long = end - start + 1L

    val period = new RegexNumericPeriod(LongWidth)
    val ex = period.apply(from, to)
    val pattern = Pattern.compile(ex)

    println(s"from = $from, to = $to")
    println(s"start = $start, end = $end")
    println(s"amountCheckedValues = $amountCheckedValues")
    //    println(ex)

    require(amountCheckedValues <= Int.MaxValue, "amountCheckedValues must be <= Int.MaxValue")
    (0L until amountCheckedValues by 1234567L)
      //      .par
      .foreach { i =>
      val value: Long = start + i
      val valueWithLeadingZeros = s"%0${LongWidth}d".format(value) // ?????????????????????????????????????????????
      assert(pattern.matcher(valueWithLeadingZeros).matches(), valueWithLeadingZeros)
      println(valueWithLeadingZeros)
    }
  }

  test("testApplyLong(from,to) middle, to, must be true") {
    val (from, to): (Long, Long) = (Long.MaxValue - 2L * Int.MaxValue, Long.MaxValue - Int.MaxValue - 1L)
    val (start, end): (Long, Long) = (from, to) // inclusive
    val amountCheckedValues: Long = end - start + 1L

    val period = new RegexNumericPeriod(LongWidth)
    val ex = period.apply(from, to)
    val pattern = Pattern.compile(ex)

    println(s"from = $from, to = $to")
    println(s"start = $start, end = $end")
    println(s"amountCheckedValues = $amountCheckedValues")
    //    println(ex)

    require(amountCheckedValues <= Int.MaxValue, "amountCheckedValues must be <= Int.MaxValue")
    (0L until amountCheckedValues by 1234567L)
      //      .par
      .foreach { i =>
      val value: Long = start + i
      val valueWithLeadingZeros = s"%0${LongWidth}d".format(value) // ?????????????????????????????????????????????
      assert(pattern.matcher(valueWithLeadingZeros).matches(), valueWithLeadingZeros)
      println(valueWithLeadingZeros)
    }
    val valueWithLeadingZeros = s"%0${LongWidth}d".format(to)
    assert(pattern.matcher(valueWithLeadingZeros).matches(), valueWithLeadingZeros)
    println(valueWithLeadingZeros)
  }

  test("testApplyLong(from,to) middle, before, must be false") {
    val (from, to): (Long, Long) = (Long.MaxValue - 2L * Int.MaxValue, Long.MaxValue - Int.MaxValue - 1L)
    val (start, end): (Long, Long) = (from - Int.MaxValue, from - 1L) // inclusive
    val amountCheckedValues: Long = end - start + 1L

    val period = new RegexNumericPeriod(LongWidth)
    val ex = period.apply(from, to)
    val pattern = Pattern.compile(ex)

    println(s"from = $from, to = $to")
    println(s"start = $start, end = $end")
    println(s"amountCheckedValues = $amountCheckedValues")
    //    println(ex)

    require(amountCheckedValues <= Int.MaxValue, "amountCheckedValues must be <= Int.MaxValue")
    (0L until amountCheckedValues by 1234567L)
      //      .par
      .foreach { i =>
      val value: Long = start + i
      val valueWithLeadingZeros = s"%0${LongWidth}d".format(value) // ?????????????????????????????????????????????
      assert(!pattern.matcher(valueWithLeadingZeros).matches(), valueWithLeadingZeros)
      println(valueWithLeadingZeros)
    }

    val valueWithLeadingZeros = s"%0${LongWidth}d".format(end)
    assert(!pattern.matcher(valueWithLeadingZeros).matches(), valueWithLeadingZeros)
    println(valueWithLeadingZeros)
  }

  test("testApplyLong(from,to) middle, after, to, must be false") {
    val (from, to): (Long, Long) = (Long.MaxValue - 2L * Int.MaxValue, Long.MaxValue - Int.MaxValue - 1L)
    val (start, end): (Long, Long) = (to + 1L, to + Int.MaxValue) // inclusive
    val amountCheckedValues: Long = end - start + 1L

    val period = new RegexNumericPeriod(LongWidth)
    val ex = period.apply(from, to)
    val pattern = Pattern.compile(ex)

    println(s"from = $from, to = $to")
    println(s"start = $start, end = $end")
    println(s"amountCheckedValues = $amountCheckedValues")
    //    println(ex)

    require(amountCheckedValues <= Int.MaxValue, "amountCheckedValues must be <= Int.MaxValue")
    (0L until amountCheckedValues by 1234567L)
      //      .par
      .foreach { i =>
      val value: Long = start + i
      val valueWithLeadingZeros = s"%0${LongWidth}d".format(value) // ?????????????????????????????????????????????
      assert(!pattern.matcher(valueWithLeadingZeros).matches(), valueWithLeadingZeros)
      println(valueWithLeadingZeros)
    }
  }

  test("testApply(from,to): must throw exception when 'from' < 'to'") {
    val (from, to): (Long, Long) = (Long.MaxValue, Int.MaxValue)
    intercept[IllegalArgumentException] {
      (new RegexNumericPeriod(LongWidth)).apply(from, to)
    }
  }

  test("testApply(from,to): must throw exception when 'from' == 'to'") {
    val (from, to): (Long, Long) = (Long.MaxValue, Long.MaxValue)
    intercept[IllegalArgumentException] {
      (new RegexNumericPeriod(LongWidth)).apply(from, to)
    }
  }

  test("testApply(from,to): must throw exception when 'width of from' > 'requiredWidth'") {
    val (from, to) = (111, 11)
    val requiredWidth = 2
    intercept[IllegalArgumentException] {
      (new RegexNumericPeriod(requiredWidth)).apply(from, to)
    }
  }

  test("testApply(from,to): must throw exception when 'width of to' > 'requiredWidth'") {
    val (from, to) = (11, 111)
    val requiredWidth = 2
    intercept[IllegalArgumentException] {
      (new RegexNumericPeriod(requiredWidth)).apply(from, to)
    }
  }

  test("testApply(exactTime) ") {
    val exactTime = Long.MaxValue - Int.MaxValue
    val period = new RegexNumericPeriod(LongWidth)
    val ex = period.apply(exactTime)
    val pattern = Pattern.compile(ex)

    assert(pattern.matcher(s"%0${LongWidth}d".format(exactTime)).matches(), s"%0${LongWidth}d".format(exactTime))
    assert(!pattern.matcher(s"%0${LongWidth}d".format(exactTime - 1L)).matches(), s"%0${LongWidth}d".format(exactTime - 1L))
    assert(!pattern.matcher(s"%0${LongWidth}d".format(exactTime + 1L)).matches(), s"%0${LongWidth}d".format(exactTime + 1L))
    assert(!pattern.matcher(s"%0${LongWidth}d".format(0L)).matches(), s"%0${LongWidth}d".format(0L))
    //
    //    assert(pattern.matcher("0000000000000000015").matches())
    //    assert(!pattern.matcher("0000000000000000000").matches())
    //    assert(!pattern.matcher("0000000000000000014").matches())
    //    assert(!pattern.matcher("0000000000000000016").matches())
  }

  test("testApply(exactTime): must throw exception when 'width of exactValue' > 'requiredWidth'") {
    val exactValue = 111
    val requiredWidth = 2
    intercept[IllegalArgumentException] {
      (new RegexNumericPeriod(requiredWidth)).apply(exactValue)
    }
  }

}


