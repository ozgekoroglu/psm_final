package org.processmining.scala.log.common.filtering.expressions.events.regex.impl

import org.processmining.scala.log.common.types.{RegexNumericPeriod, RegexNumericPeriodTestStub}
import org.scalatest.FunSuite


private object TestEventExFactory {
  def apply(activityName: String): EventEx = new EventEx(RegexNumericPeriodTestStub.ByteIntLongConverters, Some(activityName), None, Map())

  def apply(): EventEx = new EventEx(RegexNumericPeriodTestStub.ByteIntLongConverters, None, None, Map())
}

class EventExTest extends FunSuite {

  test("testDuring") {
    val (from, to) = (1, 5)
    val eventEx = TestEventExFactory().during(from, to)
    //    println(eventEx.timestamp)
    assert(eventEx.timestamp.isDefined && eventEx.timestamp.get == RegexNumericPeriodTestStub.FromToValLong)
  }

  test("testDuring: no exception when 'from' == 0 and 'from' < 'to'") {
    val (from, to) = (0, 5)
    TestEventExFactory().during(from, to)
  }

  //  test("testDuring_long values") {
  //    val (from, to) = (Long.MaxValue - 1000, Long.MaxValue)
  //    val eventEx = TestEventExFactory().during(from, to)
  //    assert(eventEx.timestamp.isDefined && eventEx.timestamp.get == RegexNumericPeriodStub.FromToVal)
  //  }

  test("testDuring must throw exception when 'from' < 0") {
    val (from, to) = (-1, 2)
    intercept[IllegalArgumentException] {
      TestEventExFactory.apply().during(from, to)
    }
  }

  test("testDuring must throw exception when 'from' > 'to'") {
    val (from, to) = (2, 1)
    intercept[IllegalArgumentException] {
      TestEventExFactory.apply().during(from, to)
    }
  }

  test("testDuring must throw exception when 'from' == 'to'") {
    val (from, to) = (5, 5)
    intercept[IllegalArgumentException] {
      TestEventExFactory.apply().during(from, to)
    }
  }

  test("testAt") {
    val exactTime = 555
    val eventEx = TestEventExFactory().at(exactTime)
    //    println(eventEx.timestamp)
    assert(eventEx.timestamp.isDefined && eventEx.timestamp.get == RegexNumericPeriodTestStub.ExactValLong)
  }

  test("testAt: no exception with 'exactTime' == 0") {
    val exactTime = 0
    TestEventExFactory().at(exactTime)
  }

  //  test("testAt_long value") {
  //    val exactTime = Long.MaxValue
  //    val eventEx = TestEventExFactory().at(exactTime)
  //    println(eventEx.timestamp)
  //    assert(eventEx.timestamp.isDefined && eventEx.timestamp.get == RegexNumericPeriodStub.ExactTimeVal)
  //  }

  test("testAt must throw exception when 'exactTime' < 0") {
    val exactTime = -1
    intercept[IllegalArgumentException] {
      TestEventExFactory.apply().at(exactTime)
    }
  }

  test("testWithValue (name: String, value: T) T is Byte") {
    val name = "ABCdef"
    val value: Byte = Byte.MaxValue
    val eventEx = TestEventExFactory().withValue(name, value)
    assert(eventEx.attrs.size === 1)
    assert(eventEx.attrs(name) === RegexNumericPeriodTestStub.ExactValByte)
  }

  test("testWithValue (name: String, value: T) T is Int") {
    val name = "ABCdef"
    val value: Int = Int.MaxValue
    val eventEx = TestEventExFactory().withValue(name, value)
    assert(eventEx.attrs.size === 1)
    assert(eventEx.attrs(name) === RegexNumericPeriodTestStub.ExactValInt)
  }

  test("testWithValue (name: String, value: T) T is Long") {
    val name = "ABCdef"
    val value: Long = Long.MaxValue
    val eventEx = TestEventExFactory().withValue(name, value)
    assert(eventEx.attrs.size === 1)
    assert(eventEx.attrs(name) === RegexNumericPeriodTestStub.ExactValLong)
  }

  test("testWithValue (name: String, value: T) no exception with 'value' == 0 and 'name' is not empty") {
    val name = "ABCdef"
    val value = 0
    TestEventExFactory().withValue(name, value)
  }


  test("testWithValue (name: String, value: T) must throw exception when 'name' is empty") {
    val name = ""
    val value = 123
    intercept[IllegalArgumentException] {
      TestEventExFactory().withValue(name, value)
    }
  }

  test("testWithValue (name: String, value: T) must throw exception with 'value' < 0") {
    val name = "ABCdef"
    val value = -123
    intercept[IllegalArgumentException] {
      TestEventExFactory().withValue(name, value)
    }
  }

  test("testWithValue(name: String, value: String)") {
    val name = "ABCdef"
    val value = "ghiJKL"
    val eventEx = TestEventExFactory().withValue(name, value)
    assert(eventEx.attrs.size === 1)
    assert(eventEx.attrs(name) === value)
  }

  test("testWithValue (name: String, value: T) no exception with 'value' is empty and 'name' is not empty") {
    val name = "ABCdef"
    val value = ""
    TestEventExFactory().withValue(name, value)
  }

  test("testWithValue(name: String, value: String) must throw exception when 'name' is empty") {
    val name = ""
    val value = "ghiJKL"
    intercept[IllegalArgumentException] {
      TestEventExFactory().withValue(name, value)
    }
  }

  test("testWithRange(name: String, from: T, until: T) T is Byte ") {
    val name = "ABCdef"
    val (from, until): (Byte, Byte) = (1, Byte.MaxValue)
    val eventEx = TestEventExFactory().withRange(name, from, until)
    assert(eventEx.attrs(name) === RegexNumericPeriodTestStub.FromToValByte)
  }

  test("testWithRange(name: String, from: T, until: T) T is Int ") {
    val name = "ABCdef"
    val (from, until): (Int, Int) = (1, Int.MaxValue)
    val eventEx = TestEventExFactory().withRange(name, from, until)
    assert(eventEx.attrs(name) === RegexNumericPeriodTestStub.FromToValInt)
  }

  test("testWithRange(name: String, from: T, until: T) T is Long ") {
    val name = "ABCdef"
    val (from, until): (Long, Long) = (1, Long.MaxValue)
    val eventEx = TestEventExFactory().withRange(name, from, until)
    assert(eventEx.attrs(name) === RegexNumericPeriodTestStub.FromToValLong)
  }

  test("testWithRange: no exception when 'from' == 0 and and 'name' is not empty and 'from' < 'until'") {
    val name = "ABCdef"
    val (from, until) = (0, 123)
    TestEventExFactory().withRange(name, from, until)
  }

  test("testWithRange must throw exception when 'name' is empty") {
    val name = ""
    val (from, until) = (1, 5)
    intercept[IllegalArgumentException] {
      TestEventExFactory().withRange(name, from, until)
    }
  }

  test("testWithRange must throw exception when 'from' < 0") {
    val name = "ABCdef"
    val (from, until) = (-5, Long.MaxValue)
    intercept[IllegalArgumentException] {
      TestEventExFactory().withRange(name, from, until)
    }
  }

  test("testWithRange must throw exception when 'from' > 'until'") {
    val name = "ABCdef"
    val (from, until) = (5, 1)
    intercept[IllegalArgumentException] {
      TestEventExFactory().withRange(name, from, until)
    }
  }

  test("testWithValue and testWithRange: several attributes") {
    val eventEx = TestEventExFactory()
      .withValue("Attr1", Long.MaxValue)
      .withValue("Attr2", "value string")
      .withRange("Attr3", 5, 678)
    //    println(eventEx.attrs)
    assert(eventEx.attrs.size === 3)
    assert(eventEx.attrs("Attr1") === RegexNumericPeriodTestStub.ExactValLong)
    assert(eventEx.attrs("Attr2") === "value string")
    assert(eventEx.attrs("Attr3") === RegexNumericPeriodTestStub.FromToValInt)
  }

  test("testTranslate: event expression is empty") {
    val eventEx = new EventEx(RegexNumericPeriod.ByteIntLongConverters, None, None, Map())
    assert(eventEx.translate() ===
      "<[^>]+?@\\d+#[^>]*?>")
  }

  test("testTranslate: all fields are empty") {
    val eventEx = new EventEx(RegexNumericPeriod.ByteIntLongConverters, Some(""), Some(""), Map("" -> ""))
    assert(eventEx.translate() ===
      "<@#[^>]*?&%[^>]*?>")
  }

  test("testTranslate: one attribute") {
    val eventEx = new EventEx(RegexNumericPeriod.ByteIntLongConverters, Some("ActivityName"), Some("Timestamp"),
      Map("AttrName1" -> "AttrValue1"))
    assert(eventEx.translate() ===
      "<ActivityName@Timestamp#[^>]*?&AttrValue1%[^>]*?>")
  }

  test("testTranslate: several attributes") {
    val eventEx = new EventEx(RegexNumericPeriod.ByteIntLongConverters, Some("ActivityName"), Some("Timestamp"),
      Map("AttrName1" -> "AttrValue1", "AttrName2" -> "AttrValue2", "AttrName3" -> "AttrValue3"))
    assert(eventEx.translate() ===
      "<ActivityName@Timestamp#[^>]*?&AttrValue1%[^>]*?&AttrValue2%[^>]*?&AttrValue3%[^>]*?>")
  }

  test("testTranslate: attribute with name and without value") {
    val eventEx = new EventEx(RegexNumericPeriod.ByteIntLongConverters, Some("ActivityName"), Some("Timestamp"),
      Map("AttrName1" -> ""))
    assert(eventEx.translate() ===
      "<ActivityName@Timestamp#[^>]*?&%[^>]*?>")
  }

  test("testApply(activityName)"){
    val eventEx = EventEx.apply("ActivityName")
    assert(eventEx.activity === Some("ActivityName"))
    assert(eventEx.timestamp === None)
    assert(eventEx.attrs === Map())
  }

  test("testApply(activityName): 'activityName' is empty"){
    val eventEx = EventEx.apply("")
    assert(eventEx.activity === Some(""))
    assert(eventEx.timestamp === None)
    assert(eventEx.attrs === Map())
  }

  test("testApply()"){
    val eventEx = EventEx.apply()
    assert(eventEx.activity === None)
    assert(eventEx.timestamp === None)
    assert(eventEx.attrs === Map())
  }
}
