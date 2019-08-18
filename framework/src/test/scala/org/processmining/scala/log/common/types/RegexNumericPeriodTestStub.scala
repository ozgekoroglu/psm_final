package org.processmining.scala.log.common.types

class RegexNumericPeriodTestStub(val exactVal: String, val fromToVal: String) extends AbstractRegexNumericPeriod {

  override def apply(from: Long, to: Long): String = fromToVal

  override def apply(exactTime: Long): String = exactVal
}

object RegexNumericPeriodTestStub {

  val ExactValByte: String = "ExactValByte"
  val FromToValByte: String = "FromToValByte"
  val ExactValInt: String = "ExactValInt"
  val FromToValInt: String = "FromToValInt"
  val ExactValLong: String = "ExactValLong"
  val FromToValLong: String = "FromToValLong"

  val ByteConverter = new RegexNumericPeriodTestStub(ExactValByte, FromToValByte)
  val IntegerConverter = new RegexNumericPeriodTestStub(ExactValInt, FromToValInt)
  val LongConverter = new RegexNumericPeriodTestStub(ExactValLong, FromToValLong)
  val ByteIntLongConverters = (ByteConverter, IntegerConverter, LongConverter)
}


