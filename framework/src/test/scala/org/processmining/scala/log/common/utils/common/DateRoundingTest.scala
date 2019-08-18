package org.processmining.scala.log.common.utils.common

import java.time.{Instant, ZoneId, ZonedDateTime}

import org.scalatest.FunSuite

class DateRoundingTest extends FunSuite {

  test("testTruncatedToWeeks") {
    val date = 1522847495000L
    val truncated = DateRounding.truncatedToWeeks(ZoneId.of("Europe/Amsterdam"), date)
    val x = ZonedDateTime.
      ofInstant(Instant.ofEpochMilli(truncated), ZoneId.of("Europe/Amsterdam"))
    println(x)


  }

}
