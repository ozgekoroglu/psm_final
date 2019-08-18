package org.processmining.scala.log.dev

import java.util.regex.Pattern

import org.processmining.scala.log.common.types.RegexNumericPeriod

private object RegexTimePeriodExample {


  def check(from: Long, to: Long, width: Int, ex: Pattern, format: String, max: Long) = {

    (from to to).par.map {
      x =>
        val matching = ex.matcher(x.formatted(format)).matches()
        assert(matching, println(s"should match: $x"))

    }
    (0L until from).par.map {
      x =>
        val matching = ex.matcher(x.formatted(format)).matches()
        assert(!matching, println(s"wrong matching (L) for: $x"))

    }

    (Math.min(to + 1, max) until max).par.map {
      x =>
        val matching = ex.matcher(x.formatted(format)).matches()
        assert(!matching, println(s"wrong matching (R) for: $x"))

    }

  }

  def main(args: Array[String]): Unit = {
    val width = 3
    val format: String = s"%0${width}d"
    val max: Long = Math.pow(10, width).toLong - 1
    val tp = new RegexNumericPeriod(width)

    (0L until max)
      .par
      .map { from =>
        (from + 1 to max)
          .par
          .map {
            to =>
              val exString = tp(from, to)
              //println(exString)
              val ex = Pattern.compile(exString)
              check(from, to, width, ex, format, max)
          }
      }
  }

}
