package org.processmining.scala.applications.mhs.bhs.t3.utils

import java.util.regex.Pattern

import org.apache.spark.rdd.RDD
import org.processmining.scala.log.common.types._
import org.processmining.scala.log.common.unified.log.spark.UnifiedEventLog
import org.processmining.scala.log.common.utils.common.types.EventWithClazz

private[bhs] object RecirculationDetector {
  def loadSorterRecirculationClazz[E](
                                       log: UnifiedEventLog,
                                       patternString: String
                                     ): RDD[EventWithClazz] = {
    val pattern = Pattern.compile(patternString)
    log.traces.map { t =>
      t._2.filter(x => pattern.matcher(x.activity).matches())
        .zipWithIndex
        .map { e =>
          EventWithClazz(
            t._1.id,
            e._1.timestamp,
            e._2 + 1)
        }
    }
      .flatMap { x => x }
  }
}
