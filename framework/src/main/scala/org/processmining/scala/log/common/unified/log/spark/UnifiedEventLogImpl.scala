package org.processmining.scala.log.common.unified.log.spark

import java.util.regex.Pattern

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.processmining.scala.log.common.filtering.expressions.events.regex.EventExpression
import org.processmining.scala.log.common.filtering.expressions.events.common.AbstractTracePredicate
import org.processmining.scala.log.common.filtering.expressions.traces.{AbstractTraceExpression, SubtraceExpression}
import org.processmining.scala.log.common.unified.event.UnifiedEvent
import org.processmining.scala.log.common.unified.log.common.UnifiedEventLog.UnifiedTrace
import org.processmining.scala.log.common.unified.trace.UnifiedTraceId

import scala.reflect.ClassTag

private[spark] class UnifiedEventLogImpl(override val traces: RDD[UnifiedTrace],
                                         val debug: String => Unit = { _ => }
                                        ) extends UnifiedEventLog {

  override def persist(newLevel: StorageLevel) = {
    traces.persist(newLevel)
    this
  }

  override def unpersist() = {
    traces.unpersist()
    this
  }

  override def withDebugInfo(debug: String => Unit = println(_)) = new UnifiedEventLogImpl(traces, debug)

  override def withoutDebugInfo() = new UnifiedEventLogImpl(traces)

  override def map(ex: AbstractTraceExpression): UnifiedEventLog =
    map(ex.transform(_))

  override def map(f: UnifiedTrace => UnifiedTrace): UnifiedEventLog =
    UnifiedEventLog.fromTraces(
      traces
        .map(f)
        .filter(_._2.nonEmpty)
    )

  override def flatMap(f: UnifiedTrace => List[UnifiedTrace]): UnifiedEventLog =
    UnifiedEventLog.fromTraces(
      traces
        .flatMap(f)
        .filter(_._2.nonEmpty)
    )

  override def flatMap(ex: SubtraceExpression): UnifiedEventLog =
    flatMap(ex.transform(_))


  override def filter(ex: AbstractTracePredicate): UnifiedEventLog =
    UnifiedEventLog
      .fromTraces(
        traces
          .filter(ex.evaluate)
          .filter(_._2.nonEmpty))

  //  override def filter(schema: StructType): UnifiedEventLog =
  //    UnifiedEventLog
  //      .fromTraces(traces
  //          .map(t => (t._1,  t._2.filter(_.attrs.schema == schema)))
  //          .filter(_._2.nonEmpty)
  //      )


  override def fullOuterJoin(that: UnifiedEventLog): UnifiedEventLog =
    UnifiedEventLog.fromTraces(
      traces
        .fullOuterJoin(that.traces)
        .mapValues(x => {
          val l1 = if (x._1.isDefined) x._1.get else List[UnifiedEvent]()
          val l2 = if (x._2.isDefined) x._2.get else List[UnifiedEvent]()
          (l1 ::: l2.filter(!l1.contains(_)))
            .sortBy(_.timestamp)
        }
        )
    )

  override def join(that: UnifiedEventLog): UnifiedEventLog =
    UnifiedEventLog.fromTraces(
      traces
        .join(that.traces)
        .mapValues(x => {
          val l1 = x._1
          val l2 = x._2
          (l1 ::: l2.filter(!l1.contains(_)))
            .sortBy(_.timestamp)
        }
        )
    )

  override def sample(withReplacement: Boolean, fraction: Double, seed: Long): UnifiedEventLog =
    UnifiedEventLog.fromTraces(traces.sample(withReplacement, fraction, seed))


  override def filterByIds(that: UnifiedEventLog, keepContained: Boolean): UnifiedEventLog = {
    val ids = that.traces().keys.map(_.id).collect().toSet
    UnifiedEventLog.fromTraces(traces.filter(x => if (keepContained) ids.contains(x._1.id) else !ids.contains(x._1.id)))
  }

  override def events(): RDD[(UnifiedTraceId, UnifiedEvent)] =
    traces.flatMap(t => t._2.map((t._1, _)))

  override def minMaxTimestamp(): (Long, Long) = {
    val timestamps = traces
      .flatMap(t => t._2.map(_.timestamp))
      .persist()

    val min = timestamps.takeOrdered(1)(Ordering[Long])
    val max = timestamps.takeOrdered(1)(Ordering[Long].reverse)
    if (min.isEmpty) (0, 0) else (min(0), max(0))
  }


  override def filterByAttributeNames(attrNames: Set[String]) =
    UnifiedEventLog
      .fromTraces(
        traces
          .mapValues(_.filter(_.hasAttributes(attrNames)))
          .filter(_._2.nonEmpty)


      )

  override def project(ex: EventExpression*): UnifiedEventLog = projectImpl(true, ex: _*)

  override def remove(ex: EventExpression*): UnifiedEventLog = projectImpl(false, ex: _*)

  private def projectImpl(include: Boolean, ex: EventExpression*): UnifiedEventLog = {
    val patterns = ex
      .map(_.translate)
      .map(x => {
        debug(x)
        Pattern.compile(x)
      }
      )
    //TODO: create fromEvent with UnifiedTraceID to avoid the last transformation
    UnifiedEventLog.create(events.filter(x => {
      val exists = patterns.exists(_.matcher(x._2.regexpableForm()).matches())
      if (include) exists else !exists
    }

    ).map(x => (x._1.id, x._2)))
  }

  override def filterByTraceIds(ids: String*): UnifiedEventLog =
    UnifiedEventLog.fromTraces(traces.filter(x => ids.contains(x._1.id)))


  //  //TODO: move to combineByKey
  //  override def aggregate(f: List[UnifiedEvent] => List[(String, String)]): Array[((String, String), Int)] =
  //    traces
  //      .flatMap(x => f(x._2))
  //      .map((_, 1))
  //      .groupByKey()
  //      .mapValues(_.sum)
  //      .collect()
  //
  //  override def aggregate2(f: List[UnifiedEvent] => List[(String, String)]): Array[((String, String), Int)] = {
  //    val createCombiner: Int => Int = x => x
  //    val scoreCombiner: (Int, Int) => Int = _ + _
  //    val scoreMerger = scoreCombiner
  //
  //    traces
  //      .flatMap(x => f(x._2))
  //      .map((_, 1))
  //      .combineByKey(createCombiner, scoreCombiner, scoreMerger)
  //      .collect()
  //  }

  @deprecated
  override def aggregate[T: ClassTag](f: List[UnifiedEvent] => List[T]): Array[(T, Int)] = {
    val createCombiner: Int => Int = x => x
    val scoreCombiner: (Int, Int) => Int = _ + _
    val scoreMerger = scoreCombiner
    traces
      .flatMap(x => f(x._2))
      .map((_, 1))
      .combineByKey(createCombiner, scoreCombiner, scoreMerger)
      .collect()
  }

  override def combineAndReduce[T: ClassTag, V: ClassTag, S: ClassTag](f: List[UnifiedEvent] => List[((String, String), T)],
                                                                       createCombiner: (T => V),
                                                                       scoreCombiner: (V, T) => V,
                                                                       scoreMerger: (V, V) => V,
                                                                       reducer: (((String, String), V) => S)
                                                                      ): Array[((String, String), S)] =
    traces
      .flatMap(x => f(x._2))
      .combineByKey(createCombiner, scoreCombiner, scoreMerger)
      .map(x => (x._1, reducer(x._1, x._2)))
      .collect()

  override def mapIf(p: AbstractTracePredicate, exIfYes: AbstractTraceExpression, exIfNo: AbstractTraceExpression): UnifiedEventLog =
    UnifiedEventLog.fromTraces(traces.map(t => if (p.evaluate(t)) exIfYes.transform(t) else exIfNo.transform(t)))
}


private[spark] object UnifiedEventLogImpl {
  def groupAndSort(events: RDD[(UnifiedTraceId, UnifiedEvent)]): RDD[UnifiedTrace] =
    events
      .groupByKey
      .mapValues(_.toList.sortBy(_.timestamp))
}