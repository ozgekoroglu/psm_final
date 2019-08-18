package org.processmining.scala.log.common.unified.log.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.processmining.scala.log.common.filtering.expressions.events.regex.EventExpression
import org.processmining.scala.log.common.filtering.expressions.events.common.AbstractTracePredicate
import org.processmining.scala.log.common.filtering.expressions.traces.{AbstractTraceExpression, SubtraceExpression}
import org.processmining.scala.log.common.unified.event.UnifiedEvent
import org.processmining.scala.log.common.unified.log.common.UnifiedEventLog.UnifiedTrace
import org.processmining.scala.log.common.unified.trace.{UnifiedTraceId, UnifiedTraceIdImpl}

import scala.collection.parallel.ParSeq
import scala.reflect.ClassTag

/**
  * Spark-based interface of event logs
  */
trait UnifiedEventLog extends Serializable {

  /** returns traces */
  def traces(): RDD[UnifiedTrace]

  /** returns number of traces */
  def count(): Long = traces().count()

  /** returns events */
  def events(): RDD[(UnifiedTraceId, UnifiedEvent)]

  /** sets StorageLevel (see RDD.persist) */
  def persist(newLevel: StorageLevel = StorageLevel.MEMORY_ONLY): UnifiedEventLog

  /** unpersists the log from memory (see RDD.unpersist) */
  def unpersist(): UnifiedEventLog

  /** returns a new log with debug print enabled */
  def withDebugInfo(debug: String => Unit = println(_)): UnifiedEventLog

  /** returns a new log with debug print disabled */
  def withoutDebugInfo(): UnifiedEventLog

  //type LogEvent = Traceable

  /** transforms every trace according to expression provided */
  def map(ex: AbstractTraceExpression): UnifiedEventLog

  /** transforms every trace according to function provided */
  def map(f: UnifiedTrace => UnifiedTrace): UnifiedEventLog

  /** transforms every trace according to expressions provided*/
  def mapIf(p: AbstractTracePredicate, exIfYes: AbstractTraceExpression, exIfNo: AbstractTraceExpression): UnifiedEventLog

  /** transforms every trace according to expression provided */
  def flatMap(ex: SubtraceExpression): UnifiedEventLog

  /** transforms every trace into several ones according to function provided */
  def flatMap(f: UnifiedTrace => List[UnifiedTrace]): UnifiedEventLog

  /** filters traces according to predicate provided */
  def filter(ex: AbstractTracePredicate): UnifiedEventLog

  /** filters events to keep in only events that have the names provided */
  def filterByAttributeNames(attrNames: Set[String]): UnifiedEventLog

  /** filters events to keep in only events that have the names provided */
  def filterByAttributeNames(attrNames: String*): UnifiedEventLog = filterByAttributeNames(attrNames.toSet)

  /** filters traces: keeps in traces with schema provided */
  //def filter(schema: StructType): UnifiedEventLog

  /** implements full outer join of 2 logs. Duplicated events will be removed */
  def fullOuterJoin(that: UnifiedEventLog): UnifiedEventLog

  /** implements join of 2 logs. Duplicated events will be removed */
  def join(that: UnifiedEventLog): UnifiedEventLog

  /** keep only traces which IDs are in 'that' log
    * similar to method join but doesn't take into account events of 'that'
    * */
  def filterByIds(that: UnifiedEventLog, keepContained: Boolean): UnifiedEventLog

  /** returns min and max timestamps or (0, 0) for empty log */
  def minMaxTimestamp(): (Long, Long)

  /** keep events that match provided patterns */
  def project(ex: EventExpression*): UnifiedEventLog

  /** remove events that match provided patterns */
  def remove(ex: EventExpression*): UnifiedEventLog

  /** keep traces with IDs identical to provided ones */
  def filterByTraceIds(ids: String*): UnifiedEventLog

  //  @deprecated
  //  def aggregate(f: List[UnifiedEvent] => List[(String, String)]): Array[((String, String), Int)]
  //
  //  @deprecated
  //  def aggregate2(f: List[UnifiedEvent] => List[(String, String)]): Array[((String, String), Int)]

  @deprecated
  def aggregate[T: ClassTag](f: List[UnifiedEvent] => List[T]): Array[(T, Int)]

  def combineAndReduce[T: ClassTag, V: ClassTag, S: ClassTag](f: List[UnifiedEvent] => List[((String, String), T)],
                                                              createCombiner: (T => V),
                                                              scoreCombiner: (V, T) => V,
                                                              scoreMerger: (V, V) => V,
                                                              reducer: (((String, String), V) => S)
                                                             ): Array[((String, String), S)]


  def sample(withReplacement: Boolean, fraction: Double, seed: Long): UnifiedEventLog

}

/** Factory for event log objects */
object UnifiedEventLog {

    /**
    * Creates a new event log from RDD of events
    *
    * @param events pairs of trace ID and events
    * @return a new event log object
    */
  def create(events: RDD[(String, UnifiedEvent)]): UnifiedEventLog =
    UnifiedEventLog.fromTraces(
      UnifiedEventLogImpl.groupAndSort(
        events
          .map(x => (new UnifiedTraceIdImpl(x._1), x._2))
      )
    )

  /** creates an empty log */
  def createEmpty(spark: SparkSession): UnifiedEventLog =
    new UnifiedEventLogImpl(spark.sparkContext.parallelize(Seq()))

  /**
    * Creates a new event log from RDD of traces
    *
    * @param traces RDD of traces
    * @return a new event log object
    */
  def fromTraces(traces: RDD[UnifiedTrace]): UnifiedEventLog = new UnifiedEventLogImpl(traces)

  /**
    * Creates a new event log from Seq of traces
    *
    * @param traces Seq of traces
    * @param spark  Spark session to be used
    * @return a new event log object
    */
  def fromTraces(traces: Seq[UnifiedTrace], spark: SparkSession): UnifiedEventLog = new UnifiedEventLogImpl(spark.sparkContext.parallelize(traces))

  val DefaultNumSlices = -1

  /**
    * Creates a new event log from ParSeq of traces
    *
    * @param traces ParSeq of traces
    * @param spark  Spark session to be used
    * @return a new event log object
    */
  def fromTraces(traces: ParSeq[UnifiedTrace], spark: SparkSession, numSlices: Int = DefaultNumSlices): UnifiedEventLog = new UnifiedEventLogImpl(
    if (numSlices == DefaultNumSlices) spark.sparkContext.parallelize(traces.seq) else spark.sparkContext.parallelize(traces.seq, numSlices)
  )

  def create(log: UnifiedEventLog): org.processmining.scala.log.common.unified.log.parallel.UnifiedEventLog =
    org.processmining.scala.log.common.unified.log.parallel.UnifiedEventLog.fromTraces(log.traces.collect().toSeq)
}
