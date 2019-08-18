package org.processmining.scala.log.common.utils.common.types

import org.processmining.scala.log.common.types._

@deprecated
class Event(override val id: String, override val activity: String, override val timestamp: Long)
  extends Serializable with Id with Timestamp with Activity{

  override def toString: String = s"""$timestamp "$id": "$activity""""

  //override def toCsv(msToStringTimestamp: Long => String): String = s""""$id";"${msToStringTimestamp(timestamp)}";"$activity""""

  //override def csvHeader: String = """"Id";"Timestamp";"Activity""""


  def canEqual(other: Any): Boolean = other.isInstanceOf[Event]

  override def equals(other: Any): Boolean = other match {
    case that: Event =>
      (that canEqual this) &&
        id == that.id &&
        activity == that.activity &&
        timestamp == that.timestamp
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(id, activity, timestamp)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}
@deprecated
object Event {
  def apply(id: String, activity: String, timestamp: Long) = new Event(id, activity, timestamp)
}

@deprecated
case class EventWithClazz(id: String, timestamp: Long, clazz: Int) extends Serializable with Id with Timestamp with Clazz


