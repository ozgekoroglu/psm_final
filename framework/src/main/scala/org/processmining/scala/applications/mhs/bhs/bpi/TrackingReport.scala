package org.processmining.scala.applications.mhs.bhs.bpi

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.processmining.scala.applications.mhs.bhs.bpi.FailedDirectionError.FailedDirectionError
import org.processmining.scala.applications.mhs.bhs.t3.eventsources.{BpiImporter, TrackingReportSchema}
import org.processmining.scala.log.common.unified.event.UnifiedEvent
import org.processmining.scala.log.common.unified.trace.UnifiedTraceId

import scala.collection.immutable.SortedMap

private[bhs] object FailedDirectionError extends Enumeration {
  type FailedDirectionError = Value
  val NoError,
  NotAllowedFlowControl,
  NoDirectionLmRouting,
  NotAvailableOrFull,
  NotAllowedBlockedByLm,
  NotAllowedSecurity,
  CapacityTooHigh,
  TechnicalFailure,
  NotAllowedForcedDirection,
  NotAllowedDimensions,
  Unknown,
  Last
  = Value
}

private[bhs] object TrackingReportEvent {
  def createArtificialEvent(location: String, delta: Long, id: UnifiedTraceId, e: UnifiedEvent): UnifiedEvent =
    UnifiedEvent(
      e.timestamp + delta,
      location,
      SortedMap(TrackingReportSchema.AttrNameFailedDirectionClass -> 0.asInstanceOf[Byte]),
      None)

  private val patternNoError = "()".r
  private val patternNotAllowedFlowControl = ".*(FailedReason=NOT ALLOWED - FLOW CONTROL).*".r
  private val patternNoDirectionLmRouting = ".*(FailedReason=NO DIRECTION - LM ROUTING).*".r
  private val patternNotAvailableOrFull = ".*(FailedReason=NOT AVAILABLE OR FULL).*".r
  private val patternNotAllowedBlockedByLm = ".*(FailedReason=NOT ALLOWED - BLOCKED BY LM).*".r
  private val patternNotAllowedSecurity = ".*(FailedReason=NOT ALLOWED - SECURITY).*".r
  private val patternCapacityTooHigh = ".*(FailedReason=CAPACITY TOO HIGH).*".r
  private val patternTechnicalFailure = ".*(FailedReason=TECHNICAL FAILURE).*".r
  private val patternNotAllowedForcedDirection = ".*(FailedReason=NOT ALLOWED - FORCED DIRECTION).*".r
  private val patternNotAllowedDimensions = ".*(FailedReason=NOT ALLOWED - DIMENSIONS).*".r

  def failedDirectionClassifier(x: String): FailedDirectionError =
    x match {
      case patternNoError(_) => FailedDirectionError.NoError
      case patternNotAllowedFlowControl(_) => FailedDirectionError.NotAllowedFlowControl
      case patternNoDirectionLmRouting(_) => FailedDirectionError.NoDirectionLmRouting
      case patternNotAvailableOrFull(_) => FailedDirectionError.NotAvailableOrFull
      case patternNotAllowedBlockedByLm(_) => FailedDirectionError.NotAllowedBlockedByLm
      case patternNotAllowedSecurity(_) => FailedDirectionError.NotAllowedSecurity
      case patternCapacityTooHigh(_) => FailedDirectionError.CapacityTooHigh
      case patternTechnicalFailure(_) => FailedDirectionError.TechnicalFailure
      case patternNotAllowedForcedDirection(_) => FailedDirectionError.NotAllowedForcedDirection
      case patternNotAllowedDimensions(_) => FailedDirectionError.NotAllowedDimensions
      case _ => FailedDirectionError.Unknown
    }
}
