//import org.processmining.scala.applications.mhs.bhs.t3.eventsources.BpiImporter
//import org.processmining.scala.applications.mhs.bhs.t3.scenarios.spark._
//import org.processmining.scala.log.common.enhancment.segments.common.{PreprocessingSession, Q3DurationClassifier, Q4DurationClassifier}
//import org.processmining.scala.log.common.enhancment.segments.spark.DurationSegmentProcessor
//import org.processmining.scala.log.common.filtering.expressions.events.regex.EventEx
//import org.processmining.scala.log.common.filtering.expressions.traces.{TraceExpression, _}
//import org.processmining.scala.log.common.unified.event.CommonAttributeSchemas
//import org.processmining.scala.log.common.unified.trace.UnifiedEventAggregator
//import org.processmining.scala.log.common.utils.common.EventAggregatorImpl
//import org.processmining.scala.applications.mhs.bhs.t3.eventsources.TrackingReportSchema
//import org.processmining.scala.applications.mhs.bhs.t3.eventsources.TaskReportSchema
//import org.processmining.scala.log.common.filtering.expressions.events.variables.EventVar
//import java.time.Duration
//
////val t = org.processmining.scala.log.common.filtering.expressions.traces.TraceExpression()
//
//object EvaluationEnvironment extends T3Session(
//  "C:\\evaluation\\input\\20171223", // folder with logs
//  "C:\\evaluation\\output\\1m/20171223Q3I", // output folder
//  "23-12-2017 01:00:00", //start date and time, UTC timezone
//  "24-12-2017 01:00:00", // end date and time, UTC timezone
//  60000 //time window size (ms) for Performance Spectrum Miner
//) {
//  def main(args: Array[String]): Unit = {
//    EvaluationEnvironment.logger.info("EvaluationEnvironment started")
//    report(new Q3DurationClassifier())
//
//  }
//}
//
