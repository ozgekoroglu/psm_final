//package org.processmining.scala.applications.bp.bpi_challenge.scenarios.parallel.dev
//
//import java.io._
//import java.time.Duration
//
//import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
//import org.apache.spark.sql.types.{StringType, StructField, StructType}
//import org.processmining.scala.applications.bp.bpi_challenge.scenarios.parallel.TemplateForBpi2017CaseStudy
//import org.processmining.scala.log.common.csv.parallel.CsvWriter
//import org.processmining.scala.log.common.unified.event.{UnifiedEvent, UnifiedEventImpl}
//import org.processmining.scala.log.common.unified.log.parallel.UnifiedEventLog
//import org.processmining.scala.log.common.unified.trace.UnifiedTraceIdImpl
//
//import scala.util.Random
//
//object LogCloner extends TemplateForBpi2017CaseStudy {
//  val scale = 25000
//  val daysDelta = 30
//  val maxAmount = 50000
//  val dir = s"${caseStudyConfig.outDir}/log"
//  private val _ = new File(dir).mkdirs() // create out dir
//  val w = new PrintWriter(new BufferedWriter(new FileWriter(dir + "/log.csv")))
//  val SourceAttributesSchema = new StructType(Array(StructField("offeredAmount", StringType, false)))
//
//  val activityMap: Map[String, Int] = logOffers
//    .events()
//    .map(_._2.activity)
//    .distinct
//    .zipWithIndex
//    .map(x => x._1 -> x._2)
//    .seq
//    .toMap
//
//  def generateEvent(e: UnifiedEvent, delta: Long, amount: Int): UnifiedEvent =
//    UnifiedEventImpl(e.timestamp + delta,
//      activityMap(e.activity).toString,
//      new GenericRowWithSchema(Array(amount.toString + ".0"), SourceAttributesSchema),
//      None)
//
//  def getDelta(r: Random): Long =
//    (r.nextInt(Duration.ofDays(daysDelta).toHours.toInt) * (if (r.nextInt(2) == 0) 1 else -1)).toLong * 3600000L
//
//  def generateAndSaveTraces(id: Int, events: List[UnifiedEvent], index: Int): Unit = {
//    println(index)
//    val r = Random
//    r.setSeed(id)
//    val eventsToSave = (0 until scale)
//      .map((_, r.nextInt(maxAmount)))
//      .map(pair => (new UnifiedTraceIdImpl(s"${id}_${pair._1}"),
//        events.map(generateEvent(_, getDelta(r), pair._2))))
//
//    CsvWriter.logToCsvLocalFilesystem(
//      w,
//      UnifiedEventLog
//        .fromTraces(eventsToSave),
//      s"$dir/log_$id.csv",
//      csvExportHelper.timestamp2String,
//      SourceAttributesSchema, index == 0, false)
//  }
//
//  def main(args: Array[String]): Unit = {
//    logOffers
//      .traces()
//      .zipWithIndex
//      .map(t => (t._2, t._1._2))
//      .zipWithIndex
//      .seq
//      .foreach(t => generateAndSaveTraces(t._1._1, t._1._2, t._2))
//    w.close()
//  }
//
//}
