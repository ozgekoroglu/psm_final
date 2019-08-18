package org.processmining.scala.applications.bp.bpi_challenge.types

import java.io.File

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.processmining.scala.log.common.csv.spark.CsvReader
import org.processmining.scala.log.common.unified.event.UnifiedEvent
import org.processmining.scala.log.common.unified.trace.UnifiedTraceId
import org.processmining.scala.log.utils.common.csv.common.{CsvExportHelper, CsvImportHelper}

import scala.collection.immutable.SortedMap
import scala.collection.parallel.ParSeq

private[bpi_challenge] object ApplicationEvent {

  // UserGuide: specify name of input file
  val inputFile = "applications_reexport.csv"

  // UserGuide: specify internal names of event types to use
  val ApplicationAttributesSchema = new StructType(Array(
    StructField("requestedAmount", DoubleType, false),
    StructField("requestedAmountClass", ByteType, false),
    StructField("offeredAmount", DoubleType, false),
    StructField("offeredAmountClass", ByteType, false),
    StructField("lifecycle:transition", StringType, false),
    StructField("org:resource", StringType, false)
  ))

  val schema = Array("requestedAmount", "requestedAmountClass", "offeredAmount", "offeredAmountClass", "lifecycle:transition", "org:resource")

  // specific of artificial events
  def createArtificialEvent(activity: String, delta: Long, id: UnifiedTraceId, e: UnifiedEvent): UnifiedEvent = {
    val zeroClazz: Byte = 0
    UnifiedEvent(
      e.timestamp + delta,
      activity,
        // UserGuide: specify default type and value for artificial event
        SortedMap("requestedAmount" -> 0.0,
          "requestedAmountClass" -> zeroClazz,
          "offeredAmount" -> 0.0,
          "offeredAmountClass" -> zeroClazz,
          "lifecycle:transition" -> "COMPLETE",
          "org:resource" -> ""),
      None)
  }

  private val importCsvHelper = new CsvImportHelper("yyyy/MM/dd HH:mm:ss.SSS", CsvExportHelper.AmsterdamTimeZone)
  private val importCsvHelperBillion = new CsvImportHelper("dd-MM-yy HH.mm.ss.SSS", CsvExportHelper.AmsterdamTimeZone)

  private def toDouble(row: Array[String], index: Int): Double = {
    val cell = row(index);
    if (cell.isEmpty) Double.NaN else cell.toDouble
  }

  private def factory(
                       importHelper: CsvImportHelper,
                       caseIdIndex: Int,
                       completeTimestampIndex: Int,
                       activityIndex: Int,
                       requestedAmountIndex: Int,
                       offeredAmountIndex: Int,
                       lifeCycleTransitionIndex: Int,
                       resourceIndex: Int,
                       row: Array[String]): (String, UnifiedEvent) =
  {
    //println("parse "+row.mkString(","))
    (
      row(caseIdIndex),
      UnifiedEvent(
        importHelper.extractTimestamp(row(completeTimestampIndex)),
        row(activityIndex),
        SortedMap("requestedAmount" -> toDouble(row,requestedAmountIndex),
          "requestedAmountClass" -> offeredAmountClassifier(toDouble(row,requestedAmountIndex)),
          "offeredAmount" -> toDouble(row,offeredAmountIndex),
          "offeredAmountClass" -> offeredAmountClassifier(toDouble(row,offeredAmountIndex)),
          "lifecycle:transition" -> row(lifeCycleTransitionIndex),
          "org:resource" -> row(resourceIndex)),
        None))
  }
  //Spark
  def loadEventsFromCsv(
                   spark: SparkSession,
                   csvReader: CsvReader,
                   inDir: String): RDD[(String, UnifiedEvent)] = {

    // UserGuide: specify input file for analysis using Spark
    val (header, lines) = csvReader.read(s"$inDir/"+inputFile, spark)
    val events = csvReader.parse(
      lines,
      // UserGuide: specify mapping from column names to event type
      factory(importCsvHelper, header.indexOf("case"),
        header.indexOf("time"),
        header.indexOf("event"),
        header.indexOf("RequestedAmount"),
        header.indexOf("OfferedAmount"),
        header.indexOf("lifecycle:transition"),
        header.indexOf("org:resource"),
        _: Array[String])
    )
    return events
  }

  private def getFileNames(dir: String): Array[String] = {
    new File(dir)
      .listFiles()
      .filter(_.isFile)
      .filter(_.getName().toLowerCase().endsWith(".csv"))
      .map(s"$dir/" + _.getName())

  }


  //Spark
  //  def loadEventsFromCsvDir(
  //                   spark: SparkSession,
  //                   csvReader: CsvReader,
  //                   inDir: String): RDD[(String, UnifiedEvent)] = {
  //
  //
  //    val (header, lines) = csvReader.read(getFileNames(inDir), spark)
  //    csvReader.parse(
  //      lines,
  //      factory(header.indexOf("id"),
  //        header.indexOf("timestamp"),
  //        header.indexOf("activity"),
  //        header.indexOf("offeredAmount"), _: Array[String])
  //    )
  //  }

  //Spark
  //  def loadEventsFromCsvBillion(
  //                          spark: SparkSession,
  //                          csvReader: CsvReader,
  //                          inDir: String): RDD[(String, UnifiedEvent)] = {
  //
  //    val (header, lines) = csvReader.read(s"$inDir/"+inputFile, spark)
  //    csvReader.parse(
  //      lines,
  //      factory(importCsvHelperBillion,
  //        header.indexOf("id"),
  //        header.indexOf("timestamp"),
  //        header.indexOf("activity"),
  //        header.indexOf("offeredAmount"), _: Array[String])
  //    )
  //  }

  //Collections
  def loadEventsFromCsv(
                   csvReader: org.processmining.scala.log.common.csv.parallel.CsvReader,
                   inDir: String): ParSeq[(String, UnifiedEvent)] = {

    // UserGuide: specify input file for analysis using Scala Collection
    val (header, lines) = csvReader.read(s"$inDir/"+inputFile)
    val events = csvReader.parse(
      lines,
      // UserGuide: specify mapping from column names to event type
      factory(importCsvHelper, header.indexOf("case"),
        header.indexOf("time"),
        header.indexOf("event"),
        header.indexOf("RequestedAmount"),
        header.indexOf("OfferedAmount"),
        header.indexOf("lifecycle:transition"),
        header.indexOf("org:resource"),
        _: Array[String])
    )
    return events
  }

  def offeredAmountClassifier(offeredAmount: Double): Byte =
    if (offeredAmount.isNaN) 0
    else (
      offeredAmount match {
        case amount if 0 until 5000 contains amount => 1
        case amount if 5000 until 10000 contains amount => 2
        case amount if 10000 until 15000 contains amount => 3
        case amount if 15000 until 20000 contains amount => 4
        case amount if 20000 until 25000 contains amount => 5
        case amount if 25000 until 30000 contains amount => 6
        case amount if 30000 until 35000 contains amount => 7
        case amount if 35000 until 45000 contains amount => 8
        case _ => 9
      })

  val ClazzCount = 10

}
