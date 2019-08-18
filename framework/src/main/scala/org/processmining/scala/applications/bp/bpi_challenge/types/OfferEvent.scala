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


private[bpi_challenge] object OfferEvent {

  val OfferAttributesSchema = new StructType(Array(
    StructField("offeredAmount", StringType, false),
    StructField("offeredAmountClass", ByteType, false)
  ))


  def createArtificialEvent(activity: String, delta: Long, id: UnifiedTraceId, e: UnifiedEvent): UnifiedEvent = {
    val zeroClazz: Byte = 0
    UnifiedEvent(
      e.timestamp + delta,
      activity,
      SortedMap("offeredAmount" -> "0.0", "offeredAmountClass" -> zeroClazz),
      None)
  }

  private val importCsvHelper = new CsvImportHelper("yyyy/MM/dd HH:mm:ss.SSS", CsvExportHelper.AmsterdamTimeZone)
  //private val importCsvHelperBillion = new CsvImportHelper("dd-MM-yy HH.mm.ss.SSS", CsvExportHelper.AmsterdamTimeZone)

  private def factory(importHelper: CsvImportHelper, caseIdIndex: Int, completeTimestampIndex: Int, activityIndex: Int, offeredAmountIndex: Int, row: Array[String]): (String, UnifiedEvent) =
    (
      row(caseIdIndex),
      UnifiedEvent(
        importHelper.extractTimestamp(row(completeTimestampIndex)),
        row(activityIndex),
        SortedMap("offeredAmount" -> row(offeredAmountIndex), "offeredAmountClass" -> offeredAmountClassifier(row(offeredAmountIndex))),
        None))

  //Spark
  def loadFromCsv(
                   spark: SparkSession,
                   csvReader: CsvReader,
                   inDir: String): RDD[(String, UnifiedEvent)] = {

    val (header, lines) = csvReader.read(s"$inDir/offers.csv", spark)
    csvReader.parse(
      lines,
      factory(importCsvHelper, header.indexOf("Case ID"),
        header.indexOf("Complete Timestamp"),
        header.indexOf("Activity"),
        header.indexOf("(case) OfferedAmount"), _: Array[String])
    )
  }

  private def getFileNames(dir: String): Array[String] = {
    new File(dir)
      .listFiles()
      .filter(_.isFile)
      .filter(_.getName().toLowerCase().endsWith(".csv"))
      .map(s"$dir/" + _.getName())

  }


  //Spark
  //  def loadFromCsvDir(
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
  //  def loadFromCsvBillion(
  //                          spark: SparkSession,
  //                          csvReader: CsvReader,
  //                          inDir: String): RDD[(String, UnifiedEvent)] = {
  //
  //    val (header, lines) = csvReader.read(s"$inDir/offers.csv", spark)
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
  def loadFromCsv(
                   csvReader: org.processmining.scala.log.common.csv.parallel.CsvReader,
                   inDir: String): ParSeq[(String, UnifiedEvent)] = {

    val (header, lines) = csvReader.read(s"$inDir/offers.csv")
    csvReader.parse(
      lines,
      factory(importCsvHelper, header.indexOf("Case ID"),
        header.indexOf("Complete Timestamp"),
        header.indexOf("Activity"),
        header.indexOf("(case) OfferedAmount"), _: Array[String])
    )
  }

  def offeredAmountClassifier(offeredAmount: String): Byte =
    if (offeredAmount.isEmpty) 0 else offeredAmountClassifier(offeredAmount.toDouble)

  def offeredAmountClassifier(offeredAmount: Double): Byte =
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
    }

  val ClazzCount = 10

}
