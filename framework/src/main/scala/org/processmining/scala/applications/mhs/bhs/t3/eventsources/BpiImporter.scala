package org.processmining.scala.applications.mhs.bhs.t3.eventsources

import java.io.File

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.processmining.scala.applications.mhs.bhs.bpi.{PackageInfoEvent, PackageInfoWithLpcWithProcessDefinitionName, TaskReportEvent, TrackingReportEvent}
import org.processmining.scala.log.common.csv.spark.CsvReader
import org.processmining.scala.log.common.types._
import org.processmining.scala.log.common.unified.event.UnifiedEvent
import org.processmining.scala.log.common.utils.common.types.EventWithClazz

import scala.collection.immutable.SortedMap

case class BpiImporterConfig(
                                           inDir: String,
                                           bpiCsvHelper: BpiCsvImportHelper,
                                           csvReader: CsvReader,
                                           //timestamp1Ms: Long,
                                           //timestamp2Ms: Long,
                                           twSize: Long,
                                           additionalFilter: String => Boolean
                                         ) extends Serializable

private[bhs] class BpiCsvFilenames(val dir: String, additionalFilter: String => Boolean) extends Serializable {

  private val filesInDir = new File(dir)
    .listFiles()
    .filter(_.isFile)
    .filter(x => additionalFilter(x.getName))
    .filter(_.getName().toLowerCase().endsWith(".csv"))
    .map(_.getName())

  def wcAvailabilitytypereport(): Array[String] = getFileName(BpiCsvFilenames.WcAvailabilitytypereport)

  def wcPackageinfo(): Array[String] = getFileName(BpiCsvFilenames.WcPackageinfo)

  def wcPackagereport(): Array[String] = getFileName(BpiCsvFilenames.WcPackagereport)

  def wcTaskreport(): Array[String] = getFileName(BpiCsvFilenames.WcTaskreport)

  def wcTrackingreport(): Array[String] = getFileName(BpiCsvFilenames.WcTrackingreport)

  private def getFileName(suffix: String): Array[String] = {
    val names = filesInDir.filter(_.toLowerCase().contains(suffix.toLowerCase()))
    if (names.isEmpty)
      throw new IllegalArgumentException(s"Zero files '$suffix' are found")
    names
  }

}

private[bhs] object BpiCsvFilenames {
  private val WcAvailabilitytypereport: String = "WC_AVAILABILITYREPORT"
  private val WcPackageinfo: String = "WC_PACKAGEINFO"
  private val WcPackagereport: String = "WC_PACKAGEREPORT"
  private val WcTaskreport: String = "WC_TASKREPORT"
  private val WcTrackingreport: String = "WC_TRACKINGREPORT"
}


class BpiImporter(val config: BpiImporterConfig) extends Serializable {

  val filenames = new BpiCsvFilenames(config.inDir, config.additionalFilter)

  def loadTaskReportProblematicId(spark: SparkSession, isRegistration: Boolean): RDD[String] = {

    val tasktype = if (isRegistration) "Registration" else "Deregistration"
    val deRegisterReason = if (isRegistration) "UNEXPECTED" else "MISSING"
    case class TaskReport(id: String, timestamp: Long, tasktype: String, de_registerreason: String) extends Serializable with Id with Timestamp

    def factory(idIndex: Int, eventTsIndex: Int, tasktypeIndex: Int, de_registerreasonIndex: Int)(a: Array[String]) =
      TaskReport(a(idIndex), config.bpiCsvHelper.extractOracleTimestampMs(a(eventTsIndex)), a(tasktypeIndex), a(de_registerreasonIndex))

    val filenameList = getFilenamesArray(filenames.wcTaskreport)

    val (header, lines) = config.csvReader.read(filenameList, spark)
    config.csvReader.parse(
      lines,
      factory(header.indexOf("PID"), header.indexOf("EVENTTS"), header.indexOf("TASKTYPE"),
        header.indexOf(if (isRegistration) "REGISTERREASON" else "DEREGISTERREASON"))
    )
      .filter(x => x.id != "0" && x.tasktype == tasktype && x.de_registerreason == deRegisterReason)
      //.filter(e => (e.timestamp >= config.timestamp1Ms) && (e.timestamp <= config.timestamp2Ms))
      .map(_.id)
      .distinct()


  }


  def trackingReportEntryFactory(idIndex: Int, eventTsIndex: Int, areaidIndex: Int, zoneidIndex: Int, equipmentidIndex: Int)(a: Array[String]): (String, UnifiedEvent) =
    (a(idIndex),
      UnifiedEvent(
        config.bpiCsvHelper.extractOracleTimestampMs(a(eventTsIndex)),
        s"${a(areaidIndex)}.${a(zoneidIndex)}.${a(equipmentidIndex)}"))

  private def trackingReportEntryFactoryWithEmptyFailedDirection(idIndex: Int, eventTsIndex: Int, areaidIndex: Int, zoneidIndex: Int, equipmentidIndex: Int)(a: Array[String]): (String, UnifiedEvent) =
    (a(idIndex),
      UnifiedEvent(
        config.bpiCsvHelper.extractOracleTimestampMs(a(eventTsIndex)),
        s"${a(areaidIndex)}.${a(zoneidIndex)}.${a(equipmentidIndex)}",
        SortedMap(TrackingReportSchema.AttrNameFailedDirectionClass -> 0.asInstanceOf[Byte]),
        None))


  private def trackingReportEventFactory(idIndex: Int, eventTsIndex: Int, areaidIndex: Int, zoneidIndex: Int, equipmentidIndex: Int, failedDirectionIndex: Int, ideventIndex: Int)(a: Array[String]): (String, UnifiedEvent) =
    (a(idIndex),
      UnifiedEvent(
        config.bpiCsvHelper.extractOracleTimestampMs(a(eventTsIndex)),
        s"${a(areaidIndex)}.${a(zoneidIndex)}.${a(equipmentidIndex)}",
        SortedMap(TrackingReportSchema.AttrNameFailedDirectionClass -> TrackingReportEvent.failedDirectionClassifier(a(failedDirectionIndex)).id.toByte),
        None))


  def loadTrackingReportFromCsv(spark: SparkSession): RDD[(String, UnifiedEvent)] = {
    val filenameList = getFilenamesArray(filenames.wcTrackingreport)
    val (wcTrackingReportHeader, wcTrackingReportLines) =
      config.csvReader.read(filenameList, spark)
    config.csvReader.parse(
      wcTrackingReportLines,
      trackingReportEventFactory(wcTrackingReportHeader.indexOf("PID"),
        wcTrackingReportHeader.indexOf("EVENTTS"),
        wcTrackingReportHeader.indexOf("AREAID"), wcTrackingReportHeader.indexOf("ZONEID"), wcTrackingReportHeader.indexOf("EQUIPMENTID"),
        wcTrackingReportHeader.indexOf("L_FAILEDDIRECTION"),
        wcTrackingReportHeader.indexOf("IDEVENT")
      )
    )
      .filter(_._1 != "0") //removing all events with zero PID (e.g. events from tubs)
  }

  def fTrackingReport(spark: SparkSession)(x: Unit): RDD[(String, UnifiedEvent)] = loadPackageReportFromCsv(spark)

  def loadPackageReportFromCsv(spark: SparkSession): RDD[(String, UnifiedEvent)] = {

    val filenameList = getFilenamesArray(filenames.wcPackagereport)
    val (wcPackageReportHeader, wcPackageReportLines) = config.csvReader.read(filenameList, spark)
    config.csvReader.parse(
      wcPackageReportLines,
      trackingReportEntryFactory(wcPackageReportHeader.indexOf("PID"),
        wcPackageReportHeader.indexOf("EVENTTS"),
        wcPackageReportHeader.indexOf("AREAID"), wcPackageReportHeader.indexOf("ZONEID"), wcPackageReportHeader.indexOf("EQUIPMENTID"))
    )
      .filter(_._1 != "0") //removing all events with zero PID (e.g. events from tubs)
  }

  def loadPackageReportFromCsvWithEmptyFailedDirection(spark: SparkSession): RDD[(String, UnifiedEvent)] = {

    val filenameList = getFilenamesArray(filenames.wcPackagereport)
    val (wcPackageReportHeader, wcPackageReportLines) = config.csvReader.read(filenameList, spark)
    config.csvReader.parse(
      wcPackageReportLines,
      trackingReportEntryFactoryWithEmptyFailedDirection(wcPackageReportHeader.indexOf("PID"),
        wcPackageReportHeader.indexOf("EVENTTS"),
        wcPackageReportHeader.indexOf("AREAID"), wcPackageReportHeader.indexOf("ZONEID"), wcPackageReportHeader.indexOf("EQUIPMENTID"))
    )
      .filter(_._1 != "0") //removing all events with zero PID (e.g. events from tubs)
  }

  def loadNodeSegments(spark: SparkSession): RDD[Segment] = {
    val filenameList = getFilenamesArray(filenames.wcTrackingreport)
    val (wcTrackingReportHeader, wcTrackingReportLines) = config.csvReader.read(filenameList, spark)
    config.csvReader.parse(
      wcTrackingReportLines,
      trackingReportEntryFactory(wcTrackingReportHeader.indexOf("PID"),
        wcTrackingReportHeader.indexOf("EVENTTS"),
        wcTrackingReportHeader.indexOf("AREAID"), wcTrackingReportHeader.indexOf("ZONEID"), wcTrackingReportHeader.indexOf("EQUIPMENTID"))
    )
      .filter(_._1 != "0") //removing all events with zero PID (e.g. events from tubs)
      .map { x => Segment(x._1, s"${x._2.activity}:", x._2.timestamp, 0) }
  }

  def loadPackageInfoClazz(spark: SparkSession, columnName: String, clazzifier: String => Int): RDD[EventWithClazz] = {
    val filenameList = getFilenamesArray(filenames.wcPackageinfo)
    val (header, lineRdd) = config.csvReader.read(filenameList, spark)
    config.csvReader.parse(
      lineRdd,
      PackageInfoEvent(config.bpiCsvHelper, header.indexOf("PID"), header.indexOf("EVENTTS"), header.indexOf(columnName))
    )
      .map { x => EventWithClazz(x.id, x.timestamp, clazzifier(x.activity))
      }
      .filter(_.clazz != 0)
      .distinct()
  }

  def packageInfoEntryFactory(idIndex: Int, eventTsIndex: Int, lpcIndex: Int, processIndex: Int)(a: Array[String]) =
    PackageInfoWithLpcWithProcessDefinitionName(a(idIndex),
      config.bpiCsvHelper.extractOracleTimestampMs(a(eventTsIndex)),
      if (a(lpcIndex).isEmpty) None else Some(a(lpcIndex)),
      if (a(processIndex).isEmpty) None else Some(a(processIndex)))

  def loadLateBagIdsFromCsv(spark: SparkSession): RDD[String] = {
    // loading WC_PACKAGEINFO table
    val filenameList = getFilenamesArray(filenames.wcPackageinfo)
    val (wcPackageInfoHeader, wcPackageInfoLines) = config.csvReader.read(filenameList, spark)

    config.csvReader.parse(
      wcPackageInfoLines,
      packageInfoEntryFactory(wcPackageInfoHeader.indexOf("PID"),
        wcPackageInfoHeader.indexOf("EVENTTS"),
        wcPackageInfoHeader.indexOf("LPC"),
        wcPackageInfoHeader.indexOf("PROCESSDEFINITIONNAME"))
    ).filter(x => x.id != "0" && !x.process.isEmpty && x.process.get == "Too Late")
      .map(x => x.id)
      .distinct()
  }

  def loadNotDistinctLateBagIdsFromCsv(spark: SparkSession): RDD[String] = {
    // loading WC_PACKAGEINFO table
    val filenameList = getFilenamesArray(filenames.wcPackageinfo)
    val (wcPackageInfoHeader, wcPackageInfoLines) = config.csvReader.read(filenameList, spark)

    config.csvReader.parse(
      wcPackageInfoLines,
      packageInfoEntryFactory(wcPackageInfoHeader.indexOf("PID"),
        wcPackageInfoHeader.indexOf("EVENTTS"),
        wcPackageInfoHeader.indexOf("LPC"),
        wcPackageInfoHeader.indexOf("PROCESSDEFINITIONNAME"))
    ).filter(x => x.id != "0" && !x.process.isEmpty && x.process.get == "Too Late")
      .map(x => x.id)

  }



  def loadLpcFromCsv(spark: SparkSession): RDD[(String, String)] = {
    // loading WC_PACKAGEINFO table
    val filenameList = getFilenamesArray(filenames.wcPackageinfo)
    val (wcPackageInfoHeader, wcPackageInfoLines) = config.csvReader.read(filenameList, spark)
    config.csvReader.parse(
      wcPackageInfoLines,
      packageInfoEntryFactory(wcPackageInfoHeader.indexOf("PID"),
        wcPackageInfoHeader.indexOf("EVENTTS"),
        wcPackageInfoHeader.indexOf("LPC"),
        wcPackageInfoHeader.indexOf("PROCESSDEFINITIONNAME"))
    ).filter(x => x.id.nonEmpty && x.id != "0" && x.lpc.isDefined)
      .map(x => (x.id, x.lpc.get))
      .distinct()
  }

  def fLoadTaskType(spark: SparkSession)(x: Unit): RDD[(String, UnifiedEvent)] = loadTaskType(spark)

  def loadTaskType(spark: SparkSession): RDD[(String, UnifiedEvent)] = {
    def factory(idIndex: Int, eventTsIndex: Int, tasktypeIndex: Int)(a: Array[String]) =
      (a(idIndex),
        UnifiedEvent(
          config.bpiCsvHelper.extractOracleTimestampMs(a(eventTsIndex)),
          a(tasktypeIndex),
          SortedMap(TaskReportSchema.AttrNameTaskClass -> TaskReportEvent.taskClassifier(a(tasktypeIndex))),
          None))

    val filenameList = getFilenamesArray(filenames.wcTaskreport)
    val (header, lines) = config.csvReader.read(filenameList, spark)
    config.csvReader.parse(
      lines,
      factory(header.indexOf("PID"), header.indexOf("EVENTTS"), header.indexOf("TASKTYPE"))
    )
      .filter(x => x._1 != "0" && !x._2.activity.isEmpty)
      .distinct()
  }

  private def availabilityFactory(idIndex: Int, eventTsIndex: Int, availabilityIndex: Int, operationalStateIndex: Int, areaidIndex: Int, zoneidIndex: Int, equipmentidIndex: Int)(a: Array[String]) =
    (a(idIndex), config.bpiCsvHelper.extractOracleTimestampMs(a(eventTsIndex)), a(availabilityIndex), a(operationalStateIndex), a(areaidIndex), a(zoneidIndex), a(equipmentidIndex))


  def getFilenamesArray(f: () => Array[String]): Array[String] =
    f().map(x => s"${config.inDir}/$x")


  def readAvailability(spark: SparkSession): RDD[(String, Long, String, String, String, String, String)] = {
    val filenameList = getFilenamesArray(filenames.wcAvailabilitytypereport)
    val (availabilityHeader, availabilityLines) = config.csvReader.read(filenameList, spark)
    config.csvReader.parse(
      availabilityLines,
      availabilityFactory(
        availabilityHeader.indexOf("DESTINATION"),
        availabilityHeader.indexOf("EVENTTS"),
        availabilityHeader.indexOf("AVAILABILITY"),
        availabilityHeader.indexOf("OPERATIONALSTATE"),
        availabilityHeader.indexOf("AREAID"),
        availabilityHeader.indexOf("ZONEID"),
        availabilityHeader.indexOf("EQUIPMENTID")
      )
    )

  }

  def loadAvailabilitySegments(mcAvailability: RDD[(String, Long, String, String)]): RDD[Segment] = {
    mcAvailability.map { x => Segment(x._1, s"${x._1}:", x._2, 0) }
  }


  def loadAvailability(mcAvailability: RDD[(String, Long, String, String)]): RDD[EventWithClazz] =
    mcAvailability.map { x => EventWithClazz(x._1, x._2, if (x._3 == "NOT-AVAILABLE") 1 else if (x._4 == "LOGGED_OFF") 2 else 3) }
}

object TrackingReportSchema {

  /** Byte */
  val AttrNameFailedDirectionClass: String = "failedDirectionClass"
  val Schema = Set(AttrNameFailedDirectionClass)
}

object TaskReportSchema {

  /** Byte */
  val AttrNameTaskClass: String = "taskClass"
  val Schema = Set(AttrNameTaskClass)
}


private[bhs] object BpiImporter {
  val AvailabilityClazzCount = 4
}