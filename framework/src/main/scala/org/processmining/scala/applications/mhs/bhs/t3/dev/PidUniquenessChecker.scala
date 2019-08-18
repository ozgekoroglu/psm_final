package org.processmining.scala.applications.mhs.bhs.t3.dev

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.processmining.scala.applications.mhs.bhs.bpi.{Lpc, PackageInfoLpcEntry}
import org.processmining.scala.applications.mhs.bhs.t3.eventsources.BpiCsvImportHelper
import org.processmining.scala.log.common.csv.spark.CsvReader
import org.processmining.scala.log.common.types.{Id, Timestamp}
import org.processmining.scala.log.utils.common.csv.common.CsvExportHelper


private object PidUniquenessChecker {
  private val spark: SparkSession =
    SparkSession
      .builder()
      .appName("Lpc Uniqueness Checker")
      .config("spark.master", "local[*]")
      .getOrCreate()


  /**
    * @return Pairs of (PID, list of LPCs)
    */
  def getPidToLpcMapping[T <: Id with Lpc](packageInfoLpcEntryRdd: RDD[T]): RDD[(String, List[String])] = {
    packageInfoLpcEntryRdd
      .filter { x => !x.id.isEmpty && !x.lpc.isEmpty }
      .map { x => (x.id, x.lpc) }
      .groupByKey()
      .mapValues {
        _.to[Set].to[List]
      }
  }

  /**
    * @return Pairs of (PID, LPC) where LPC is the last observed LPC for this PID
    */
  def getPidToLpcUniqueMapping[T <: Id with Lpc with Timestamp](packageInfoLpcEntryRdd: RDD[T]): RDD[(String, String)] = {
    packageInfoLpcEntryRdd
      .filter { x => !x.id.isEmpty && !x.lpc.isEmpty }
      .map { x => (x.id, (x.lpc, x.timestamp)) }
      .groupByKey()
      .mapValues {
        _
          .toList
          .sortBy(_._2)
          .last
          ._1
      }
  }


  def main(args: Array[String]): Unit = {
    val csvReader = new CsvReader()
    val bpiCsvHelper = new BpiCsvImportHelper("dd-MMM-yy HH.mm.ss.SSSSSS", CsvExportHelper.UtcTimeZone)

    val (header, lineRdd) = csvReader.read(s"src/main/resources/logs/WC_PACKAGEINFO.csv.processed", spark)
    val packageInfoLpcEntryRdd = csvReader.parse(
      lineRdd,
      PackageInfoLpcEntry(bpiCsvHelper, header.indexOf("PID"), header.indexOf("LPC"), header.indexOf("EVENTTS"))
    ).cache()

    getPidToLpcMapping(packageInfoLpcEntryRdd)
      .filter {
        _._2.size > 1
      }
      .collect()
      .foreach { x => println(s"""PID="${x._1}" N=${x._2.size} LPCs: ${x._2.toString()}""") }

    val pair = getPidToLpcUniqueMapping(packageInfoLpcEntryRdd)
      .filter(x => x._1 == "17810834")
      .collect()
        .head

      println(s"""PID=${pair._1} LPC=${pair._2}""")
  }


}
