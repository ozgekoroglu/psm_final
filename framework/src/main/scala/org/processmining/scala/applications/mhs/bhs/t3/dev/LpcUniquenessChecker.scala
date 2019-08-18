package org.processmining.scala.applications.mhs.bhs.t3.dev

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.processmining.scala.applications.mhs.bhs.bpi.{Lpc, Surname}
import org.processmining.scala.applications.mhs.bhs.t3.eventsources.BpiCsvImportHelper
import org.processmining.scala.log.common.csv.spark.CsvReader
import org.processmining.scala.log.common.types.{Id, Timestamp}
import org.processmining.scala.log.utils.common.csv.common.CsvExportHelper

import scala.collection.immutable.HashSet

private object LpcUniquenessChecker {
  private val spark: SparkSession =
    SparkSession
      .builder()
      .appName("Lpc Uniqueness Checker")
      .config("spark.master", "local[*]")
      .getOrCreate()

  /**
    * @return Pairs of (LPC, list of surnames)
    */
  def getNonUniqueLpc[T <: Lpc with Surname](packageInfoLpcEntryRdd: RDD[T]): RDD[(String, List[String])] = {
    val nonUniqueLpc = packageInfoLpcEntryRdd
      .map { x => (x.lpc, x.surname) } // to PairRDD and also removing all possible extra fields of T
      .groupByKey // group by LPC
      .mapValues {
      _./:(new HashSet[String])((set, surname) => // fold each collection into Set of surnames and  skip empty surnames
        if (!surname.isEmpty()) set + surname else set)
    }
      .filter { x => !x._1.isEmpty() && x._2.size > 1 } // filter out empty keys (i.e. LPC) and unique LPC (i.e. Sets with 1 item)
      .map { x => (x._1, x._2.to[List]) } // Sets to Lists

    nonUniqueLpc // further detection of identical surnames
      .map { x =>
      (x._1, x._2.filter {
        // Filter out surnames that contain in other surnames.
        // More sophisticated logic may be required, e.g. to remove MS/MR prefixes and suffixes
        // Could be difficult to aggregate using, for example, Oracle SQL
        surname => x._2.forall { it => it == surname || !it.contains(surname) }
      })
    }.filter(_._2.size > 1) // again filter out unique LPC (i.e. Sets with less than 2 items)

  }

  def main(args: Array[String]): Unit = {
    val bpiHelper = new BpiCsvImportHelper("dd-MMM-yy HH.mm.ss.SSSSSS", CsvExportHelper.UtcTimeZone)
    val csvReader = new CsvReader()

    // Id and Timestamp are actually not required, they are added for experiments
    case class PackageInfoLpcEntry(id: String, timestamp: Long, lpc: String, surname: String)
      extends Serializable with Id with Timestamp with Lpc with Surname

    def packageInfoLpcEntryFactory(idIndex: Int, eventTsIndex: Int, lpcIndex: Int, surnameIndex: Int)(a: Array[String]) =
      PackageInfoLpcEntry(a(idIndex), bpiHelper.extractOracleTimestampMs(a(eventTsIndex)), a(lpcIndex), a(surnameIndex))

    val (header, lineRdd) = csvReader.read("D:\\instantclient_12_1\\july\\WC_PACKAGEINFO.csv.processed", spark)


    val packageInfoLpcEntryRdd = csvReader.parse(
      lineRdd,
      packageInfoLpcEntryFactory(header.indexOf("PID"), header.indexOf("EVENTTS"), header.indexOf("LPC"), header.indexOf("SURNAME"))
    )

    getNonUniqueLpc(packageInfoLpcEntryRdd)
      .collect()
      .foreach { x => println(s"""LPC="${x._1}" N=${x._2.size} Surnames: ${x._2.toString()}""") }
  }

}
