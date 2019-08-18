package org.processmining.scala.applications.mhs.acp.example

import org.processmining.scala.log.common.csv.spark.CsvReader
import org.processmining.scala.log.common.utils.spark.AbstractSession
import org.processmining.scala.log.utils.common.csv.common.CsvExportHelper

object Example extends AbstractSession("Example") {
  val helper =
    new SplunkCsvImportHelper("yyyy-MM-dd HH:mm:ss.nnnnnnnnn", CsvExportHelper.AmsterdamTimeZone)

  val reader = new CsvReader()
  val BaseDir = "D:\\logs\\acp\\example\\"

  val rdd1 = {
    val (header, lines) = reader.read(s"$BaseDir/ExampleSet1.csv", spark)
    reader.parse(lines, Entry1.apply(helper.extractTimestamp, _: Array[String]))
  }

  val rdd2 = {
    val (header, lines) = reader.read(s"$BaseDir/ExampleSet2.csv", spark)
    reader.parse(lines, Entry2.apply(helper.extractTimestamp(_: String), _: Array[String]))
  }

  val rdd3 = {
    val (header, lines) = reader.read(s"$BaseDir/ExampleSet3.csv", spark)
    reader.parse(lines, Entry3.apply(helper.extractTimestamp, _: Array[String]))
  }


  val rdd4 = {
    val (header, lines) = reader.read(s"$BaseDir/ExampleSet4.csv", spark)
    reader.parse(lines, Entry4.apply(helper.extractTimestamp, _: Array[String]))
  }


  def main(args: Array[String]): Unit = {

    //converting initial tables into paired RDDs (for join)
    val paired1 = rdd1.map(x => (x.TrayTsuId, x)) //ExampleSet1
    val paired2 = rdd2.map(x => (x.TrayTsuId, x)) //ExampleSet2
    val paired3 = rdd3.map(x => (x.TrayTsuId, x)) //ExampleSet3
    val paired4 = rdd4.map(x => (x.TrayTsuId, x)) //ExampleSet4

    val join1 = paired4
      .join(paired1) // i.	join by TrayTsuId (not left outer join)
      .filter { x => x._2._2.PalTimeStamp > x._2._1.LastAsrExitTime } //ii.	‘where’ PalTimeStamp > LastAsrExitTIme
      .groupByKey // grouping pairs with identical TrayTsuId into collections
      .values // getting rid of keys (TrayTsuId)
      .map { x => (x.head._1, x.map(_._2).toList.sortBy(_.PalTimeStamp).last) } //iii.	Sort descending on LastAsrExitTime
      .map(x => ((x._1.TrayTsuId, x._2.PalOrderId), x)) // creating keys (TrayTsuId, PalOrderId) for removing duplicates
      .reduceByKey((x, _) => x) //iv.	Remove duplicates on (TrayTsuId, PalOrderId)
      .map(x => (x._1._1, x._2)) // getting rid of the keys

    val join2 = join1 // very similar to the previous expression
      .join(paired2)
      .filter((x => x._2._2.EtrTime < x._2._1._2.PalTimeStamp))
      .groupByKey
      .values
      .map { x => (x.head._2.TrayTsuId, (x.head._1, x.map(_._2).toList.sortBy(_.EtrTime).last)) }

    val join3 = join2 // basically 'copy-paste' of the previous expression
      .join(paired3)
      .filter((x => x._2._2.FirstEntryAfterPal < x._2._1._1._2.PalTimeStamp))
      .groupByKey
      .values
      .map { x => (x.head._1, x.map(_._2).toList.sortBy(_.FirstEntryAfterPal).last) }

    //val res = join3.map(x => (x, createEtrOrStr(x))) // a stub: the condition syntax in the email is not clear


    println(join1.count())
    println(join2.count())
    println(join3.count())


  }


}
