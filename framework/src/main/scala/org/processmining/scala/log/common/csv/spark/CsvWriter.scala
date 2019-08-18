package org.processmining.scala.log.common.csv.spark

import java.io.{File, FileOutputStream, PrintWriter}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.processmining.scala.log.common.unified.log.spark.UnifiedEventLog

// Writes CSV files
object CsvWriter {

  /**
    * Save RDD of traces into a CSV file
    * Works only for a local file system and small datasets fitting into the node memory!
    */
  def eventsToCsvLocalFilesystem[E](events: RDD[E],
                                    csvHeader: String,
                                    toCsv: E => String,
                                    filename: String): Unit = {
    val w = new PrintWriter(filename)
    try {
      if (!events.isEmpty) {
        w.println(csvHeader)
        events
          .collect()
          .foreach { e => w.println(toCsv(e)) }
      }
    } finally {
      w.close()
    }
  }

  def eventsToCsvWithHeader[E](spark: SparkSession,
                               events: RDD[E],
                               csvHeader: String,
                               toCsv: E => String,
                               filename: String
                              ): Unit = {

    val headerRdd = spark.sparkContext.parallelize(List(csvHeader)).map((_, -1L))
    val lines = events
      .map(toCsv)
      .zipWithIndex()
      .union(headerRdd)
      .sortBy(_._2)
      .map(_._1)
    lines.saveAsTextFile(filename)
  }

  def eventsToCsv[E <: AnyRef](spark: SparkSession,
                               events: RDD[E],
                               toCsv: E => String,
                               filename: String): Unit =
    events
      .map(toCsv).saveAsTextFile(filename)


  def dataframeToCsvWithHeader(spark: SparkSession, dataFrame: DataFrame, filename: String): Unit = {
    val header = spark.sparkContext.parallelize(List((("\"" + dataFrame.columns.mkString("""";"""") + "\""), -1L)))
    dataFrame
      .rdd
      .map(_.mkString("\"", "\";\"", "\""))
      .zipWithIndex()
      .union(header)
      .sortBy(_._2)
      .map(_._1)
      .saveAsTextFile(filename)
  }

  def dataframeToCsv(spark: SparkSession, dataFrame: DataFrame, filename: String): Unit =
    dataFrame
      .rdd
      .map(_.mkString("\"", "\";\"", "\""))
      .saveAsTextFile(filename)


  /**
    * Save a DF into a CSV file
    * Works only for a local file system and small datasets fitting into the node memory!
    */
  def dataframeToLocalCsv(dataFrame: DataFrame, filename: String, addHeader: Boolean = true, append: Boolean = false): Unit = {
    val w = new PrintWriter(new FileOutputStream(new File(filename), append))
    try {
      if (addHeader) w.println("\"" + dataFrame.columns.mkString("""";"""") + "\"")
      dataFrame
        .collect()
        .foreach(r => w.println(r.mkString("\"", "\";\"", "\"")))
    }
    finally {
      w.close()
    }
  }


  def logToCsvLocalFilesystem(unifiedEventLog: UnifiedEventLog,
                              filename: String,
                              timestampConverter: Long => String,
                              unsortedAttrs: String*
                             ): Unit = {
    val w = new PrintWriter(filename)
    val attrs = unsortedAttrs.sorted
    try {
      //val attrs = schema.fields.map(_.name)
      val attrsHeader = if (attrs.isEmpty) "" else attrs.mkString(""";"""", """";"""", """"""")
      w.println(""""id";"timestamp";"activity"""" + attrsHeader)
      unifiedEventLog
        //.filter(schema)
        .events
        .map(t => s""""${t._1.id}";${t._2.toCsv(timestampConverter, attrs: _*)}""")
        .collect()
        .foreach(w.println(_))
    } finally {
      w.close()
    }
  }


//  def logToCsvLocalFilesystem(unifiedEventLog: UnifiedEventLog,
//                              filename: String,
//                              timestampConverter: Long => String,
//                              unsortedAttrs: String*
//                             ): Unit = {
//    val w = new PrintWriter(filename)
//    try {
//
//      val attrs = unsortedAttrs.sorted
//      val attrsHeader = if (attrs.isEmpty) "" else attrs.mkString(""";"""", """";"""", """"""")
//      w.println(""""id";"timestamp";"activity"""" + attrsHeader)
//      unifiedEventLog.
//        events
//        //.filter(_._2.hasAttributes(attrs: _*))
//        .map(t => s""""${t._1.id}";${t._2.toCsv(timestampConverter, attrs: _*)}""")
//        .collect()
//        .foreach(w.println(_))
//    } finally {
//      w.close()
//    }
//  }


}
