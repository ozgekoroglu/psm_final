package org.processmining.scala.log.common.csv.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.processmining.scala.log.utils.common.csv.common.CsvReaderHelper

import scala.reflect.ClassTag

class CsvReader(val defaultSep: (String, String) = CsvReaderHelper.EmptySep) extends Serializable {

  def parse[T: ClassTag](lines: RDD[Array[String]], create: (Array[String]) => T): RDD[T] =
    lines.map {
      create(_)
    }

  def read(filenames: Array[String], spark: SparkSession): (Array[String], RDD[Array[String]]) = {
    val datasets = filenames.map(read(_, spark))
    val identicalCsvHeadersCount = datasets.map(_._1.mkString("")).distinct.size
    if (identicalCsvHeadersCount != 1)
      throw new IllegalArgumentException(s"Number of identical CSV headers must be 1, found $identicalCsvHeadersCount")
    (datasets.head._1, datasets.map(_._2)
      .reduce(spark.sparkContext.union(_, _)))
  }

  def read(filename: String, spark: SparkSession): (Array[String], RDD[Array[String]]) = {
    val rdd = spark.sparkContext.textFile(filename)
    val header = rdd.first()
    val (splitterRegEx, wrap) = if (defaultSep == CsvReaderHelper.EmptySep) CsvReaderHelper.detectSep(header) else defaultSep
    //println(s"Separator for '$filename' is '$splitterRegEx', wrapping charachter is '$wrap'")
    val splitter = (line: String) => line.substring(wrap.length, line.length - wrap.length).split(splitterRegEx)
    val headerColumns = splitter(header)
    val data =
      rdd //TODO: revert
        .mapPartitionsWithIndex { (i, it) =>
          if (i == 0) it.drop(1) // skip the header line
          else it // if (i == rdd.getNumPartitions - 1) it.take(it.size - 2) else it // skip the last 2 lines
        }
        .map {
          splitter(_)
        }
    (headerColumns, data)
  }
}

