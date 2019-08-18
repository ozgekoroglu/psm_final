package org.processmining.scala.log.common.utils.spark

import org.apache.spark.sql.SparkSession

/** Helper for Spark-based scripts */
class AbstractSession(name: String) extends Serializable{

  // Spark initialization
  @transient
  protected val spark: SparkSession =
    SparkSession
      .builder()
      .appName(name)
      .config("spark.master", "local[*]")
      .config("spark.driver.maxResultSize", "0")
//      .config("mapred.min.split.size", "10485760")
//      .config("mapred.max.split.size", "10485760")
      //.config("spark.master", "spark://131.155.68.43:7077")
      .getOrCreate()

  @transient
  protected val sc = spark.sparkContext
}
