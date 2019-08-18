package org.processmining.scala.applications.dev

import java.io.File
import java.util.prefs.Preferences

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.ini4j.{Ini, IniPreferences}

private object HelloWorldCluster {
  // Spark initialization
  @transient
  protected val spark: SparkSession =
  SparkSession
    .builder()
    .appName("HelloWorldCluster")
    //.config("spark.master", "local[*]")
    .config("spark.master", "spark://131.155.70.35:7077")
    .getOrCreate()

  @transient
  protected val sc = spark.sparkContext

  def main(args: Array[String]): Unit = {
    println("Main started")
    println(classOf[SparkContext].getPackage.getImplementationVersion)
    val iniPrefs = new IniPreferences(new Ini(new File("xxx.ini")))
    val rdd = sc.parallelize((0 until 100000))
    val sum = rdd
      .map(_ + 1)
      .reduce(_ + _)


    println(classOf[SparkContext].getPackage.getImplementationVersion)
    println(sum)

  }
}
