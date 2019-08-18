package org.processmining.scala.log.common.utils.common.export

import java.awt.Color

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import org.processmining.scala.log.common.utils.common.export._

case class DescriptiveStatisticsEntry(n: Long, percentile: Double, mean: Double, stdev: Double, kurtosis: Double, skewness: Double, min: Double, max: Double)

object EmptyDescriptiveStatisticsEntry extends DescriptiveStatisticsEntry(0, -1000, -1000, -1000, -1, -1,-1000,-1000)

object EventRelationsStatistics {

  def createCombiner(x: Long): DescriptiveStatistics = {
    val ds = new DescriptiveStatistics();
    ds.addValue(x);
    ds
  }

  def scoreCombiner(ds: DescriptiveStatistics, x: Long): DescriptiveStatistics = {
    ds.addValue(x);
    ds
  }

  def scoreMerger(ds1: DescriptiveStatistics, ds2: DescriptiveStatistics): DescriptiveStatistics = {
    ds2.getValues.foreach(ds1.addValue(_))
    ds1
  }

  def reducer(percentile: Double)(key: (String, String), ds: DescriptiveStatistics): DescriptiveStatisticsEntry =
    DescriptiveStatisticsEntry(ds.getN, ds.getPercentile(percentile), ds.getMean(), ds.getStandardDeviation, ds.getKurtosis, ds.getSkewness, ds.getMin, ds.getMax)

  def reducerWithDistr(percentile: Double,
                       filenameFunc: (String, String) => (String, String),
                       k: Double,
                       color: Color,
                       bins: Int,
                       height: Int)(key: (String, String),ds: DescriptiveStatistics): (DescriptiveStatisticsEntry, String) = {
    val (filename, relFilename) = filenameFunc(key._1, key._2)
    val ds2 = new DescriptiveStatistics()
    ds.getValues.map(_ * k).foreach(ds2.addValue)
    LogSimDiffRenderer.writeHistogramm(ds2, filename, color, bins, height)
    (reducer(percentile)(key, ds), relFilename)
  }
}
