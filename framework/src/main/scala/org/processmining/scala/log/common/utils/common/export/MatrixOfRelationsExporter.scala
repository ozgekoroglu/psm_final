package org.processmining.scala.log.common.utils.common.export

import java.io._
import java.text.DecimalFormat
import java.util.concurrent.TimeUnit
import org.processmining.scala.log.common.utils.common.export._
import scala.io.Source
import scala.reflect.ClassTag

object MatrixOfRelationsExporter {

  private def mkCsvString(s: Seq[Any]): String = s.mkString("\"", "\",\"", "\"")


  val Zero = "#"
  val AB = ">"
  val BA = "<"
  val Diagonal = "@"
  val Parallel = "|"
  val ND = Zero
  val Undefined = 0


  def format(thresholdPercent: Double)(x1: Int, x2: Int, s: Boolean): (Int, String) = {
    val cell = (x1, x2, s)
    cell match {
      case (0, 0, _) => (0, Zero)
      case (x1, 0, _) => (x1, AB)
      case (0, x2, _) => (x2, BA)
      case (x1, _, true) => (x1, Diagonal)
      case (x1, x2, _) =>
        if (x1 >= x2) {
          val diffPercent = 100.0 - (x1 - x2) / x1.asInstanceOf[Double] * 100.0
          if (diffPercent > thresholdPercent) (x1, Parallel)
          else (x1, AB)
        } else {
          val diffPercent = 100.0 - (x2 - x1) / x2.asInstanceOf[Double] * 100.0
          if (diffPercent > thresholdPercent) (x1, Parallel)
          else (x2, BA)
        }
    }
  }

  def formatDouble(x1: Double, x2: Double, s: Boolean): (Double, String) =
    (x1, (x1).asInstanceOf[Long].toString)


  def formatMatrix[T: ClassTag](matrixOfRelations: (Array[Array[(T, T, Boolean)]], List[String]),
                                formatFunc: (T, T, Boolean) => (T, String)): (Array[Array[(T, String)]], List[String]) = {
    val (matrix, activities) = matrixOfRelations
    (activities.zipWithIndex.toArray.map(x => matrix(x._2).map(x => formatFunc(x._1, x._2, x._3))), activities)
  }

  //  @deprecated
  //  def csvExport[T: ClassTag](matrixOfRelations: (Array[Array[(T, T, Boolean)]], List[String]),
  //                             csvFilename: String, formatFunc: (T, T, Boolean) => (T, String),
  //                             efrDfrPrefix: String): Unit = {
  //
  //    val (formattedMatrix, activities) = formatMatrix(matrixOfRelations, formatFunc)
  //    val pw = new PrintWriter(new File(csvFilename))
  //    pw.println(""""activity","type",""" + mkCsvString(activities))
  //    activities.zipWithIndex.foreach(x => {
  //      val row = formattedMatrix(x._2)
  //      pw.println(s""""${x._1}","${efrDfrPrefix}_N",${mkCsvString(row.map(_._1))}""")
  //      pw.println(s""""${x._1}","${efrDfrPrefix}_S",${mkCsvString(row.map(_._2))}""")
  //    })
  //    pw.close()
  //  }

  def csvExport2[T: ClassTag](matrices: Seq[(Array[Array[(T, T, Boolean)]], List[String])],
                              csvFilename: String,
                              formatFunc: (T, T, Boolean) => (T, String),
                              sortFunc: (Set[String]) => List[String],
                              efrDfrPrefix: String): Unit = {

    val formattedMatrices = matrices
      .map(x => formatMatrix(x, formatFunc))
      .map(x => (x._1, x._2.zipWithIndex.map(y => (y._1 -> y._2)).toMap))
    val sortedActivities = sortFunc(matrices.flatMap(_._2).distinct.toSet)
    val pw = new PrintWriter(new File(csvFilename))
    pw.println(""""activity","type",""" + mkCsvString(sortedActivities))
    sortedActivities.foreach { y =>
      val row = sortedActivities.map { x =>
        formattedMatrices.map { m => {
          val mapper = m._2
          val i = mapper.get(y)
          val j = mapper.get(x)
          if (i.isDefined && j.isDefined) m._1(i.get)(j.get) else (Undefined, ND)
        }
        }
      }.map(cell => (cell.map(_._2).mkString(""), cell.map(_._1).toArray))
      (0 until matrices.size).foreach { i =>
        if (i == 0) pw.println(s""""$y","${efrDfrPrefix}",${mkCsvString(row.map(_._1))}""")
        pw.println(s""""$y","${efrDfrPrefix}_${i + 1}",${mkCsvString(row.map(_._2(i)))}""")

      }
    }
    pw.close()
  }

  private def decimalFormatHelper(msD: Double): String = {
    if (msD.isNaN) "NaN" else {
      val ms = msD.asInstanceOf[Long]
      val s = "%03d-%02d:%02d:%02d".format(
        TimeUnit.MILLISECONDS.toDays(ms),
        TimeUnit.MILLISECONDS.toHours(ms) - TimeUnit.DAYS.toHours(TimeUnit.MILLISECONDS.toDays(ms)),
        TimeUnit.MILLISECONDS.toMinutes(ms) - TimeUnit.HOURS.toMinutes(TimeUnit.MILLISECONDS.toHours(ms)),
        TimeUnit.MILLISECONDS.toSeconds(ms) - TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(ms))
      )
      if (ms == -1000) "" else if (ms == 0) "0" else s
    }
  }


  //  def e(millis: Long): String = {
  //    val s = "%03d-%02d:%02d:%02d.%03d".format(
  //      TimeUnit.MILLISECONDS.toDays(millis),
  //      TimeUnit.MILLISECONDS.toHours(millis) - TimeUnit.DAYS.toHours(TimeUnit.MILLISECONDS.toDays(millis)),
  //      TimeUnit.MILLISECONDS.toMinutes(millis) - TimeUnit.HOURS.toMinutes(TimeUnit.MILLISECONDS.toHours(millis)),
  //      TimeUnit.MILLISECONDS.toSeconds(millis) - TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(millis)),
  //      millis % 1000
  //    )
  //    if(millis == -1000) "" else
  //    if (s.startsWith("000-00:00:00.000")) "0" else if (s.startsWith("000-00:00")) s.substring(10) else if (s.startsWith("000-00")) s.substring(7)
  //    else if (s.startsWith("000-")) s.substring(4) else s
  //  }


  def csvExportDescriptiveStatistics(matrices: Seq[(Array[Array[(DescriptiveStatisticsEntry, DescriptiveStatisticsEntry, Boolean)]], List[String])],
                                     csvFilename: String,
                                     sortFunc: (Set[String]) => List[String]): Unit = {
    val formattedMatrices = matrices
      .map(x => (x._1, x._2.zipWithIndex.map(y => (y._1 -> y._2)).toMap))
    val sortedActivities = sortFunc(matrices.flatMap(_._2).distinct.toSet)
    val pw = new PrintWriter(new File(csvFilename))
    pw.println(""""activity","parameter","year",""" + mkCsvString(sortedActivities))
    sortedActivities.foreach { y =>
      val row = sortedActivities.map { x =>
        formattedMatrices.map { m => {
          val mapper = m._2
          val i = mapper.get(y)
          val j = mapper.get(x)
          if (i.isDefined && j.isDefined) m._1(i.get)(j.get)._1 else EmptyDescriptiveStatisticsEntry
        }
        }
      }
        .map(r => r.toArray)
      (0 until matrices.size).foreach { i =>
        pw.println(s""""$y","N","${i + 1}",${mkCsvString(row.map(_ (i).n))}""")
        pw.println(s""""$y","MEDIAN","${i + 1}",${mkCsvString(row.map(_ (i).percentile))}""")
        pw.println(s""""$y","MEDIAN_F","${i + 1}",${mkCsvString(row.map((_ (i).percentile)).map(decimalFormatHelper))}""")
        pw.println(s""""$y","MEAN","${i + 1}",${mkCsvString(row.map(_ (i).mean))}""")
        pw.println(s""""$y","MEAN_F","${i + 1}",${mkCsvString(row.map(_ (i).mean).map(decimalFormatHelper))}""")
        pw.println(s""""$y","STD","${i + 1}",${mkCsvString(row.map(_ (i).stdev))}""")
        pw.println(s""""$y","STD_F","${i + 1}",${mkCsvString(row.map(_ (i).stdev).map(decimalFormatHelper))}""")
        pw.println(s""""$y","KURTOSIS","${i + 1}",${mkCsvString(row.map(_ (i).kurtosis))}""")
        pw.println(s""""$y","SKEWNESS","${i + 1}",${mkCsvString(row.map(_ (i).skewness))}""")
        pw.println(s""""$y","MIN","${i + 1}",${mkCsvString(row.map(_ (i).min))}""")
        pw.println(s""""$y","MIN_F","${i + 1}",${mkCsvString(row.map(_ (i).min).map(decimalFormatHelper))}""")
        pw.println(s""""$y","MAX","${i + 1}",${mkCsvString(row.map(_ (i).max))}""")
        pw.println(s""""$y","MAX_F","${i + 1}",${mkCsvString(row.map(_ (i).max).map(decimalFormatHelper))}""")
      }
    }
    pw.close()
  }


  private def nClassifier(max: Long)(x: Long): Int =
    if (x <= 0) 0 else if (x <= max / 4) 1 else if (x <= max / 2) 2 else if (x <= max / 4 * 3) 3 else 4


  def htmlExportDescriptiveStatistics[T: ClassTag](matrices: Seq[(Array[Array[(T, T, Boolean)]], List[String])],
                                                   csvFilename: String,
                                                   title: String,
                                                   sortFunc: (Set[String]) => List[String],
                                                   formatT: T => (String, Long),
                                                   nfunc: T => Long,
                                                   empty: T
                                                  ): Unit = {
    val lines = Source.fromFile("..\\templates\\template1.html")
    val code = lines.getLines().mkString("\n")
    val header = code.substring(0, code.indexOf("<!--BEGIN-->")).replace("<title>Event Log Footprint</title>", s"<title>$title</title>")
    val footer = code.substring(code.indexOf("<!--END-->"), code.length)
    val formattedMatrices = matrices
      .map(x => (x._1, x._2.zipWithIndex.map(y => (y._1 -> y._2)).toMap))
    val max = matrices.flatMap(_._1).flatten.map(x => nfunc(x._1)).max
    val classifier: (Long => Int) = nClassifier(max)
    val sortedActivities = sortFunc(matrices.flatMap(_._2).distinct.toSet)
    val pw = new PrintWriter(new File(csvFilename))
    pw.write(header)
    pw.println("<table id=\"t1\">")
    pw.println(s"<tr><th data-rotate>activity<br>$title</th><th data-rotate>log</th>" +
      sortedActivities.mkString("<th data-rotate>", "</th><th data-rotate>", "</th>") + "</tr>")
    sortedActivities.foreach { y =>
      val row = sortedActivities.map { x =>
        formattedMatrices.map { m => {
          val mapper = m._2
          val i = mapper.get(y)
          val j = mapper.get(x)
          if (i.isDefined && j.isDefined) m._1(i.get)(j.get)._1 else empty
        }
        }
      }
        .map(r => r.toArray)
      (0 until matrices.size).foreach { i =>
        val s = row.map(x => formatT(x(i)))
          .map(x => (x._1, classifier(x._2)))
          .map(x => s"""<td class="Q${x._2}">${x._1}</td>""").mkString("")
        pw.println(s"""<tr class="T$i"><td class="T$i">$y</td><td class="N">${i + 1}</td>$s</tr>""")
      }
    }
    pw.println("</table>")
    pw.write(footer)
    pw.close()
  }

  private val decimalFormat = new DecimalFormat("#.##")

  private def skewnessKurtosisFormatHelper(n: Double) = decimalFormat.format(n)

  def exportDs(dsMatrices: Seq[(Array[Array[((DescriptiveStatisticsEntry, String), (DescriptiveStatisticsEntry, String), Boolean)]], scala.List[String])],
               filename: String,
               sortedActivities: List[String]) =
    MatrixOfRelationsExporter.htmlExportDescriptiveStatistics[(DescriptiveStatisticsEntry, String)](
      dsMatrices,
      filename,
      "distribution",
      MatrixOfRelations.sortByOrderedList(sortedActivities, _: Set[String]),
      x => (s"""<img src="${x._2}">""", x._1.n),
      _._1.n,
      (EmptyDescriptiveStatisticsEntry, "")
    )



  def exportDse(dsMatrices: Seq[(Array[Array[(DescriptiveStatisticsEntry, DescriptiveStatisticsEntry, Boolean)]], scala.List[String])],
                filename: String => String,
                sortedActivities: List[String],
                decimalFormat: Double => String = decimalFormatHelper,
                skewnessKurtosisFormat: Double => String = skewnessKurtosisFormatHelper) = {

    MatrixOfRelationsExporter.htmlExportDescriptiveStatistics[DescriptiveStatisticsEntry](
      dsMatrices,
      filename("median"),
      "median",
      MatrixOfRelations.sortByOrderedList(sortedActivities, _: Set[String]),
      x => (decimalFormat(x.percentile), x.n),
      _.n,
      EmptyDescriptiveStatisticsEntry
    )
    MatrixOfRelationsExporter.htmlExportDescriptiveStatistics[DescriptiveStatisticsEntry](
      dsMatrices,
      filename("mean"),
      "mean",
      MatrixOfRelations.sortByOrderedList(sortedActivities, _: Set[String]),
      x => if(decimalFormat(x.mean).nonEmpty && decimalFormat(x.stdev).nonEmpty) (decimalFormat(x.mean) + "<br>&plusmn" + decimalFormat(x.stdev), x.n) else ("", x.n),
      _.n,
      EmptyDescriptiveStatisticsEntry
    )

    MatrixOfRelationsExporter.htmlExportDescriptiveStatistics[DescriptiveStatisticsEntry](
      dsMatrices,
      filename("min"),
      "min",
      MatrixOfRelations.sortByOrderedList(sortedActivities, _: Set[String]),
      x => (decimalFormat(x.min), x.n),
      _.n,
      EmptyDescriptiveStatisticsEntry
    )

    MatrixOfRelationsExporter.htmlExportDescriptiveStatistics[DescriptiveStatisticsEntry](
      dsMatrices,
      filename("max"),
      "max",
      MatrixOfRelations.sortByOrderedList(sortedActivities, _: Set[String]),
      x => (decimalFormat(x.max), x.n),
      _.n,
      EmptyDescriptiveStatisticsEntry
    )

    MatrixOfRelationsExporter.htmlExportDescriptiveStatistics[DescriptiveStatisticsEntry](
      dsMatrices,
      filename("skewness"),
      "skewness",
      MatrixOfRelations.sortByOrderedList(sortedActivities, _: Set[String]),
      x => (skewnessKurtosisFormat(x.skewness), x.n),
      _.n,
      EmptyDescriptiveStatisticsEntry
    )

    MatrixOfRelationsExporter.htmlExportDescriptiveStatistics[DescriptiveStatisticsEntry](
      dsMatrices,
      filename("kurtosis"),
      "kurtosis",
      MatrixOfRelations.sortByOrderedList(sortedActivities, _: Set[String]),
      x => (skewnessKurtosisFormat(x.kurtosis), x.n),
      _.n,
      EmptyDescriptiveStatisticsEntry
    )

    MatrixOfRelationsExporter.htmlExportDescriptiveStatistics[DescriptiveStatisticsEntry](
      dsMatrices,
      filename("N"),
      "N",
      MatrixOfRelations.sortByOrderedList(sortedActivities, _: Set[String]),
      x => (x.n.toString, x.n),
      _.n,
      EmptyDescriptiveStatisticsEntry
    )


  }


}

