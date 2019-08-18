package org.processmining.scala.log.common.enhancment.segments.spark

import java.io.{File, PrintWriter}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.processmining.scala.log.common.csv.spark.CsvWriter
import org.processmining.scala.log.common.types.{Segment, SegmentWithClazz}
import org.processmining.scala.log.common.unified.event.{CommonAttributeSchemas, UnifiedEvent}
import org.processmining.scala.log.common.unified.log.spark.UnifiedEventLog
import org.processmining.scala.log.common.enhancment.segments.common.{AbstractAggregationFunction, InventoryAggregation, PreprocessingSession}
import org.processmining.scala.log.common.enhancment.segments.parallel.SegmentVisualizationItem

import scala.collection.immutable.SortedMap


case class SegmentProcessorConfig(
                                   spark: SparkSession,
                                   segments: UnifiedEventLog,
                                   timestamp1Ms: Long,
                                   timestamp2Ms: Long,
                                   twSize: Long,
                                   aaf: AbstractAggregationFunction = InventoryAggregation
                                 )


abstract class SegmentProcessor(val config: SegmentProcessorConfig, clazzCount: Int) extends Serializable {


  protected def joinSegmentsWithTw(segTable: String): DataFrame = {

    import config.spark.implicits._
    SegmentProcessor.dfToRdd(config.spark.sql(s"""SELECT id, key, timestamp, ${CommonAttributeSchemas.AttrNameDuration}, ${CommonAttributeSchemas.AttrNameClazz} FROM $segTable"""))
      .flatMap { x =>
        config.aaf.getTws(x.timestamp, x.duration, config.timestamp1Ms, config.timestamp2Ms, config.twSize)
          .map(i => (i, x))

      }
      .map(x => (x._1, x._2.id, x._2.key, x._2.clazz))
      .toDF("index", "id", "key", CommonAttributeSchemas.AttrNameClazz)
  }

  protected def defaultPreProcessingForVisualization(selSeg: DataFrame): DataFrame = {
    selSeg.createOrReplaceTempView("selSeg")
    val segTw = joinSegmentsWithTw("selSeg")
    segTw.createOrReplaceTempView("segTw")
    config.spark.sql(
      s"""SELECT index, key, ${CommonAttributeSchemas.AttrNameClazz}, count(id) as count
         | FROM segTw
         | GROUP BY index, key, ${CommonAttributeSchemas.AttrNameClazz}""".stripMargin)
  }


  /**
    * Pre-process data for visualization
    *
    * @return "SELECT index , key, clazz, count(id) as count FROM segments GROUP BY index, key, clazz"
    *         index is a zero-based time window index
    *         key is a segment name (e.g. "A:B" for a segment of 2 events A and B
    *         clazz is a value returned by a classifier
    *         count is a number of such segments (aggregation)
    */
  def getVisualizationDatasetDf(): DataFrame


  def getVisualizationDataset(export: => RDD[(Long, SegmentWithClazz)] => Unit): (RDD[SegmentVisualizationItem], PreprocessingSession, DataFrame) = {
    val session = PreprocessingSession(config.timestamp1Ms, config.timestamp2Ms, config.twSize, clazzCount, config.aaf.getClass.getSimpleName, "")
    val df = getVisualizationDatasetDf()


    //TODO: use intermediate results
    export(getClassifiedSegments().map(x =>
      (config.aaf.getTws(x.timestamp, x.duration, config.timestamp1Ms, config.timestamp2Ms, config.twSize)
        , x))
      .filter(_._1.nonEmpty)
      .map(x => (x._1.head, x._2))
    )


    //    export(segments.map(x =>
    //      (SegmentProcessorUtils.getTws(x.timestamp, x.duration, config.timestamp1Ms, config.timestamp2Ms, config.twSize)
    //        , x))
    //      .filter(_._1.nonEmpty)
    //      .map(x => (x._1.head, x._2))
    //    )


    val maxSegs = SegmentProcessor.getMax(df, config)
    (df
      .rdd
      .map { x =>
        SegmentVisualizationItem(
          x.getAs[Long]("index"),
          x.getAs[String]("key"),
          x.getAs[Int](CommonAttributeSchemas.AttrNameClazz),
          x.getAs[Long]("count"))
      }, PreprocessingSession.commit(session), maxSegs)
  }

  def getClassifiedSegments(): RDD[SegmentWithClazz]

  def getClassifiedSegmentLog(): UnifiedEventLog =
    UnifiedEventLog.create(
      getClassifiedSegments()
        .map(x => (x.id,
          UnifiedEvent(
            x.timestamp,
            x.key,
            SortedMap(CommonAttributeSchemas.AttrNameDuration -> x.duration, CommonAttributeSchemas.AttrNameClass -> x.clazz.toByte),
            None
          )
        )))


}


object SegmentProcessor {

  val ProcessedDataFileName = "time_diff.csv"
  val StatDataFileName = "max.csv"
  val ConfigFileName = "config.ini"


  def getMax(groupedSegTw: DataFrame, config: SegmentProcessorConfig): DataFrame = {
    groupedSegTw.createOrReplaceTempView("groupedSegTw")
    val sumSegs = config.spark.sql(s"""SELECT index, key, sum(count) as sum FROM groupedSegTw GROUP BY index, key""")
    sumSegs.createOrReplaceTempView("sumSegs")
    config.spark.sql(s"""SELECT key, max(sum) as max FROM sumSegs GROUP BY key""")
  }


  def toCsv(proc: SegmentProcessor, outDir: String, legend: String): DataFrame = {
    new File(outDir).mkdirs()
    val groupedSegTw = proc.getVisualizationDatasetDf()
    CsvWriter.dataframeToLocalCsv(groupedSegTw.cache(), outDir + ProcessedDataFileName)
    val maxSegs = getMax(groupedSegTw, proc.config)
    CsvWriter.dataframeToLocalCsv(maxSegs, outDir + StatDataFileName)
    val w = new PrintWriter(outDir + ConfigFileName)
    try {

      w.println("[GENERAL]")
      w.println(s"""twSizeMs = ${proc.config.twSize}""")
      //w.println(s"""startTime = ${proc.config.csvExportHelper.timestamp2String(proc.config.timestamp1Ms)}""")
      //w.println(s"""endTime = ${proc.config.csvExportHelper.timestamp2String(proc.config.timestamp2Ms)}""")
      w.println(s"""startTimeMs = ${proc.config.timestamp1Ms}""")
      w.println(s"""endTimeMs = ${proc.config.timestamp2Ms}""")
      if (!legend.isEmpty) w.println(s"""legend = ${legend}""")
    } finally {
      w.close()
    }
    groupedSegTw
  }

  def dfToRdd(df: DataFrame): RDD[SegmentWithClazz] =
    df
      .rdd
      .map { x =>
        SegmentWithClazz(
          x.getAs[String]("id"),
          x.getAs[String]("key"),
          x.getAs[Long]("timestamp"),
          x.getAs[Long](CommonAttributeSchemas.AttrNameDuration),
          x.getAs[Int](CommonAttributeSchemas.AttrNameClazz))
      }

  def getTracesWithSegments(log: UnifiedEventLog): RDD[(String, List[Segment])] =
    log
      .traces()
      .mapValues(_.filter(_.hasAttribute(CommonAttributeSchemas.AttrNameDuration)))
      .filter(_._2.nonEmpty)
      .map(x => (x._1.id, x._2.map(e => Segment(x._1.id, e.activity, e.timestamp, e.getAs[Long](CommonAttributeSchemas.AttrNameDuration)))))


  def createSegmentRdd(log: UnifiedEventLog): RDD[Segment] =
    getTracesWithSegments(log)
      .values
      .flatMap(x => x)


  def createSegmentDataframe(log: UnifiedEventLog, spark: SparkSession): DataFrame =
    spark.createDataFrame(createSegmentRdd(log))

  def toCsvV2(proc: SegmentProcessor, outDir: String, legend: String) = {
    new File(outDir).mkdirs()
    val (dataset, session, maxSegs) = proc.getVisualizationDataset(DataExporter.segmentsToDirs(s"${outDir}/segments", _: RDD[(Long, SegmentWithClazz)]))
    DataExporter.toDirs(dataset, s"${outDir}/data")
    CsvWriter.dataframeToLocalCsv(maxSegs, outDir + StatDataFileName)
    PreprocessingSession.toDisk(session.copy(legend = legend), outDir + org.processmining.scala.log.common.enhancment.segments.parallel.SegmentProcessor.SessionFileName, false)
  }


}

