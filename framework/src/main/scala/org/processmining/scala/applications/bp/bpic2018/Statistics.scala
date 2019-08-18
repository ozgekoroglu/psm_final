package org.processmining.scala.applications.bp.bpic2018

import java.io.{File, PrintWriter}

import org.processmining.scala.log.common.unified.event.UnifiedEvent
import org.processmining.scala.log.common.unified.log.spark.UnifiedEventLog
import org.processmining.scala.log.common.xes.parallel.XesReader


object Statistics extends Bpic2018Template {

  def writeResultToTxt(resultPath: String, result: List[(String, Int, Int, Int, Int)]): Unit = {
    val pw = new PrintWriter(new File(resultPath))
    result.foreach(x => pw.println(x.toString()))
    pw.close
  }

  // DELETE
  def resourceDepartmentTable(log: UnifiedEventLog): List[(String, Int, Int, Int, Int)] = {

    val resourceDepartmentList: List[(String, String)] = log
      .traces()
      .map(x =>
        x._2.map(y =>
          (y.attributes(EventAttributes.Resource).toString,
            //          y.attributes(s"(case)_${TraceAttributes.Department}").toString) // for log from Disco
            y.attributes(s"${XesReader.DefaultTraceAttrNamePrefix}${TraceAttributes.Department}").toString)
        ))
      .collect().flatten.toList

    val resourceDepartmentOccurrenceList: Map[(String, String), Int] = resourceDepartmentList
      .groupBy(identity)
      .mapValues(_.size)

    resourceDepartmentList.map(_._1).distinct // list of unique resources
      .map(resource => (resource,
      if (resourceDepartmentOccurrenceList.get((resource, "4e")).isDefined) resourceDepartmentOccurrenceList(resource, "4e") else 0
      ,
      if (resourceDepartmentOccurrenceList.get((resource, "6b")).isDefined) resourceDepartmentOccurrenceList(resource, "6b") else 0
      ,
      if (resourceDepartmentOccurrenceList.get((resource, "d4")).isDefined) resourceDepartmentOccurrenceList(resource, "d4") else 0
      ,
      if (resourceDepartmentOccurrenceList.get((resource, "e7")).isDefined) resourceDepartmentOccurrenceList(resource, "e7") else 0
    ))

  }

  def attr1attr2table1(log: UnifiedEventLog, eventAttr1: String, eventAttr2: String, resultPath: String): Unit = {

    val pairsList: List[(String, String)] = log
      .traces()
      .map(x =>
        x._2.map(y =>
          (y.attributes(eventAttr1).toString,
            y.attributes(eventAttr2).toString)
        ))
      .collect().flatten.toList

    val occurrences: Map[(String, String), Int] = pairsList
      .groupBy(identity)
      .mapValues(_.size)

    val attr1uniqueList = pairsList.map(_._1).distinct // list of unique eventAttr1
    val attr2uniqueList = pairsList.map(_._2).distinct // list of unique eventAttr2

    val result = attr1uniqueList.map(attr1 =>
      (attr1,
        attr2uniqueList.map(attr2 =>
          if (occurrences.get((attr1, attr2)).isDefined) occurrences(attr1, attr2) else 0)
      ))

    val pw = new PrintWriter(new File(resultPath))
    pw.println(s"( , $attr2uniqueList")
    result.foreach(x => pw.println(x.toString()))
    pw.close
  }

  def attr1attr2table2(log: UnifiedEventLog, eventAttr1: String, traceAttr2: String, resultPath: String): Unit = {

    val pairsList: List[(String, String)] = log
      .traces()
      .map(x =>
        x._2.map(y =>
          (y.attributes(eventAttr1).toString,
            y.attributes(s"${XesReader.DefaultTraceAttrNamePrefix}$traceAttr2").toString)
          //            y.attributes(s"(case)_$traceAttr2").toString)// for log from Disco
        ))
      .collect().flatten.toList

    val occurrences: Map[(String, String), Int] = pairsList
      .groupBy(identity)
      .mapValues(_.size)

    val attr1uniqueList = pairsList.map(_._1).distinct // list of unique eventAttr1
    val attr2uniqueList = pairsList.map(_._2).distinct // list of unique traceAttr2

    val result = attr1uniqueList.map(attr1 =>
      (attr1,
        attr2uniqueList.map(attr2 =>
          if (occurrences.get((attr1, attr2)).isDefined) occurrences(attr1, attr2) else 0)
      ))

    val pw = new PrintWriter(new File(resultPath))
    pw.println(s"( , $attr2uniqueList")
    result.foreach(x => pw.println(x.toString()))
    pw.close
  }

  def allTracesAttrs(log: UnifiedEventLog, resultPath: String): Unit = {

    val traceAttrs = TraceAttributes.SetOfMainAttributes.toSeq.map(x => s"${XesReader.DefaultTraceAttrNamePrefix}$x")
    val traceAttrsList = log
      .traces()
      .map { x =>
        traceAttrs.map { attrNameWithPrefix =>
          val optionalValue = x._2.head.attributes.get(attrNameWithPrefix)
          if (optionalValue.isDefined) optionalValue.get.toString else ""
        }
      }
      //    x._2.head.attributes(s"(case)_$attr").toString))// for log from Disco
      .collect()
    val pw = new PrintWriter(new File(resultPath))
    pw.println(traceAttrs.mkString("\"", "\",\"", "\""))
    traceAttrsList.foreach(x => pw.println(x.mkString("\"", "\",\"", "\"")))
    pw.close
  }

  def caseAndAbsoluteFrequencyV2(log: UnifiedEventLog, eventAttr: String, resultPath: String)  = {

    val convertTraceToListOfAttrValues: (List[UnifiedEvent] => List[String]) = _.map(_.attributes.get(eventAttr)).filter(_.isDefined).map(_.get.toString)
    val absFreq = log.aggregate(convertTraceToListOfAttrValues)
    val caseFreq = log.aggregate(convertTraceToListOfAttrValues(_).distinct).toMap
    val mergedMap = absFreq.map(x => (x._1, (x._2, caseFreq(x._1))))
//    mergedMap

    val pw = new PrintWriter(new File(resultPath))
    pw.println("conceptname, absolute_frequency, case_frequency")
    mergedMap.foreach(x => pw.println(x.toString()))
    pw.close
  }

  def caseAndAbsoluteFrequency(log: UnifiedEventLog, eventAttr: String, resultPath: String): Unit = {

    val occurrencesInterm = log
      .traces()
      .map(tr =>
        tr._2.map(ev =>
          (ev.attributes(eventAttr).toString, tr._1.toString)
        )).collect().flatten

    val absolutFrequency = occurrencesInterm.groupBy(_._1).mapValues(_.size)
    val caseFrequency = ???

//    val pw = new PrintWriter(new File(resultPath))
//    pw.println("conceptname, case frequency, absolute frequency")
//    result.foreach(x => pw.println(x.toString()))
//    pw.close
  }

  def main(args: Array[String]): Unit = {
    logger.info("EvaluationEnvironment started")

    //val log = logOnlyMainAttrs.persist()

    //    writeResultToTxt("C:\\bpic2018_work\\statistic\\resourceDepartment.txt",
    //      resourceDepartmentTable(log))
    //
    //    writeResultToTxt("C:\\bpic2018_work\\statistic\\resourceDepartmentWithoutChangeDepartment.txt",
    //      resourceDepartmentTable(logWithoutChangeDepartment1(logOnlyMainAttrs)))
    //
    //    attr1attr2table1(log, EventAttributes.Resource, EventAttributes.ConceptName,
    //      "C:\\bpic2018_work\\statistic\\resourceConceptName.txt")
    //
    //    attr1attr2table1(log, EventAttributes.ConceptName, EventAttributes.Note,
    //      "C:\\bpic2018_work\\statistic\\conceptNameNote.txt")
    //
    //    attr1attr2table1(log, EventAttributes.Resource, EventAttributes.Note,
    //      "C:\\bpic2018_work\\statistic\\resourceNote.txt")

    //    allTracesAttrs(logOnlyMainAttrs, "C:\\bpic2018_work\\statistic\\allTracesAttrs.csv")

//    attr1attr2table1(log1_2015, EventAttributes.ConceptName, EventAttributes.Success, "C:\\bpic2018_work\\statistic\\conceptnameSuccess_2015.txt")
//    attr1attr2table1(log1_2016, EventAttributes.ConceptName, EventAttributes.Success, "C:\\bpic2018_work\\statistic\\conceptnameSuccess_2016.txt")
//    attr1attr2table1(log1_2017, EventAttributes.ConceptName, EventAttributes.Success, "C:\\bpic2018_work\\statistic\\conceptnameSuccess_2017.txt")

//    caseAndAbsoluteFrequencyV2(log1_2015, EventAttributes.ConceptName, "C:\\bpic2018_work\\statistic\\conceptnameFrequency_2015.txt")
//    caseAndAbsoluteFrequencyV2 (log1_2016, EventAttributes.ConceptName, "C:\\bpic2018_work\\statistic\\conceptnameFrequency_2016.txt")
//    caseAndAbsoluteFrequencyV2 (log1_2017, EventAttributes.ConceptName, "C:\\bpic2018_work\\statistic\\conceptnameFrequency_2017.txt")

  }
}
