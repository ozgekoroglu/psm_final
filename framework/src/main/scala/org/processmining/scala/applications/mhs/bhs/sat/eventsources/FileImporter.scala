package org.processmining.scala.applications.mhs.bhs.sat.eventsources

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.processmining.scala.log.common.csv.spark.CsvReader
import org.processmining.scala.log.common.unified.event.UnifiedEvent
import org.processmining.scala.log.utils.common.csv.common.CsvImportHelper

import scala.collection.immutable.SortedMap

private[bhs] object CsvFilenames {
  val DestinationReply: String = "Destination Reply 12-27-17 through 12-28-17.txt"
  val DestinationRequest: String = "Destination Request 12-27-17 through 12-28-17.txt"
  val SortReport: String = "SortReport 12-27-17 through 12-28-17.txt"
}


private[bhs] class FileImporter(inDir: String) extends Serializable {
  val dateHelper = new CsvImportHelper("yyyy-MM-dd HH:mm:ss.SSS", "US/Central") //to provide time intervals in code

  val csvReader = new CsvReader("\t", "")
  private val Zero: Byte = 0

  private def sortReportFactory(
                                 ID: Int,
                                 PLCPIC: Int,
                                 SACPIC: Int,
                                 LPC: Int,
                                 Location: Int,
                                 Destination: Int,
                                 DestinationState: Int,
                                 ExitState: Int,
                                 EventTS: Int)(a: Array[String]): UnifiedEvent =

    UnifiedEvent(
      dateHelper.extractTimestamp(a(EventTS)),
      LocationLookup(Integer.parseInt(a(Location))),
      SortedMap("ID" -> a(ID),
        "PLCPIC" -> a(PLCPIC),
        "SACPIC" -> a(SACPIC),
        "LPC" -> a(LPC),
        "Location" -> LocationLookup(Integer.parseInt(a(Location))),
        "Destination" -> a(Destination),
        "DestinationState" -> DestinationStateLookup(Integer.parseInt(a(DestinationState))),
        "DestinationStateClass" -> DestinationStateLookup.clazz(Integer.parseInt(a(DestinationState))),
        "ExitState" -> ExitStateLookup(Integer.parseInt(a(ExitState))),
        ExitStateLookup.ExitStateClass -> ExitStateLookup.clazz(Integer.parseInt(a(ExitState)))
      ), None
    )

  def loadSortReport(spark: SparkSession): RDD[UnifiedEvent] = {
    val (header, lines) = csvReader.read(s"$inDir/${CsvFilenames.SortReport}", spark)
    csvReader.parse(
      lines,
      sortReportFactory(header.indexOf("ID"),
        header.indexOf("PLCPIC"),
        header.indexOf("SACPIC"),
        header.indexOf("LPC"),
        header.indexOf("ArrivalLocation"),
        header.indexOf("Destination"),
        header.indexOf("DestinationState"),
        header.indexOf("ExitState"),
        header.indexOf("EventTS")
      )
    ) //.map(x => (x.getAs[String](idName), x))
  }

  private def destinationReplyFactory(
                                       ID: Int,
                                       PLCPIC: Int,
                                       SACPIC: Int,
                                       LPC: Int,
                                       Location: Int,
                                       Destination: Int,
                                       ExceptionID: Int,
                                       EventTS: Int)(a: Array[String]): UnifiedEvent =

    UnifiedEvent(
      dateHelper.extractTimestamp(a(EventTS)),
      LocationLookup(Integer.parseInt(a(Location))),
      SortedMap("ID" -> a(ID),
        "PLCPIC" -> a(PLCPIC),
        "SACPIC" -> a(SACPIC),
        "LPC" -> a(LPC),
        "Location" -> LocationLookup(Integer.parseInt(a(Location))),
        "Destination" -> a(Destination),
        "ExceptionID" -> ExceptionIdLookup(Integer.parseInt(a(ExceptionID))),
        "ExceptionIdClass" -> ExceptionIdLookup.clazz(Integer.parseInt(a(ExceptionID)))
        //        "DestinationState" -> "",
        //        "DestinationStateClass" -> Zero
      ), None
    )

  def loadDestinationReply(spark: SparkSession): RDD[UnifiedEvent] = {
    val (header, lines) = csvReader.read(s"$inDir/${CsvFilenames.DestinationReply}", spark)
    csvReader.parse(
      lines,
      destinationReplyFactory(header.indexOf("ID"),
        header.indexOf("PLCPIC"),
        header.indexOf("SACPIC"),
        header.indexOf("LPC"),
        header.indexOf("Location"),
        header.indexOf("Destination"),
        header.indexOf("ExceptionID"),
        header.indexOf("EventTS")
      )
    )
  }

  private def destinationRequestFactory(
                                         ID: Int,
                                         PLCPIC: Int,
                                         SACPIC: Int,
                                         LPC: Int,
                                         Location: Int,
                                         EventTS: Int)(a: Array[String]): UnifiedEvent =

    UnifiedEvent(
      dateHelper.extractTimestamp(a(EventTS)),
      LocationLookup(Integer.parseInt(a(Location))),
      SortedMap("ID" -> a(ID),
        "PLCPIC" -> a(PLCPIC),
        "SACPIC" -> a(SACPIC),
        "LPC" -> a(LPC),
        "Location" -> LocationLookup(Integer.parseInt(a(Location)))
        //        "DestinationState" -> "",
        //        "DestinationStateClass" -> Zero
      ), None
    )

  def loadDestinationRequest(spark: SparkSession): RDD[UnifiedEvent] = {
    val (header, lines) = csvReader.read(s"$inDir/${CsvFilenames.DestinationRequest}", spark)
    csvReader.parse(
      lines,
      destinationRequestFactory(header.indexOf("ID"),
        header.indexOf("PLCPIC"),
        header.indexOf("SACPIC"),
        header.indexOf("LPC"),
        header.indexOf("Location"),
        header.indexOf("EventTS")
      )
    )
  }


}


private object LocationLookup {
  def apply(n: Int): String = {
    n match {
      case 0 => "Lost"
      case 1 => "Carrousel MU1"
      case 2 => "Carrousel MU2"
      case 3 => "Carrousel MU3"
      case 4 => "Carrousel MU4"
      case 41 => "Carrousel MU4A"
      case 5 => "Carrousel MU5"
      case 51 => "Carrousel MU5A"
      case 6 => "Carrousel MU6"
      case 61 => "Carrousel MU6A"
      case 7 => "Carrousel MU1A"
      case 70 => "Divert to XSM1"
      case 71 => "Divert to XSM2"
      case 72 => "Divert to ME1"
      case 80 => "ME1"
      case 90 => "ATR ETDCL"
      case 91 => "ATR CL3"
      case 92 => "ATR SM1"
      case 93 => "ATR SM1A"
      case _ => n.toString
    }
  }
}

private[bhs] object DestinationStateLookup {

  val DestinationStateClass = "DestinationStateClass"

  private def mapping(n: Int): (String, Byte) = {
    n match {
      case 0 => ("Unknown reason for not sorting", 1)
      case 50 => ("Sorted to / arrived at destination", 0)
      case 51 => ("Destination not valid (unreachable)", 3)
      case 52 => ("Destination full", 4)
      case 53 => ("Failed to divert", 5)
      case 54 => ("No destination", 6)
      case 55 => ("Destination not available", 7)
      case 58 => ("Gap too small", 8)
      case 59 => ("Item too long", 9)
      case 60 => ("Capacity limitation", 10)
      case _ => (n.toString, 11)
    }
  }

  val Last = 12

  def apply(n: Int): String = mapping(n)._1

  def clazz(n: Int): Byte = mapping(n)._2

}

private[bhs] object ExceptionIdLookup {
  def apply(n: Int): String = mapping(n)._1

  def clazz(n: Int): Byte = mapping(n)._2

  val Last = 22

  val ExceptionIdClass = "ExceptionIdClass"

  private def mapping(n: Int): (String, Byte) = {
    n match {
      case 0 => ("No Exception, Normal Sort", 0)
      case 111 => ("No BSM", 1)
      case 112 => ("No BSM", 1)
      case 113 => ("No BSM", 1)

      case 121 => ("Unkown Flight", 2)
      case 123 => ("Unknown Flight", 2)
      case 124 => ("Unknown Flight", 2)

      case 122 => ("No Chute", 3)
      case 125 => ("No Airline", 4)
      case 126 => ("No Sortation Mode", 5)
      case 127 => ("Inbound BSM", 6)
      case 131 => ("Too Early", 7)
      case 132 => ("To Late", 8)
      case 133 => ("HOT", 9)
      case 141 => ("Early", 10)
      case 151 => ("Invalid Chute", 10)
      case 181 => ("No Read", 12)
      case 185 => ("No Read", 12)

      case 182 => ("Invalid Code", 13)
      case 183 => ("Multi Read", 14)
      case 184 => ("Different Code Read", 15)

      case 187 => ("Fall Back Tag", 16)
      case 201 => ("Max Recirculation", 17)
      case 202 => ("Destination Disabled", 18)
      case 900 => ("MCS Destination", 19)
      case 999 => ("Unhandled Exception", 20)
      case _ => (n.toString, 21)

    }
  }
}

private[bhs] object ExitStateLookup {
  def apply(n: Int): String = mapping(n)._1

  def clazz(n: Int): Byte = mapping(n)._2

  val Last = 4
  val ExitStateClass = "ExitStateClass"

  def mapping(n: Int): (String, Byte) = {
    n match {
      case 0 => ("Lost in tracking", 1)
      case 1 => ("Arrived at location (Sorted)", 0)
      case 2 => ("No exit", 2)
      case _ => (n.toString, 3)
    }
  }
}

