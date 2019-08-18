package org.processmining.scala.applications.mhs.bhs.bpi

private[bhs] object TaskReportEvent {

  val ClazzCount = 31

  def taskClassifier(task: String): Byte =
    task match {
      case "ScreenL1L2" => 1
      case "Store" => 2
      case "RouteToMC" => 3
      case "AutoScan" => 4
      case "VolumeScan" => 5
      case "ProblemBag" => 6
      case "ProblemBagStore" => 6
      case "ManualScan" => 7
      case "Dump" => 8
      case "Batch" => 9
      case "DataCapture" => 10
      case "Deregistration" => 11
      case "LinkToFlight" => 12
      case "LinkToHandler" => 13
      case "ManualDeliver" => 14
      case "ManualScreen" => 16
      case "ManualStore" => 17
      case "OperationalControl" => 18
      case "Reclaim" => 19
      case "Registration" => 20
      case "Release" => 21
      case "RequestAirportTag" => 22
      case "RequestTask" => 23
      case "Retag" => 24
      case "RouteToCache" => 25
      case "RouteToOutputPoint" => 26
      case "ScreenL2" => 27
      case "ScreenL4" => 28
      case "SpecialDestination" => 29
      case _ => 30
    }
}

