package org.processmining.scala.applications.mhs.bhs.t3.eventsources

private[bhs]  object ParameterClassifiers {
  val RecirculationClassifierClazzCount = 9


  def recirculationClassifier(count: Int): Int = count match {
    case n if 1 until 4 contains n => 1
    case n if 4 until 7 contains n => 2
    case n if 7 until 10 contains n => 3
    case n if 10 until 13 contains n => 4
    case n if 13 until 16 contains n => 5
    case n if 16 until 19 contains n => 6
    case n if 19 until 22 contains n => 7
    case _ => 8
  }

  def lengthClassifier(wString: String): Int =
    if (wString.isEmpty || wString == "0") 0
    else wString.toInt match {
      case w if 0 until 77 contains w => 1
      case w if 77 until 90 contains w => 2
      case w if 90 until 100 contains w => 3
      case w if 100 until 110 contains w => 4
      case _ => 5
    }

  def weightClassifier(wString: String): Int =
    if (wString.isEmpty || wString == "0") 0
    else wString.toInt match {
      case w if 0 until 24 contains w => 1
      case w if 24 until 30 contains w => 2
      case w if 30 until 35 contains w => 3
      case _ => 4
    }

  def screeningClassifier(wString: String): Int =
    wString match {
      case "" => 0
      case "ID=1,ScreeningLevel=1,Result=CLEARED" => 1
      case "ID=1,ScreeningLevel=1,Result=NODECISION" => 2
      case "ID=1,ScreeningLevel=1,Result=UNCLEARED" => 3
      case "ID=1,ScreeningLevel=2,Result=CLEARED" => 4
      case "ID=1,ScreeningLevel=2,Result=UNCLEARED" => 5
      case _ => 6
    }


}
