package org.processmining.scala.applications.bp.bpic2018

object Following {
  def main(args: Array[String]): Unit = {

    // for testing
    //      | A | B | C | D
    //    A | 0 | > | > | >
    //    B | > | 0 | > | #
    //    C | # | > | 0 | #
    //    D | # | > | # | 0

    //      | A | B | C | D
    //    A | 0 | | | > | >
    //    B | | | 0 | | | <
    //    C | < | | | 0 | #
    //    D | < | > | # | 0

    val listCases = List(List("A", "B", "C"), List("A", "C", "B"), List("A", "D", "B"), List("B", "A"))
    println(s"size of listCases = ${listCases.size}")
    //    val listActivities = List("A", "B", "C", "D")
    val mapActivities: Map[Int, String] = Map(0 -> "A", 1 -> "B", 2 -> "C", 3 -> "D")
    val mapSimbols: Map[Byte, String] = Map(0.toByte -> "0", 1.toByte -> ">", 2.toByte -> "<", 3.toByte -> "|", 4.toByte -> "#")
    println(s"mapActivities = $mapActivities")
    println(s"size of mapActivities = ${mapActivities.size}")
    //    val followingsArray = Array.ofDim[String](listActivities.size, listActivities.size) // создание квадратной марицы
    //    println(s"size of followingsArray = ${followingsArray.size}")


    def fillingFollowingsArray(listCases: List[List[String]], mapActivities: Map[Int, String]): Array[Array[Byte]] = {
      val followingsArray = Array.ofDim[Byte](mapActivities.size, mapActivities.size) // создание квадратной марицы
      mapActivities.map(x => // проход по рядам
        mapActivities.map(y => // проход по столбцам
          followingsArray(x._1)(y._1) = searchInAllCases(x._2, y._2, listCases))) // присвоение ячейке матрицы результата поиска
      println(followingsArray.deep.mkString("\n")) // печать первоначальной матрицы

      /*
      преобразование матрицы (отзеркаливание по диагонали)
       */
      def combination(followingsArray: Array[Array[Byte]]): Array[Array[Byte]] = {
        val followingsArrayComb = Array.ofDim[Byte](followingsArray.size, followingsArray.size)
        for {
          row <- 0 until followingsArrayComb.size
          col <- +row until followingsArrayComb.size
        }
          if (followingsArray(row)(col) == 1.toByte) { //">"
              if (followingsArray(col)(row) == 1.toByte) {
                followingsArrayComb(row)(col) = 3.toByte // "|"
                followingsArrayComb(col)(row) = 3.toByte
              } else { //if (followingsArray(col)(row) == "#")
                followingsArrayComb(row)(col) = 1.toByte // ">"
                followingsArrayComb(col)(row) = 2.toByte // "<"
              }
//            if (followingsArray(row)(col) == ">") {
//              if (followingsArray(col)(row) == ">") {
//                followingsArrayComb(row)(col) = "|"
//                followingsArrayComb(col)(row) = "|"
//              } else { //if (followingsArray(col)(row) == "#")
//                followingsArrayComb(row)(col) = ">"
//                followingsArrayComb(col)(row) = "<"
//              }
          }
          else {
            followingsArrayComb(row)(col) = followingsArray(row)(col)
            followingsArrayComb(col)(row) = followingsArray(col)(row)
          }
        followingsArrayComb
      }

      combination(followingsArray)
    }

    def findEventuallyFollovings(attrValue1: String, attrValue2: String, trace: List[String]): Boolean = trace match {
      case Nil => false
      case x :: xs =>
        if (x == attrValue1) xs match {
          case Nil => false
          case y :: ys => xs.find(f => f == attrValue2).isDefined
        }
        else if (xs != Nil) findEventuallyFollovings(attrValue1, attrValue2, xs)
        else false
    }

    def searchInAllCases(col: String, row: String, listCases: List[List[String]]): Byte = listCases match {
            case Nil => 0 // если изначально нет кейсов должно быть исключение!!!!
      case x :: xtail => { // проверка одного кейса
        if (col == row) 0.toByte
        else if (findEventuallyFollovings(col, row, x)) 1.toByte // ">" +смена row тк дальше искать по кейсам нет смысла
        else if (listCases.tail != Nil) searchInAllCases(col, row, xtail) //  переход к следующему кейсу
        else 4.toByte // "#" если мы дошли до конца, списка кейсов, а соответствия так и не обнаружили
      } //  +смена row
    }
//    def searchInAllCases(col: String, row: String, listCases: List[List[String]]): String = listCases match {
//      //      case Nil => exeption!!!!!! // если изначально нет кейсов должно быть исключение
//      case x :: xtail => { // проверка одного кейса
//        if (col == row) "0" // +смена row
//        else if (findEventuallyFollovings(col, row, x)) ">" // +смена row тк дальше искать по кейсам нет смысла
//        else if (listCases.tail != Nil) searchInAllCases(col, row, xtail) // переход к следующему кейсу
//        else "#" // если мы дошли до конца, списка кейсов, а соответствия так и не обнаружили
//      } //  +смена row
//    }

    println(fillingFollowingsArray(listCases, mapActivities).deep.mkString("\n"))//.map(x=> mapSimbols(x.toByte)))
    //        def printArrayOfArraysOfStrings(array: Array[Array[String]]): Unit = {
    //          for {
    //            row <- 0 until array.size
    //            col <- 0 until array.size
    //          } println(s"($row)($col) = ${array(row)(col)}")
    //        }
    //    printArrayOfArraysOfStrings(fillingFollowingsArray(listCases, mapActivities))
  }

}
