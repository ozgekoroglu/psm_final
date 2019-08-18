package org.processmining.scala.log.common.utils.common.export

object EventRelationsTable {

  val fillingKeys: Map[Char, Byte] = Map(
    '>' -> 1,
    '<' -> 2,
    '|' -> 3, // in both directions
    '#' -> 4, // in no way
    '@' -> 5) // loop (can only be on diagonal)

  def efrTableFinish(efr: List[((String, String), Int)]): (Array[Array[Byte]], Map[String, Int]) = {
    val colsRowsKeys = uniqueActivitiesKeys(efr)
    val tableInter = efrTableIntermediate(efr, colsRowsKeys)
    (efrTableFromIntermediateToFinish(tableInter), colsRowsKeys)
  }

  def efrTableIntermediate(efr: List[((String, String), Int)], colsRowsKeys: Map[String, Int]): Array[Array[Boolean]] = {
    val efrTableIntermediate = Array.ofDim[Boolean](colsRowsKeys.size, colsRowsKeys.size)
    efr.foreach(x => efrTableIntermediate(colsRowsKeys(x._1._1))(colsRowsKeys(x._1._2)) = true)
    efrTableIntermediate
  }

  def efrTableFromIntermediateToFinish(followingsArray: Array[Array[Boolean]]): Array[Array[Byte]] = {
    val efrTableFinish = Array.ofDim[Byte](followingsArray.size, followingsArray.size)
    for {
      row <- 0 until efrTableFinish.size
      col <- +row until efrTableFinish.size // after diagonal
    } yield {
      if (followingsArray(row)(col) == true && followingsArray(col)(row) == true) { //   >,>  |,|
        efrTableFinish(row)(col) = fillingKeys('|') //   |
        efrTableFinish(col)(row) = fillingKeys('|')
      }

      else if (followingsArray(row)(col) == true && followingsArray(col)(row) == false) { //   >,#  >,<
        efrTableFinish(row)(col) = fillingKeys('>') //   >
        efrTableFinish(col)(row) = fillingKeys('<') //   <
      }

      else if (followingsArray(row)(col) == false && followingsArray(col)(row) == true) { //   #,>  <,>
        efrTableFinish(row)(col) = fillingKeys('<') //   <
        efrTableFinish(col)(row) = fillingKeys('>') //   >
      }

      else { ///if (followingsArray(row)(col) == false && followingsArray(col)(row) == false) { //   #,#  #,#
        efrTableFinish(row)(col) = fillingKeys('#') //   #
        efrTableFinish(col)(row) = fillingKeys('#') //   #
      }
    }

    //        if (followingsArray(row)(col) == true) { //">"
    //          if (followingsArray(col)(row) == true) {
    //            efrTableFinish(row)(col) = fillingKeys(3) // "|"
    //            efrTableFinish(col)(row) = fillingKeys(3)
    //          } else { //if (followingsArray(col)(row) == "#")
    //            efrTableFinish(row)(col) = fillingKeys(1) // ">"
    //            efrTableFinish(col)(row) = fillingKeys(2) // "<"
    //          }
    //        }
    //        else {
    //          efrTableFinish(row)(col) = followingsArray(row)(col)
    //          efrTableFinish(col)(row) = followingsArray(col)(row)
    //        }
    for {
      row <- 0 until efrTableFinish.size // diagonal
    } yield {
      if (followingsArray(row)(row) == true) { //   >,>  |,|
        efrTableFinish(row)(row) = fillingKeys('@') //   @ loop
      }
      else efrTableFinish(row)(row) = fillingKeys('#') //   #

    }
    efrTableFinish
  }


  def uniqueActivitiesKeys(efr: List[((String, String), Int)]): Map[String, Int] = {
    (efr.map(t => t._1).map(_._1) :::
      efr.map(t => t._1).map(_._2))
        .sorted.toSet.zipWithIndex.toMap
  }


}
