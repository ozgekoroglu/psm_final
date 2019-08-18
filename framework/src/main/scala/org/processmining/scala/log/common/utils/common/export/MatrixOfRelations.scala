package org.processmining.scala.log.common.utils.common.export

import scala.reflect.ClassTag

/* For work with EFR/DFR relations */
object MatrixOfRelations {

  /* sort items according to sortingOrder */
  def sortByOrderedList(sortingOrder: List[String], items: Set[String]): List[String] =
    sortingOrder.filter(items.contains) ::: items.filter(!sortingOrder.contains(_)).toList.sorted

  /**
    * Build a matrix of relations (like log footprint matrix) based on a list of relations
    *
    * @param relations list of relations
    * @param sortFunc  provides desired order of activities
    * @return footprint matrix of tuples (number_of_>, number_of_<, is_diagonal_cell) and names of matrix columns/rows
    */
  def apply[T: ClassTag](relations: Array[((String, String), T)], sortFunc: (Set[String]) => List[String], zero: T):
  (Array[Array[(T, T, Boolean)]], List[String]) = {
    val sortedActivities = sortFunc(relations.flatMap(x => Seq(x._1._1, x._1._2)).toSet)
    val zippedSortedActivities = sortedActivities.zipWithIndex
    val mapper = zippedSortedActivities.map(x => x._1 -> x._2).toMap
    val matrix = Array.fill[(T, T, Boolean)](zippedSortedActivities.length, zippedSortedActivities.length)((zero, zero, false))
    relations.foreach { x => matrix(mapper(x._1._1))(mapper(x._1._2)) = (x._2, zero, mapper(x._1._1) == mapper(x._1._2)) }
    relations.foreach {
      x =>
        matrix(mapper(x._1._2))(mapper(x._1._1)) =
          (matrix(mapper(x._1._2))(mapper(x._1._1))._1, // preserving an old value
            matrix(mapper(x._1._1))(mapper(x._1._2))._1, // assigning a new value
            matrix(mapper(x._1._1))(mapper(x._1._2))._3 // preserving an old value
          )
    }
    (matrix, sortedActivities)
  }



}
