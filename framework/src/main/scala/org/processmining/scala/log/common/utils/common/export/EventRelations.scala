package org.processmining.scala.log.common.utils.common.export

import org.processmining.scala.log.common.unified.event.UnifiedEvent

import scala.reflect.ClassTag

object EventRelations {

  val DefaultSeparator = ":"

  def count(e1: UnifiedEvent, e2: UnifiedEvent): Int = 1

  def efr[T: ClassTag](areEventsSimilar: (UnifiedEvent, UnifiedEvent) => Boolean,
                       events: List[UnifiedEvent],
                       f: (UnifiedEvent, UnifiedEvent) => T): List[((String, String), T)] = {
    val groupedEvents = groupByUdf(events, areEventsSimilar)
    findEFREvents(groupedEvents)
      .map(x => ((x._1.activity, x._2.activity), f(x._1, x._2)))
  }

  def dfr[T: ClassTag](areEventsSimilar: (UnifiedEvent, UnifiedEvent) => Boolean,
                       events: List[UnifiedEvent],
                       f: (UnifiedEvent, UnifiedEvent) => T): List[((String, String), T)] = {
    val groupedEvents = groupByUdf(events, areEventsSimilar)
    findDFREvents(groupedEvents)
      .map(x => ((x._1.activity, x._2.activity), f(x._1, x._2)))
  }

  /* Creates a list of sorted and merged activities names for sequences of similar events */
  def mergeSimilarActivities(areEventsSimilar: (UnifiedEvent, UnifiedEvent) => Boolean, sortActivities: Boolean, events: List[UnifiedEvent]): List[String] =
    groupByUdf(events, areEventsSimilar)
      .filter(_.size > 1)
      .map(x => (if (sortActivities) x.sortBy(_.activity) else x).map(_.activity).mkString(DefaultSeparator))


  /**
    * Group similar events of a trace in terms of a given UDF
    *
    * @param orderedEvents    events ordered by timestamps ASC
    * @param areEventsSimilar function to check if 2 events are similar
    * @return a list of lists of event groups
    */
  def groupByUdf(orderedEvents: List[UnifiedEvent], areEventsSimilar: (UnifiedEvent, UnifiedEvent) => Boolean): List[List[UnifiedEvent]] = {
    val z: (List[List[UnifiedEvent]], Option[UnifiedEvent]) = (List(), None)
    orderedEvents.foldLeft(z)((z, e) => if (z._2.isDefined && areEventsSimilar(z._2.get, e))
      ((e :: z._1.head) :: z._1.tail, z._2)
    else
      (List(e) :: z._1, Some(e))

    )._1.reverse.map(_.reverse)
  }

  /*
  val eventsFromTraceGroupedByTime = List(List("A", "B"), List("C", "D"), List("E", "F"), List("K", "L"))
  findEFR(eventsFromTraceGroupedByTime).sortBy(_._1).sortBy(_._2)
  res27: List[(String, String)] = List((B,A), (A,B), (A,C), (B,C), (D,C), (A,D), (B,D), (C,D), (A,E), (B,E), (C,E), (D,E), (F,E), (A,F), (B,F), (C,F), (D,F), (E,F), (A,K), (B,K), (C,K), (D,K), (E,K), (F,K), (L,K), (A,L), (B,L), (C,L), (D,L), (E,L), (F,L), (K,L))
  scala> findEFR(eventsFromTraceGroupedByTime).size
  res26: Int = 32
   */


  def groupsOfEventsToGroupsOfActivities(groups: List[List[UnifiedEvent]]): List[List[String]] =
    groups.map(_.map(_.activity))

  def findEFREvents(groups: List[List[UnifiedEvent]]): List[(UnifiedEvent, UnifiedEvent)] =
    findEFR(groups)

  def findEFR[T: ClassTag](groups: List[List[T]]): List[(T, T)] =
    findEFRImpl(groups)

  def findDFREvents(groups: List[List[UnifiedEvent]]): List[(UnifiedEvent, UnifiedEvent)] =
    findDFR(groups)

  def findDFR[T: ClassTag](groups: List[List[T]]): List[(T, T)] =
    findDFRImpl(groups)


  private def findEFRImpl[T: ClassTag](groups: List[List[T]]): List[(T, T)] =
    groups match {
      case Nil => Nil
      case x :: Nil => crossJoinInListAll(x)
      case x1 :: xs =>
        crossJoinInListAll(x1) :::
          xs.flatMap(x2 => crossJoinOfSeqs(x1 :: x2 :: Nil).map(list => (list.head, list(1)))) :::
          findEFRImpl(xs)
    }

  private def findDFRImpl[T: ClassTag](groups: List[List[T]]): List[(T, T)] =
    groups match {
      case Nil => Nil
      case x :: Nil => crossJoinInListAll(x)
      case x1 :: xs =>
        crossJoinInListAll(x1) :::
          crossJoinOfSeqs(x1 :: xs.head :: Nil).map(list => (list.head, list(1))) :::
          findDFRImpl(xs)
    }


  /*
scala>  val listExample = List("A", "B", "C", "D")
scala> crossJoinInListAll(listExample)
res20: List[(String, String)] = List((A,B), (A,C), (A,D), (B,A), (B,C), (B,D), (C,A), (C,B), (C,D), (D,A), (D,B), (D,C))
 */
  def crossJoinInListAll[T: ClassTag](list: List[T]): List[(T, T)] =
    for {
      (x, idxX) <- list.zipWithIndex
      (y, idxY) <- list.zipWithIndex
      if idxX != idxY
    } yield (x, y)

  /*
scala>  val listExample = List("A", "B", "C", "D")
scala> crossJoinInList(listExample)
res0: List[(String, String)] = List((A,B), (A,C), (A,D), (B,C), (B,D), (C,D))
 */
  def crossJoinInListFollowed[T: ClassTag](list: List[T]): List[(T, T)] =
    list match {
      case Nil => Nil
      case x :: Nil => Nil
      case x :: xTail => xTail.map(m => (x, m)) ::: crossJoinInListFollowed(xTail)
    }

  /*
scala> val listExample = List(List("A","B"), List("C", "D"))
scala> crossJoin(listExample)
res0: Traversable[Traversable[String]] = List(List(A, C), List(A, D), List(B, C), List(B, D))
but:
scala> val listExample = List(List("A","B"), List("C", "D"), List("E", "F"))
res1: Traversable[Traversable[String]] = List(List(A, C, E), List(A, C, F), List(A, D, E), List(A, D, F), List(B, C, E), List(B, C, F), List(B, D, E), List(B, D, F))
 */
  def crossJoinOfSeqs[T](groups: List[List[T]]): List[List[T]] =
    groups match {
      case Nil => Nil
      case x :: Nil => x map (List(_))
      case x :: xs => for {
        i <- x
        j <- crossJoinOfSeqs(xs)
      } yield List(i) ++ j
    }
}
