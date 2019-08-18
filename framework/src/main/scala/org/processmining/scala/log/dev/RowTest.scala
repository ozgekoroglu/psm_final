//package org.processmining.scala.log.dev
//
//import java.util.regex.Pattern
//
//import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
//import org.processmining.scala.log.common.utils.spark.AbstractSession
//
//
//private object RowTest extends AbstractSession("Rows") {
//
//  val schema = StructType(List(StructField("id1", IntegerType, false), StructField("id2", IntegerType, false)))
//
//  def main(args: Array[String]): Unit = {
//
//    System.out.println("<1@0#><2@1#><3@2#><4@3#><5@4#>".replaceFirst("<1@\\d+#.*?>", ""))
//
//    //val pattern = Pattern.compile("<1@\\d+#.*?>")
//    val pattern = Pattern.compile("<1@\\d+#.*>")
//    val s = "<1@0#><2@1#><3@2#><4@3#><5@4#>"
//
//    val m = pattern.matcher(s).replaceFirst("")
//    println(m)
//
//    println(m)
//
//
//
//
//
////    val rdd = spark.sparkContext.parallelize(1 to Int.MaxValue) union spark.sparkContext.parallelize(1 to Int.MaxValue)
////
////    val s = rdd
////      .map(x => (x, x + 1))
////      .toDS()
////      .reduce((x, y) => (x._1 + y._1, x._2 + y._2))
////    println(s)
////
////    val s2 = rdd
////      .map(x => new GenericRowWithSchema(Array(x, x + 1), schema))
////      .reduce((x, y) =>
////        new GenericRowWithSchema(Array(
////          x.getAs[Int]("id1") + y.getAs[Int]("id1"),
////          x.getAs[Int]("id2") + y.getAs[Int]("id2")), schema))
////
////    println(s2.getAs[Int]("id1"))
////    println(s2.getAs[Int]("id2"))
////
//
//  }
//}
