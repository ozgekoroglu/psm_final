package org.processmining.scala.prediction.preprocessing.dev

import org.apache.log4j.PropertyConfigurator
import org.processmining.scala.log.utils.common.errorhandling.JvmParams
import org.processmining.scala.prediction.preprocessing._
import org.slf4j.LoggerFactory
import com.esotericsoftware.kryo.Kryo
import java.io.{ByteArrayInputStream, ByteArrayOutputStream, FileInputStream, FileOutputStream}

import com.esotericsoftware.kryo.io.{Input, Output}
import org.processmining.scala.log.common.enhancment.segments.common._

object Serialization {
  private val logger = LoggerFactory.getLogger(Serialization.getClass)

  val kryo = new Kryo
  val s1 = SegmentImpl("someID1", 123451L, 1000, "field1", 1)
  val s2 = SegmentImpl("someID2", 123452L, 2000, "field2", 2)
  val s3 = SegmentImpl("someID3", 123453, 3000, "field3", 3)
  //val w = Map(1 -> s1, 2 -> s2, 3 -> s3)
  //val x = List(s1, s2, s3)
  val x: List[SegmentImpl] = List()
  val y = BinsImpl(100, 1, 2, 3)
  val z = Map(1 -> 100, 2 -> 200, 3 -> 300)

  def f1() = {
    val byteOutputStream = //new Output(new FileOutputStream("x.bin"))
      new ByteArrayOutputStream()
    val output = new Output(byteOutputStream)
    kryo.writeObject(output, ListOfSegments(x))
    //MapOfSegmentImplSerializer.write(kryo, output, x)
    output.flush()
    val ba = byteOutputStream.toByteArray
    byteOutputStream.close
    val input = new Input(new ByteArrayInputStream(ba))
    val object2 = kryo.readObject(input, classOf[ListOfSegments])
    logger.info(object2.s.toString)
  }

  def f2() = {
    val byteOutputStream = //new Output(new FileOutputStream("x.bin"))
      new ByteArrayOutputStream()
    val output = new Output(byteOutputStream)
    kryo.writeObject(output, y)
    //MapOfSegmentImplSerializer.write(kryo, output, x)
    output.flush()
    val ba = byteOutputStream.toByteArray
    byteOutputStream.close
    val input = new Input(new ByteArrayInputStream(ba))
    val object2 = kryo.readObject(input, classOf[BinsImpl])
    logger.info(object2.toString)
  }

  def f3() = {
    val byteOutputStream = //new Output(new FileOutputStream("x.bin"))
      new ByteArrayOutputStream()
    val output = new Output(byteOutputStream)
    kryo.writeObject(output, MapOfInts(z))
    //MapOfSegmentImplSerializer.write(kryo, output, x)
    output.flush()
    val ba = byteOutputStream.toByteArray
    byteOutputStream.close
    val input = new Input(new ByteArrayInputStream(ba))
    val object2 = kryo.readObject(input, classOf[MapOfInts])
    logger.info(object2.s.toString)
  }

  def fd() = {
    val byteOutputStream = //new Output(new FileOutputStream("x.bin"))
      new ByteArrayOutputStream()
    val output = new Output(byteOutputStream)
    kryo.writeObject(output, MapOfInts(Map()))
    //MapOfSegmentImplSerializer.write(kryo, output, x)
    output.flush()
    val ba = byteOutputStream.toByteArray
    byteOutputStream.close
    val input = new Input(new ByteArrayInputStream(ba))
    val object2 = kryo.readObject(input, classOf[MapOfInts])
    logger.info(object2.s.toString)
  }


  def main(args: Array[String]): Unit = {
    PropertyConfigurator.configure("./log4j.properties")
    JvmParams.reportToLog(logger, "SimSegmentsToSpectrumStarter started")
    try {

      kryo.register(classOf[SegmentImpl], SegmentImplSerializer)
      kryo.register(classOf[BinsImpl], BinsImplSerializer)
      kryo.register(classOf[MapOfSegments], MapOfSegmentImplSerializer)
      kryo.register(classOf[ListOfSegments], ListOfSegmentImplSerializer)
      kryo.register(classOf[MapOfInts], MapOfIntsSerializer)
      fd()
      f1()
      f2()
      f3()

    } catch {
      case e: Throwable => logger.error(e.toString)
    }
    logger.info(s"App is completed.")
  }

}
