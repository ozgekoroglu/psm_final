package org.processmining.scala.applications.bp.bpi_challenge.scenarios.spark.dev.billion

import java.io.{BufferedWriter, File, FileWriter}

import com.thoughtworks.xstream.XStream
import com.thoughtworks.xstream.io.xml.DomDriver

protected case class Config(scale: Int, partitions: Int, inDir: String) {
  def toXml(): String = new XStream(new DomDriver).toXML(this)
}

protected object Config {
  def apply(canonicalFilename: String): Config =
    new XStream(new DomDriver).fromXML(new File(canonicalFilename)).asInstanceOf[Config]

  def toDisk(config: Config, canonicalFilename: String): Unit = {
    val bw = new BufferedWriter(new FileWriter(new File(canonicalFilename)))
    bw.write(config.toXml())
    bw.close()
  }
}