package com.github.lizepeng.play.hbase.migrate

import scala.io._
import java.io._

/**
 * @author zepeng.li@gmail.com
 */
class FileLineProducer(is : InputStream, enc : String) extends LineProducer[String] {
  import LineProducer._

  val file = Source.fromInputStream(is, enc)

  val lines = file.getLines()

  def hasMore = lines.hasNext

  def whenNoMore() { file.close() }

  def fetchLines(num: Int): Lines[String] = {
    for (i <- 1 to num; if lines.hasNext) yield {(counter + i, lines.next())}
  }
}