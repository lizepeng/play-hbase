package play.hbase.migrate

import scala.io._
import java.io.File

/**
 * @author zepeng.li@gmail.com
 */
class FileLineProducer(path: String, name: String) extends LineProducer[String] {
  import LineProducer._

  val file = Source.fromFile(new File(path, name))
  val lines = file.getLines()

  def hasMore = lines.hasNext

  def whenNoMore() { file.close() }

  def fetchLines(num: Int): Lines[String] = {
    for (i <- 1 to num; if lines.hasNext) yield {(counter + i, lines.next())}
  }
}