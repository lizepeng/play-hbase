package play.hbase.migrate

import akka.actor._
import akka.event.LoggingReceive
import play.hbase._
import play.logging._
import play.syntactic.Sugar._

/**
 * @author zepeng.li@gmail.com
 */
object LineProducer {
  case class FetchLines(num: Int)
  case class NoMore()
  type Lines[A] = Seq[(Long, A)]
}

trait LineProducer[A] extends Actor with ActorLogging with TimeLogging {
  import LineProducer._
  import context._
  protected var counter = 0L

  def hasMore: Boolean

  def receive = LoggingReceive {
    case FetchLines(num) =>
      if (hasMore) {
        val ret = fetchLines(num)
        counter += ret.size
        log.info(s"produced $counter lines. $timeSinceStart") unless counter % 100000 != 0
        sender ! ret
      }
      else {
        log.info(s"No more lines, produced $counter lines. $timeSinceStart")
        whenNoMore()
        sender ! NoMore()
        become(noMore)
      }
  }

  def noMore: Receive = {
    case _ => sender ! NoMore()
  }

  def whenNoMore()

  def fetchLines(num: Int): Lines[A]
}