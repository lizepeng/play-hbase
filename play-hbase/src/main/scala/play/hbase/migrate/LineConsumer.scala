package play.hbase.migrate

import akka.actor._
import akka.event.LoggingReceive
import util._

/**
 * @author zepeng.li@gmail.com
 */
object LineConsumer {
  case class Start()
}

trait LineConsumer[A] extends Actor with ActorLogging {
  import LineProducer._
  import LineConsumer._

  val source: ActorRef
  val limit: Int
  var counter = 0

  def receive = LoggingReceive {
    case Start() => fetchMore()

    case lines: Lines[A] => {
      log.debug(s"done ${lines.size} lines")
      counter += lines.size
      consume(lines)
      fetchMore()
    }

    case NoMore() => {
      log.info(s"ok, consumed $counter lines.")
      self ! PoisonPill
    }
  }

  def fetchMore() { source ! FetchLines(limit) }

  def consume(lines: Lines[A])
}