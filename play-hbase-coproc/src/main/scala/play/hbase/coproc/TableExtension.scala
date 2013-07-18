package play.hbase.coproc

import java.io.IOException
import org.apache.hadoop.hbase.coprocessor._
import org.apache.hadoop.hbase.filter._
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.regionserver.RegionScanner
import java.util

/**
 * @author zepeng.li@gamil.com
 */
trait TableExtensionProtocol extends CoprocessorProtocol {
  @throws(classOf[IOException])
  def count(prefix: Array[Byte], countKV: Boolean = false, filter: Filter = new FirstKeyOnlyFilter()): Long
}

class TableExtensionImpl extends BaseEndpointCoprocessor with TableExtensionProtocol {

  def count(prefix: Array[Byte], countKV: Boolean, filter: Filter) = {
    withScanner(prefix, filter) { scanner =>
      var count = 0
      var hasMore = false
      val current = new util.ArrayList[KeyValue]()
      do {
        hasMore = scanner.next(current)
        count += (if (countKV) current.size() else 1)
        current.clear()
      } while (hasMore)
      count
    }
  }

  def withScanner[A](prefix: Array[Byte], filter: Filter)(block: RegionScanner => A): A = {
    val env = getEnvironment.asInstanceOf[RegionCoprocessorEnvironment]
    val scan = new Scan(prefix).setFilter(new FilterList(new PrefixFilter(prefix), filter))
    val scanner = env.getRegion.getScanner(scan)
    try {
      block(scanner)
    } finally {
      scanner.close()
    }
  }
}