package com.github.lizepeng.play

import scala.language.implicitConversions
import scala.collection.mutable.ListBuffer
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.fs._
import org.joda.time._
import hbase.implicitConverters._

/**
 * @author zepeng.li@gmail.com
 */
package object hbase {

  val EmptyBytes = Array[Byte]()

  def combine(els: Array[Byte]*) = els.reduce(Bytes.add)

  implicit class RichByteArray(val bytes: Array[Byte]) extends AnyVal {
    def hb2Boolean = Bytes.toBoolean(bytes)

    def hb2Double = Bytes.toDouble(bytes)

    def hb2Float = Bytes.toFloat(bytes)

    def hb2Int = Bytes.toInt(bytes)

    def hb2Long = Bytes.toLong(bytes)

    def hb2Short = Bytes.toShort(bytes)

    def hb2String = Bytes.toString(bytes)

    def min(that: String): Array[Byte] = min(string2hbaseBytes(that))

    def max(that: String): Array[Byte] = max(string2hbaseBytes(that))

    def ===(that: String): Boolean = Bytes.compareTo(bytes, that) == 0

    def min(that: Array[Byte]): Array[Byte] = if (Bytes.compareTo(bytes, that) < 0) bytes else that

    def max(that: Array[Byte]): Array[Byte] = if (Bytes.compareTo(bytes, that) > 0) bytes else that

    def ===(that: Array[Byte]): Boolean = Bytes.compareTo(bytes, that) == 0

    def nextPrefix: Array[Byte] = {
      //TODO fix bug may be flow...
      val another = bytes.clone()
      val last = another.length - 1
      another.update(last, (another(last) + 1).toByte)
      another
    }
  }

  implicit class RichGet(val get: Get) extends AnyVal {
    def during(interval: Interval) = get.setTimeRange(interval.getStartMillis, interval.getEndMillis)

    def apply[A <: RColumn[Any]](cs: Set[A]): Get = {
      cs.foreach {
        col => get.addColumn(col.family.name, col.name)
      }
      get
    }

    def apply[A <: RColumn[Any]](cs: A*): Get = apply(cs.toSet)
  }

  implicit class RichPut(val put: Put) extends AnyVal {
    def apply[A](col: WColumn[A], dt: DateTime, v: A) = {
      put.add(col.family.name, col.name, dt.getMillis, col.encode(v))
    }

    def add[A](col: RWColumn[A], dt: DateTime, v: String) = {
      put.add(col.family.name, col.name, dt.getMillis, col.encode(col.fromText(v)))
    }

    def addFamily(fm: Family, dt: DateTime, v: Array[Byte]) = {
      put.add(fm.name, EmptyBytes, dt.getMillis, v)
    }

    def withoutWriteToWAL = {
      put.setWriteToWAL(false)
      put
    }
  }

  implicit class RichResult(val rlt: Result) extends AnyVal {
    def isNullOrEmpty = rlt == null || rlt.isEmpty

    def apply[A](col: RColumn[A]): Option[A] =
      Option(rlt.getValue(col.family.name, col.name)).map(col.decode)

    def kvs[A](col: RColumn[A]) = rlt.getColumn(col.family.name, col.name)
  }

  implicit class RichScan(val scan: Scan) extends AnyVal {
    def during(interval: Interval) = scan.setTimeRange(interval.getStartMillis, interval.getEndMillis)

    def withCaching(implicit app: play.api.Application): Scan = {
      withCaching(HB.config.getInt("htable.scan.caching").getOrElse(5000))
    }

    def withCaching(caching: Int): Scan = {
      scan.setCaching(caching)
      scan
    }

    def apply[Any](cols: Set[RColumn[Any]]): Scan = {
      cols.foreach {
        col => scan.addColumn(col.family.name, col.name)
      }
      scan
    }

    def apply(cols: RColumn[Any]*): Scan = apply(cols.toSet)

    def apply(fm: Family) = scan.addFamily(fm.name)
  }

  implicit class RichResultScanner(val rs: ResultScanner) extends AnyVal {
    def mapEach[A](block: Array[Result] => A)(implicit app: play.api.Application): ListBuffer[A] = {
      mapEach(HB.config.getInt("htable.put.caching").getOrElse(5000))(block)
    }

    def mapEach[A](cacheSize: Int)(block: Array[Result] => A): ListBuffer[A] = {
      val ret = ListBuffer[A]()
      var cache = Array[Result]()
      do {
        cache = rs.next(cacheSize)
        if (!cache.isEmpty) ret += block(cache)
      } while (!cache.isEmpty)
      ret
    }
  }

  implicit class RichHTable(val t: HTableInterface) extends AnyVal {
    def increment(row: RowKey, counter: Column[Int]): Int = {
      increment(row, counter, 1)
    }

    def increment(row: RowKey, counter: Column[Int], amount: Int): Int = {
      t.incrementColumnValue(row, counter.family, counter, amount).toInt
    }

    def incrementL(row: RowKey, counter: Column[Long]): Long = {
      incrementL(row, counter, 1L)
    }

    def incrementL(row: RowKey, counter: Column[Long], amount: Long): Long = {
      t.incrementColumnValue(row, counter.family, counter, amount)
    }
  }

  implicit class RichPath(val path: Path) extends AnyVal {
    def /(child: String) = new Path(path, child)

    def /(child: Path) = new Path(path, child)
  }

  implicit class RichStringAsPath(val path: String) extends AnyVal {

    def /(child: String) = s"$path${Path.SEPARATOR}$child"
  }

}