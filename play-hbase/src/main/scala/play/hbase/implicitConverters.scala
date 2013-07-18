package play.hbase

import scala.language.implicitConversions
import org.apache.hadoop.hbase.util.Bytes

/**
 * @author zepeng.li@gmail.com
 */
object implicitConverters {
  implicit def family2hbaseBytes(f: Family) = Bytes.toBytes(f.name)

  implicit def column2hbaseBytes(c: Column[_]) = Bytes.toBytes(c.name)

  implicit def rowkey2hbaseBytes(r: RowKey) = r.toBytes

  implicit def boolean2hbaseBytes(v: Boolean) = Bytes.toBytes(v)

  implicit def double2hbaseBytes(v: Double) = Bytes.toBytes(v)

  implicit def float2hbaseBytes(v: Float) = Bytes.toBytes(v)

  implicit def int2hbaseBytes(v: Int) = Bytes.toBytes(v)

  implicit def long2hbaseBytes(v: Long) = Bytes.toBytes(v)

  implicit def short2hbaseBytes(v: Short) = Bytes.toBytes(v)

  implicit def string2hbaseBytes(v: String) = Bytes.toBytes(v)

  implicit def hb2Boolean(bytes: Array[Byte]) = Bytes.toBoolean(bytes)

  implicit def hb2Double(bytes: Array[Byte]) = Bytes.toDouble(bytes)

  implicit def hb2Float(bytes: Array[Byte]) = Bytes.toFloat(bytes)

  implicit def hb2Int(bytes: Array[Byte]) = Bytes.toInt(bytes)

  implicit def hb2Long(bytes: Array[Byte]) = Bytes.toLong(bytes)

  implicit def hb2Short(bytes: Array[Byte]) = Bytes.toShort(bytes)

  implicit def hb2String(bytes: Array[Byte]) = Bytes.toString(bytes)
}