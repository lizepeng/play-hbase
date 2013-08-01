package com.github.lizepeng.play.hbase

import scala.collection.convert.WrapAsScala._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter._
import org.apache.hadoop.hbase.KeyValue
import org.joda.time._
import com.github.lizepeng.play.datetime.Calc._
import play.api.Application
import implicitConverters._

/**
 * @author zepeng.li@gmail.com
 */
trait RowKey {
  def toBytes: Array[Byte]
}

trait HBTable {
  val name: String

  def withHTable[A](block: HTableInterface => A)(implicit app: Application): A = {
    HB.withHTable(name)(block)
  }
}

case class Family(name: String)

abstract case class Column[A](family: Family, name: String = "") extends RWColumn[A]

trait RWColumn[A] extends RColumn[A] with WColumn[A]

trait RColumn[+A] {

  val family: Family

  val name: String

  def decode(bytes: Array[Byte]): A

  def fromText(str: String): A
}

trait WColumn[-A] {

  val family: Family

  val name: String

  def encode(v: A): Array[Byte]

  def toText(v: A): String

  def singleColValFilter(v: A) = {
    new SingleColumnValueFilter(family.name, name, CompareFilter.CompareOp.EQUAL, encode(v))
  }
}

import play.api.libs.json._

/**
 * Use json as the format of a instance saving into HBase.
 */
trait JsonRWColumn[A] extends RWColumn[A] {

  val defaultValue: A

  val jsonColumn: Column[JsValue]

  val jsonFormats: Format[A]

  def decode(bytes: Array[Byte]): A = decodeToJson(bytes).getOrElse(defaultValue)

  def fromText(str: String): A = fromTextToJson(str).getOrElse(defaultValue)

  def decodeToJson(bytes: Array[Byte]): JsResult[A] = Json.fromJson(jsonColumn.decode(bytes))(jsonFormats)

  def fromTextToJson(str: String): JsResult[A] = Json.fromJson(jsonColumn.fromText(str))(jsonFormats)

  def encode(v: A): Array[Byte] = jsonColumn.encode(toJson(v))

  def toText(v: A): String = jsonColumn.toText(toJson(v))

  def toJson(v: A): JsValue = Json.toJson(v)(jsonFormats)
}

abstract case class JsonColumn[A](family: Family, name: String = "") extends JsonRWColumn[A] {
  val jsonColumn = new Column[JsValue](family, name) with JsonConverter
}

/**
 * Save time series data as versions of column into HBase.
 * TS is equal to Time Series.
 */
trait TSRColumn[+A] extends RColumn[A] {
  def toDailyData(r: Result): Map[LocalDate, A] = toDailyData(r.kvs(this))

  def toDailyData(kvs: Seq[KeyValue]): Map[LocalDate, A] = {
    kvs.map {
      kv => kv.getTimestamp.toLocalDate -> decode(kv.getValue)
    }.toMap[LocalDate, A]
  }

  def toMonthlyData(kvs: Seq[KeyValue]): Map[YearMonth, A] = {
    kvs.map {
      kv => kv.getTimestamp.toYearMonth -> decode(kv.getValue)
    }.toMap[YearMonth, A]
  }
}

abstract case class TSColumn[A](
  family: Family,
  name: String = ""
) extends RWColumn[A] with TSRColumn[A]