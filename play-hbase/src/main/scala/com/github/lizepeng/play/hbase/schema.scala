package com.github.lizepeng.play.hbase

import scala.collection.convert.WrapAsScala._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.KeyValue
import org.joda.time._
import com.github.lizepeng.play.datetime.Calc._

/**
 * @author zepeng.li@gmail.com
 */
trait RowKey {
  def toBytes: Array[Byte]
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
}


import play.api.libs.json._

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