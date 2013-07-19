package com.github.lizepeng.play.hbase

import org.apache.hadoop.hbase.util.Bytes

/**
 * @author zepeng.li@gmail.com
 */
trait DoubleConverter {

  def decode(bytes: Array[Byte]) = Bytes.toDouble(bytes)

  def fromText(str: String) = str.toDouble

  def encode(v: Double): Array[Byte] = Bytes.toBytes(v)

  def toText(v: Double) = v.toString
}

trait LongConverter {

  def decode(bytes: Array[Byte]) = Bytes.toLong(bytes)

  def fromText(str: String) = str.toLong

  def encode(v: Long): Array[Byte] = Bytes.toBytes(v)

  def toText(v: Long) = v.toString
}

trait IntConverter {

  def decode(bytes: Array[Byte]) = Bytes.toInt(bytes)

  def fromText(str: String) = str.toInt

  def encode(v: Int): Array[Byte] = Bytes.toBytes(v)

  def toText(v: Int) = v.toString
}

trait StringConverter {

  def decode(bytes: Array[Byte]) = Bytes.toString(bytes)

  def fromText(str: String) = str

  def encode(v: String): Array[Byte] = Bytes.toBytes(v)

  def toText(v: String) = v

}

trait BytesConverter {

  def decode(bytes: Array[Byte]) = bytes

  def fromText(str: String) = Bytes.toBytes(str)

  def encode(v: Array[Byte]): Array[Byte] = v

  def toText(v: Array[Byte]) = Bytes.toString(v)
}

import play.api.libs.json._

trait JsonConverter {

  def decode(bytes: Array[Byte]): JsValue = Json.parse(bytes)

  def fromText(str: String): JsValue = Json.parse(str)

  def encode(v: JsValue): Array[Byte] = Bytes.toBytes(toText(v))

  def toText(v: JsValue): String = Json.stringify(v)
}