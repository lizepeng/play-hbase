package com.github.lizepeng.play.hbase

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client._
import play.api._

/**
 * @author zepeng.li@gmail.com
 */
object HB {
  private def error = throw new Exception("HB plugin is not registered.")

  def withTable[A](name: String)(block: HTableInterface => A)(implicit app: Application): A = {
    app.plugin[HBPlugin].map(_.api.withTable(name)(block)).getOrElse(error)
  }

  def config(implicit app: Application) = {
    app.plugin[HBPlugin].map(_.api.config).getOrElse(error)
  }
}

trait HBApi {
  def pool: HTablePool

  def withTable[A](name: String)(block: HTableInterface => A): A = {
    val table = pool.getTable(name)
    try {
      block(table)
    } finally {
      table.close()
    }
  }

  def checkAvailable()

  def quorumURL: Option[String]

  def config: Configuration
}

private[hbase] class HBaseApi(conf: Configuration) extends HBApi {
  private val quorum = "hbase.zookeeper.quorum"

  lazy val pool: HTablePool = buildTablePool

  lazy val quorumURL: Option[String] = conf.getString(quorum)

  lazy val hbaseConf = {
    val hbConf = HBaseConfiguration.create()
    quorumURL.map(hbConf.set(quorum, _))
    hbConf
  }

  def config = conf

  private def buildTablePool = {
    checkAvailable()
    new HTablePool(hbaseConf, conf.getInt("htable.pool.size").getOrElse(256))
  }

  def checkAvailable() {
    HBaseAdmin.checkHBaseAvailable(hbaseConf)
  }
}

trait HBPlugin extends Plugin {
  def api: HBApi
}

class HBasePlugin(app: Application) extends HBPlugin {

  lazy val hbConfig = app.configuration.getConfig("hb").getOrElse(Configuration.empty)

  private lazy val isDisabled = {
    app.configuration.getString("hb-plugin").filter(_ == "disabled").isDefined || hbConfig.subKeys.isEmpty
  }

  private lazy val hbApi: HBApi = new HBaseApi(hbConfig)

  def api: HBApi = hbApi

  override def enabled = !isDisabled

  override def onStart() {
    hbApi.checkAvailable()
    Logger("play").info(s"HBase connected at ${hbApi.quorumURL.getOrElse("")}")
  }

  override def onStop() {
    Logger("play").info("HBase disconnected")
  }
}