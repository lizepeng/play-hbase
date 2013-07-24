package com.github.lizepeng.play.hbase

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client._
import play.api._
import org.apache.hadoop.fs._


/**
 * Provides API for HBase.
 */
object HB {
  private def error = throw new Exception("HBase plugin is not registered.")

  /**
   * Execute a block of code, providing a HTable instance. The HTable will be
   * closed after executing the code.
   *
   * @param name The table name
   * @param block Code block to execute
   */
  def withHTable[A](name: String)(block: HTableInterface => A)(implicit app: Application): A = {
    app.plugin[HBPlugin].map(_.api.withHTable(name)(block)).getOrElse(error)
  }

  /**
   * Retrieves the configuration of HBasePlugin under current Application
   */
  def config(implicit app: Application) = {
    app.plugin[HBPlugin].map(_.api.config).getOrElse(error)
  }
}

/**
 * Provides API for Hadoop Distributed File System
 */
object HDFS {
  private def error = throw new Exception("HBase plugin is not registered.")

  /**
   * Execute a block of code, providing a HDFS file output stream. The stream will be
   * closed after executing the code.
   * @param path The uri of hdfs file.
   * @param block Code block to execute.
   */
  def withOutputStream[A](path: String)(block: FSDataOutputStream => A)(implicit app: Application): A = {
    app.plugin[HBPlugin].map(_.hdfs.withOutputStream(path)(block)).getOrElse(error)
  }

  /**
   * Create an OutputStream at the indicated Path
   * @param path The uri of hdfs file.
   */
  def create(path: String)(implicit app: Application): FSDataOutputStream = {
    app.plugin[HBPlugin].map(_.hdfs.create(path)).getOrElse(error)
  }
}

/**
 * The HBase API which Wrapped HTablePool
 */
trait HBApi {
  def pool: HTablePool

  /**
   * Execute a block of code, providing a HTable instance. The HTable will be
   * closed after executing the code.
   *
   * @param name The table name
   * @param block Code block to execute
   */
  def withHTable[A](name: String)(block: HTableInterface => A): A = {
    val table = pool.getTable(name)
    try {
      block(table)
    } finally {
      table.close()
    }
  }

  /**
   * Check if able to connect with hbase master
   */
  def checkAvailable()

  /**
   * Retrieves the setting of zookeeper quorum
   */
  def quorumURL: Option[String]

  /**
   * Retrieves the configuration of HBasePlugin
   */
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

/**
 * The HDFS API which provided basic function, such as read or write a file in hdfs.
 */
trait HDFSApi {

  /**
   * Execute a block of code, providing a HDFS file output stream. The stream will be
   * closed after executing the code.
   * @param path The uri of hdfs file.
   * @param block Code block to execute.
   */
  def withOutputStream[A](path: String)(block: FSDataOutputStream => A): A

  /**
   * Create an OutputStream at the indicated Path
   * @param path The uri of hdfs file.
   */
  def create(path: String): FSDataOutputStream
}

private[hbase] class HDFileSystemApi(conf: Configuration) extends HDFSApi {

  import org.apache.hadoop.conf.{Configuration => HadoopConf}
  import java.net._

  lazy val urlOp: Option[String] = conf.getString("fs.default.name")

  lazy val hdConfig = new HadoopConf()

  def create(path: String): FSDataOutputStream = {
    val user = conf.getString("fs.user").getOrElse("")
    val url = new URI(urlOp.getOrElse(s"file://${System.getProperty("java.io.tmpdir")}"))
    val fs = FileSystem.get(url, hdConfig, user)
    fs.create(new Path(s"$url$path"), true)
  }

  def withOutputStream[A](path: String)(block: FSDataOutputStream => A): A = {
    val ops = create(path)
    try {block(ops)} finally {ops.close()}
  }
}

/**
 * Generic HBPlugin interface
 */
trait HBPlugin extends Plugin {
  def api: HBApi

  def hdfs: HDFSApi
}

/**
 * A HBPlugin implementation that provides a HBApi
 *
 * @param app the application that is registering the plugin
 */
class HBasePlugin(app: Application) extends HBPlugin {

  lazy val hbConfig = app.configuration.getConfig("hb").getOrElse(Configuration.empty)

  lazy val hdfsConfig = app.configuration.getConfig("hdfs").getOrElse(Configuration.empty)

  private lazy val isDisabled = {
    app.configuration.getString("hb-plugin").filter(_ == "disabled").isDefined || hbConfig.subKeys.isEmpty
  }

  private lazy val hbApi: HBApi = new HBaseApi(hbConfig)

  private lazy val hdfsApi: HDFSApi = new HDFileSystemApi(hdfsConfig)

  def api: HBApi = hbApi

  def hdfs: HDFSApi = hdfsApi

  override def enabled = !isDisabled

  override def onStart() {
    hbApi.checkAvailable()
    Logger("play").info(s"HBase connected at ${hbApi.quorumURL.getOrElse("")}")
  }

  override def onStop() {
    Logger("play").info("HBase disconnected")
  }
}