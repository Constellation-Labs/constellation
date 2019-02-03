package org.constellation

import akka.stream.ActorMaterializer
import better.files.File
import com.typesafe.scalalogging.Logger

import org.constellation.primitives._

/** Documentation. */
class DAO(val nodeConfig: NodeConfig = NodeConfig()) extends NodeData
  with Genesis
  with EdgeDAO {

  val logger = Logger(s"Data")

  var actorMaterializer: ActorMaterializer = _

  var confirmWindow : Int = 30

  var transactionAcceptedAfterDownload: Long = 0L
  var downloadFinishedTime: Long = System.currentTimeMillis()

  var preventLocalhostAsPeer: Boolean = true

  /** Documentation. */
  def idDir = File(s"tmp/${id.medium}")

  /** Documentation. */
  def dbPath: File = {
    val f = File(s"tmp/${id.medium}/db")
    f.createDirectoryIfNotExists()
    f
  }

  /** Documentation. */
  def snapshotPath: File = {
    val f = File(s"tmp/${id.medium}/snapshots")
    f.createDirectoryIfNotExists()
    f
  }

  /** Documentation. */
  def snapshotHashes: Seq[String] = {
    snapshotPath.list.toSeq.map{_.name}
  }

  /** Documentation. */
  def peersInfoPath: File = {
    val f = File(s"tmp/${id.medium}/peers")
    f
  }

  /** Documentation. */
  def seedsPath: File = {
    val f = File(s"tmp/${id.medium}/seeds")
    f
  }

  /** Documentation. */
  def restartNode(): Unit = {
    downloadMode = true
  }

}

