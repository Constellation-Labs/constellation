package org.constellation

import akka.stream.ActorMaterializer
import better.files.File
import com.typesafe.scalalogging.Logger

import org.constellation.primitives._

/** Data access object class. */
class DAO extends NodeData
  with Reputation
  with PeerInfoUDP
  with Genesis
  with EdgeDAO {

  // Set up a logger instance.
  val logger = Logger(s"Data")

  var actorMaterializer: ActorMaterializer = _

  var confirmWindow: Int = 30

  var transactionAcceptedAfterDownload: Long = 0L

  var downloadFinishedTime: Long = System.currentTimeMillis()

  var preventLocalhostAsPeer: Boolean = true

  /** @return Id File. */
  def idDir = File(s"tmp/${id.medium}")

  /** @return Database path. */
  def dbPath: File = {
    val f = File(s"tmp/${id.medium}/db")
    f.createDirectoryIfNotExists()
    f
  }

  /** @return Snapshot path. */
  def snapshotPath: File = {
    val f = File(s"tmp/${id.medium}/snapshots")
    f.createDirectoryIfNotExists()
    f
  }

  /** @return Snapshot hashes. */
  def snapshotHashes: Seq[String] = {
    snapshotPath.list.toSeq.map {
      _.name
    }
  }

  /** @return Peers file. */
  def peersInfoPath: File = {
    val f = File(s"tmp/${id.medium}/peers")
    f
  }

  /** @return Seed file. */
  def seedsPath: File = {
    val f = File(s"tmp/${id.medium}/seeds")
    f
  }

  /** Reset */
  def restartNode(): Unit = {
    downloadMode = true
    signedPeerLookup.clear()
    peersAwaitingAuthenticationToNumAttempts.clear()
    signedPeerLookup.clear()
    deadPeers = Seq()
  }

} // end class DAO
