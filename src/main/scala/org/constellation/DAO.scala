package org.constellation

import akka.stream.ActorMaterializer
import better.files.File
import com.typesafe.scalalogging.StrictLogging
import org.constellation.primitives._

class DAO extends NodeData
  with Reputation
  with PeerInfoUDP
  with Genesis
  with EdgeDAO
  with StrictLogging {
  
  var actorMaterializer: ActorMaterializer = _

  var confirmWindow : Int = 30

  var transactionAcceptedAfterDownload: Long = 0L
  var downloadFinishedTime: Long = System.currentTimeMillis()

  var preventLocalhostAsPeer: Boolean = true

  def idDir = File(s"tmp/${id.medium}")

  def dbPath: File = {
    val f = File(s"tmp/${id.medium}/db")
    f.createDirectoryIfNotExists()
    f
  }

  def snapshotPath: File = {
    val f = File(s"tmp/${id.medium}/snapshots")
    f.createDirectoryIfNotExists()
    f
  }

  def snapshotHashes: Seq[String] = {
    snapshotPath.list.toSeq.map{_.name}
  }

  def peersInfoPath: File = {
    val f = File(s"tmp/${id.medium}/peers")
    f
  }

  def seedsPath: File = {
    val f = File(s"tmp/${id.medium}/seeds")
    f
  }


  def restartNode(): Unit = {
    downloadMode = true
    signedPeerLookup.clear()
    peersAwaitingAuthenticationToNumAttempts.clear()
    signedPeerLookup.clear()
    deadPeers = Seq()
  }

}
