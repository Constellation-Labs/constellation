package org.constellation

import akka.stream.ActorMaterializer
import better.files.File
import com.typesafe.scalalogging.Logger
import org.constellation.primitives._

class DAO extends MetricsExt
  with NodeData
  with Reputation
  with PeerInfoUDP
  with Genesis
  with EdgeDAO {

  val logger = Logger(s"Data")

  var actorMaterializer: ActorMaterializer = _

  var confirmWindow : Int = 30

  var preventLocalhostAsPeer: Boolean = true

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

  def peersInfoPath: File = {
    val f = File(s"tmp/${id.medium}/peers")
    f
  }


  def restartNode(): Unit = {
    downloadMode = true
    signedPeerLookup.clear()
    resetMetrics()
    peersAwaitingAuthenticationToNumAttempts.clear()
    signedPeerLookup.clear()
    deadPeers = Seq()
  }

}
