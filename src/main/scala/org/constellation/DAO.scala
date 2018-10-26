package org.constellation

import java.io.File

import akka.stream.ActorMaterializer
import com.typesafe.scalalogging.Logger
import org.constellation.primitives._

class DAO extends MetricsExt
  with NodeData
  with Reputation
  with PeerInfo
  with Genesis
  with EdgeDAO {

  val logger = Logger(s"Data")

  var actorMaterializer: ActorMaterializer = _

  var confirmWindow : Int = 30

  def dbPath: File = {
    val f = new File(s"tmp/${id.medium}/db")
    f.mkdirs()
    f
  }

  def snapshotPath: File = {
    val f = new File(s"tmp/${id.medium}/snapshots")
    f.mkdirs()
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
