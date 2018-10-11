package org.constellation

import akka.stream.ActorMaterializer
import com.typesafe.scalalogging.Logger
import org.constellation.primitives._

class Data extends MetricsExt
  with NodeData
  with Reputation
  with EdgeExt
  with PeerInfo
  with Ledger
  with Genesis
  with EdgeDAO {

  val logger = Logger(s"Data")

  var actorMaterializer: ActorMaterializer = _

  var confirmWindow : Int = 30

  def restartNode(): Unit = {
    downloadMode = true
    validLedger.clear()
    memPoolLedger.clear()
    signedPeerLookup.clear()
    resetMetrics()
    peersAwaitingAuthenticationToNumAttempts.clear()
    signedPeerLookup.clear()
    peerSync.clear()
    deadPeers = Seq()
  }

}
