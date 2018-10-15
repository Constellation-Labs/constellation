package org.constellation

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

  def restartNode(): Unit = {
    downloadMode = true
    syncPendingTXHashes = Set()
    syncPendingBundleHashes = Set()
    validLedger.clear()
    memPoolLedger.clear()
    signedPeerLookup.clear()
    maxBundleMetaData = None
    bundleToSheaf.clear()
    last100ValidBundleMetaData = Seq()
    resetMetrics()
    peersAwaitingAuthenticationToNumAttempts.clear()
    signedPeerLookup.clear()
    peerSync.clear()
    deadPeers = Seq()
  }

}
