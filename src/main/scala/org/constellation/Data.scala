package org.constellation

import akka.stream.ActorMaterializer
import com.typesafe.scalalogging.Logger
import org.constellation.primitives._

class Data extends MetricsExt
  with NodeData
  with BundleDataExt
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
    genesisBundle = None
    downloadMode = true
    validLedger.clear()
    memPoolLedger.clear()
    syncPendingTXHashes = Set()
    syncPendingBundleHashes = Set()
    signedPeerLookup.clear()
    activeDAGManager.activeSheafs = Seq()
    activeDAGManager.cellKeyToCell.clear()
    maxBundleMetaData = None
    bundleToSheaf.clear()
    last100ValidBundleMetaData = Seq()
    resetMetrics()
    peersAwaitingAuthenticationToNumAttempts.clear()
    signedPeerLookup.clear()
    txInMaxBundleNotInValidation = Set()
    peerSync.clear()
    deadPeers = Seq()
  }

}
