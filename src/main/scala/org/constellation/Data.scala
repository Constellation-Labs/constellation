package org.constellation

import akka.stream.ActorMaterializer
import com.typesafe.scalalogging.Logger
import org.constellation.primitives.Schema.TransactionV1
import org.constellation.primitives._

class Data extends MetricsExt
  with BundleDataExt
  with Reputation
  with PeerInfo
  with NodeData
  with Ledger
  with TransactionExt
  with Genesis
  with EdgeDAO {

  val logger = Logger(s"Data")

  var actorMaterializer: ActorMaterializer = _

  var confirmWindow : Int = 30

  def restartNode(): Unit = {
 //   restartDB()
    genesisBundle = None
    downloadMode = true
    validLedger.clear()
    memPoolLedger.clear()
    syncPendingTXHashes = Set()
    syncPendingBundleHashes = Set()
    signedPeerLookup.clear()
    memPool = Set()
    activeDAGManager.activeSheafs = Seq()
    activeDAGManager.cellKeyToCell.clear()
    maxBundleMetaData = None
    txHashToTX.clear()
    bundleToSheaf.clear()
    last10000ValidTXHash = Seq()
    last100ValidBundleMetaData = Seq()
    resetMetrics()
    peersAwaitingAuthenticationToNumAttempts.clear()
    signedPeerLookup.clear()
    txSyncRequestTime.clear()
    txInMaxBundleNotInValidation = Set()
    last100SelfSentTransactions = Seq()
    peerSync.clear()
    deadPeers = Seq()
  }

  def handleTransaction(tx: TransactionV1): Unit = {
    if (lookupTransaction(tx.hash).isEmpty) {
      storeTransaction(tx)
      numSyncedTX += 1
    }
    syncPendingTXHashes -= tx.hash
    txSyncRequestTime.remove(tx.hash)
  }


}
