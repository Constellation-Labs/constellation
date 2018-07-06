package org.constellation

import com.typesafe.scalalogging.Logger
import org.constellation.primitives._

class Data extends MetricsExt
  with BundleDataExt
  with Reputation
  with PeerInfo
  with NodeData
  with Ledger
  with TransactionExt
  with Genesis {

  val logger = Logger(s"Data")

  def restartNode(): Unit = {
    restartDB()
    genesisBundle = null
    downloadMode = true
    validLedger.clear()
    memPoolLedger.clear()
    syncPendingTXHashes = Set()
    syncPendingBundleHashes = Set()
    peerLookup.clear()
    memPool = Set()
  }
}
