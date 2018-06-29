package org.constellation

import java.io.File
import java.security.KeyPair

import akka.http.scaladsl.server.Directives.complete
import akka.http.scaladsl.server.StandardRoute
import com.typesafe.scalalogging.Logger
import org.constellation.primitives.Schema._
import org.constellation.util.ProductHash

import scala.collection.concurrent.TrieMap
import scala.util.Try
import constellation._
import org.constellation.consensus.Consensus.{CC, RoundHash}
import org.constellation.primitives._

class Data extends MetricsExt
  with BundleDataExt
  with Reputation
  with PeerInfo
  with NodeData
  with Ledger
  with TransactionExt {

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


  def handleBundle(bundle: Bundle): Unit = {

    totalNumBundleMessages += 1

    val parentHash = bundle.extractParentBundleHash.pbHash
    val bmd = db.getAs[BundleMetaData](bundle.hash)
    val notPresent = bmd.isEmpty
    val zero = if (notPresent) {
      val rxTime = System.currentTimeMillis()
      BundleMetaData(bundle, rxTime = rxTime)
    } else bmd.get

    if (zero.isResolved) {
      updateMaxBundle(zero)
    } else {
      attemptResolveBundle(zero, parentHash)
    }
  }


}
