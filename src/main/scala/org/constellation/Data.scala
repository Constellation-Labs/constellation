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
  with Ledger {

  @volatile var db: LevelDB = _

  val logger = Logger(s"Data")

  def tmpDirId = new File("tmp", id.medium)

  def restartDB(): Unit = {
    Try {
      db.destroy()
    }
    db = new LevelDB(new File(tmpDirId, "db"))
  }

  def updateKeyPair(kp: KeyPair): Unit = {
    keyPair = kp
    restartDB()
  }

  def restartNode(): Unit = {
    restartDB()
    genesisBundle = null
    downloadMode = true
    validLedger.clear()
    memPoolLedger.clear()
    last1000ValidTX = Seq()
    syncPendingTXHashes = Set()
    syncPendingBundleHashes = Set()
    bestBundle = null
    peerLookup.clear()
    memPool = Set()
  }


  var minGenesisDistrSize: Int = 3

  @volatile var heartbeatRound = 0L
  @volatile var downloadMode: Boolean = true
  @volatile var downloadInProgress: Boolean = false


  def acceptGenesis(b: Bundle): Unit = {
    genesisBundle = b
    bestBundle = genesisBundle
    db.put(genesisBundle)
    processNewBundleMetadata(genesisBundle, genesisBundle.extractTX, isGenesis = true, setActive = false)
    //validBundles = Seq(genesisBundle)
    last100BundleHashes = Seq(genesisBundle.hash)
    totalNumValidBundles += 1
    val gtx = b.extractTX.head
    gtx.txData.data.updateLedger(memPoolLedger)
    acceptTransaction(gtx)
  }

  def createGenesis(tx: TX): Unit = {
    db.put(tx)
    acceptGenesis(Bundle(BundleData(Seq(ParentBundleHash("coinbase"), TransactionHash(tx.hash))).signed()))
    downloadMode = false
  }

  def findAncestors(
                     parentHash: String,
                     ancestors: Seq[BundleMetaData] = Seq()
                   ): Seq[BundleMetaData] = {
    val parent = db.getAs[BundleMetaData](parentHash)
    if (parent.isEmpty) {
      syncPendingBundleHashes += parentHash
      ancestors
    } else if (parentHash == genesisBundle.hash) {
      Seq(db.getAs[BundleMetaData](genesisBundle.hash).get) ++ ancestors
    } else {
      findAncestors(
        parent.get.bundle.extractParentBundleHash.pbHash,
        Seq(parent.get) ++ ancestors
      )
    }
  }

  def findAncestorsUpToLastResolved(
                                     parentHash: String,
                                     ancestors: Seq[BundleMetaData] = Seq()
                                   ): Seq[BundleMetaData] = {
    val parent = db.getAs[BundleMetaData](parentHash)
    val updatedAncestors = Seq(parent.get) ++ ancestors
    if (parent.isEmpty) {
      syncPendingBundleHashes += parentHash
      ancestors
    } else if (parent.get.isResolved) updatedAncestors
    else {
      findAncestors(
        parent.get.bundle.extractParentBundleHash.pbHash,
        updatedAncestors
      )
    }
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

      zero.bundle.bundleScore

    }

      val ancestors = findAncestorsUpToLastResolved(parentHash)

      if (ancestors.nonEmpty) {

        val chain = ancestors.tail ++ Seq(zero)

        // TODO: Change to recursion to implement abort-early fold
        chain.fold(ancestors.head) {
          case (left, right) =>

            if (!left.isResolved) right
            else if (right.isResolved) right
            else {

              val ids = right.bundle.extractIds
              val newReps = left.reputations.map { case (id, rep) =>
                id -> {
                  if (ids.contains(id)) rep + 1 else rep
                }
              }

              val updatedRight = right.copy(height = Some(left.height.get + 1), reputations = newReps)
              db.put(right.bundle.hash, updatedRight)
              updatedRight
            }
        }


      } else {
        db.put(bundle.hash, zero)
      }
  }


  def jaccard[T](t1: Set[T], t2: Set[T]): Double = {
    t1.intersect(t2).size.toDouble / t1.union(t2).size.toDouble
  }

  def prettifyBundle(b: Bundle): String = b.pretty

  def createTransaction(dst: String, amount: Long, normalized: Boolean = true, src: String = selfAddress.address): TX = {
    val tx = createTransactionSafe(src, dst, amount, keyPair, normalized)
    db.put(tx)
    if (last100SelfSentTransactions.size > 100) {
      last100SelfSentTransactions.tail :+ tx
    }
    last100SelfSentTransactions :+= tx
    updateMempool(tx)
    tx
  }

  def handleSendRequest(s: SendToAddress): StandardRoute = {
    val tx = createTransaction(s.dst, s.amount, s.normalized)
    complete(tx.prettyJson)
  }

  def acceptTransaction(tx: TX, updatePending: Boolean = true): Unit = {

    if (!last1000ValidTX.contains(tx.hash)) {
      if (last1000ValidTX.size >= 10000) {
        last1000ValidTX = last1000ValidTX.tail :+ tx.hash
      } else {
        last1000ValidTX = last1000ValidTX :+ tx.hash
      }
      totalNumValidatedTX += 1

      tx.txData.data.updateLedger(validLedger)
      memPool -= tx.hash
    }
  }


  @volatile var lastCheckpointBundle: Option[Bundle] = None

  def updateMempool(tx: TX): Boolean = {
    val hash = tx.hash
    val txData = tx.txData.data
    val validUpdate = !memPool.contains(hash) && tx.valid && txData.ledgerValid(memPoolLedger) &&
      !last1000ValidTX.contains(hash)
    // logger.debug(s"Update mempool $validUpdate ${tx.valid} ${txData.ledgerValid(memPoolLedger)} ${!last1000ValidTX.contains(hash)}")
    if (validUpdate) {
      txData.updateLedger(memPoolLedger)
      memPool += hash
    }
    validUpdate
  }


}
