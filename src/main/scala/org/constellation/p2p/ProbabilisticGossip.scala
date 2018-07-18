package org.constellation.p2p

import java.net.InetSocketAddress

import org.constellation.Data
import org.constellation.primitives.Schema._
import org.constellation.util.ProductHash
import constellation._
import org.constellation.LevelDB.{DBDelete, DBPut}

import scala.util.{Failure, Random, Success, Try}

trait ProbabilisticGossip extends PeerAuth with LinearGossip {

  val data: Data

  import data._

  def handleGossip(gm : GossipMessage, remote: InetSocketAddress): Unit = {

    totalNumGossipMessages += 1

    gm match {
      case BatchBundleHashRequest(hashes) =>

        hashes.foreach{ h =>
          lookupBundleDBFallbackBlocking(h).foreach{
            b =>
              // TODO: Send all meta for sync conflict detection.
              udpActor ! UDPSend(b.bundle, remote)
          }
        }

      case BatchTXHashRequest(hashes) =>

        hashes.foreach{h =>
          lookupTransactionDBFallbackBlocking(h).foreach{
            b =>
              udpActor ! UDPSend(b, remote)
          }
        }

      case b: Bundle =>

        handleBundle(b)

      case tx: Transaction =>

        if (lookupTransactionDBFallbackBlocking(tx.hash).isEmpty) {
          storeTransaction(tx)
          numSyncedTX += 1
        }

        syncPendingTXHashes -= tx.hash
        txSyncRequestTime.remove(tx.hash)

      case bb: PeerSyncHeartbeat =>
        processPeerSyncHeartbeat(bb)

      case x =>
        logger.debug("Unrecognized gossip message " + x)
    }
  }

  @volatile var heartBeatInProgress = false

  def gossipHeartbeat(): Unit = {
    // Gather any missing transaction or bundle data
    dataRequest()

    if (!downloadMode && genesisBundle.nonEmpty && maxBundleMetaData.nonEmpty && !downloadInProgress) {
      if (!heartBeatInProgress) {

        heartBeatInProgress = true

        Try {
          bundleHeartbeat()
        } match {
          case Success(x) =>
          case Failure(e) => e.printStackTrace()
        }

        heartBeatInProgress = false
      }
    }
  }

  def bundleHeartbeat(): Unit = {
    // Bootstrap the initial genesis tx
    acceptInitialDistribution()

    // Lookup from db or ask peers for peer bundle data that is missing
    attemptResolvePeerBundles()

    // TODO: remove, should only live within the test context
    if (generateRandomTX) {
      simulateTransactions()
    }

    // Tell peers about our latest best bundle and chain
    broadcast(PeerSyncHeartbeat(maxBundleMetaData.get, validLedger.toMap, id))

    // Tell peers about our latest mempool state
    poolEmit()

    // Compress down bundles
    combineBundles()

    // clean up any bundles that have been compressed
    cleanupStrayChains()
  }

  def dataRequest(): Unit = {

    // Request missing bundle data
    if (syncPendingBundleHashes.nonEmpty) {
      broadcast(BatchBundleHashRequest(syncPendingBundleHashes))
    }

    // Request missing transaction data
    if (syncPendingTXHashes.nonEmpty) {

      broadcast(BatchTXHashRequest(syncPendingTXHashes))

      if (syncPendingTXHashes.size > 300) {
        val toRemove = txSyncRequestTime.toSeq.sortBy(_._2).zipWithIndex.filter{_._2 > 50}.map{_._1._1}.toSet
        syncPendingTXHashes --= toRemove
      }

      // ask peers for this transaction data
      broadcast(BatchTXHashRequest(syncPendingTXHashes))
    }
  }

  def getParentHashEmitter(stackDepthFilter: Int = minGenesisDistrSize - 1): ParentBundleHash = {
    val mb = maxBundle.get
    def pbHash = ParentBundleHash(mb.hash)
    val pbh = if (totalNumValidatedTX == 1) pbHash
    else if (mb.maxStackDepth < stackDepthFilter) mb.extractParentBundleHash
    else pbHash
    pbh
  }

  def poolEmit(): Unit = {
    val mb = maxBundle
    val pbh = getParentHashEmitter()

    val maybeData = lookupBundle(pbh.pbHash)
    val ids = maybeData.get.bundle.extractIds
    val lastPBWasSelf = maybeData.exists(_.bundle.bundleData.id == id)
    val selfIsFacilitator = (BigInt(pbh.pbHash, 16) % ids.size).toInt == 0
    val doEmit = !lastPBWasSelf && selfIsFacilitator

    if (!lastPBWasSelf || totalNumValidatedTX == 1) {

      // Emit an origin bundle. This needs to be managed by prob facil check on hash of previous + ids
      val memPoolEmit = Random.nextInt() < 0.3
      val filteredPool = memPool.diff(txInMaxBundleNotInValidation).filterNot(last10000ValidTXHash.contains)

      val memPoolSelSize = Random.nextInt(5)

      val memPoolSelection = Random.shuffle(filteredPool.toSeq)
        .slice(0, memPoolSelSize + minGenesisDistrSize + 1)

      if (memPoolEmit && filteredPool.nonEmpty) {

        val b = Bundle(
          BundleData(
            memPoolSelection.map {
              TransactionHash
            } :+ pbh
          ).signed()
        )

        val meta = mb.get.meta.get

        updateBundleFrom(meta, Sheaf(b))

        numMempoolEmits += 1

        broadcast(b)
      }
    }
  }

  def acceptInitialDistribution(): Unit = {
    // Force accept initial distribution
    if (totalNumValidatedTX == 1 && maxBundle.get.extractTX.size >= (minGenesisDistrSize - 1)) {

      maxBundle.get.extractTX.foreach{tx =>
        acceptTransaction(tx)
        tx.txData.data.updateLedger(memPoolLedger)
      }

      totalNumValidBundles += 1
      last100ValidBundleMetaData :+= maxBundleMetaData.get
    }
  }

  def attemptResolvePeerBundles(): Unit = peerSync.foreach{
    case (id, hb) =>
      if (hb.maxBundle.meta.exists(z => !z.isResolved)) {
        attemptResolveBundle(hb.maxBundle.meta.get, hb.maxBundle.extractParentBundleHash.pbHash)
      }
  }

  def combineBundles(): Unit = {

    // Maybe only emit when PBH matches our current?
    // Need to slice this down here
    // Only emit max by new total score?
    val groupedBundles = activeDAGBundles.groupBy(b => b.bundle.extractParentBundleHash -> b.bundle.maxStackDepth)

    var toRemove = Set[Sheaf]()

    groupedBundles.filter{_._2.size > 1}.toSeq //.sortBy(z => 1*z._1._2).headOption
      .foreach { case (pbHash, bundles) =>

      if (Random.nextDouble() > 0.6) {
        val best3 = bundles.sortBy(z => -1 * z.totalScore.get).slice(0, 2)
        val allIds = best3.flatMap(_.bundle.extractIds)

        //   if (!allIds.contains(id)) {
        val b = Bundle(BundleData(best3.map {
          _.bundle
        }).signed())

        val maybeData = lookupBundle(pbHash._1.pbHash)

        val pbData = maybeData.get

        updateBundleFrom(pbData, Sheaf(b))

        // Skip ids when depth below a certain amount, else tell everyone.
        // TODO : Fix ^
        broadcast(b, skipIDs = allIds)
      }

      if (bundles.size > 30) {
        val toRemoveHere = bundles.sortBy(z => z.totalScore.get)
          .zipWithIndex.filter{_._2 < 15}.map{_._1}.toSet

        toRemove ++= toRemoveHere
      }
    }

    if (toRemove.nonEmpty) {
      activeDAGManager.activeSheafs = activeDAGManager.activeSheafs.filterNot(toRemove.contains)
    }

  }

  def bundleCleanup(): Unit = {
    if (heartbeatRound % 20 == 0 && maxBundleMetaData.exists {
      _.height.exists {
        _ > 20
      }
    }) {

      val currentHeight = maxBundleMetaData.get.height.get

      var numDeleted = 0

      bundleToSheaf.foreach { case (h, s) =>

        val heightOld = s.height.exists(h => h < (currentHeight - 15))

        val partOfValidation = last100ValidBundleMetaData.contains(s)

        val isOld = s.rxTime < (System.currentTimeMillis() - 120 * 1000)

        val unresolved = s.height.isEmpty && isOld

        def removeTX(): Unit = s.bundle.extractTXHash.foreach { txH =>
          removeTransactionFromMemory(txH.txHash)
        }

        if (unresolved) {
          numDeleted += 1
          deleteBundle(h, dbDelete = false)
          removeTX()
        }

        if (heightOld) {

          if (partOfValidation) {
            deleteBundle(h, dbDelete = false)
          } else {
            deleteBundle(h, dbDelete = false)
          }

          removeTX()

          numDeleted += 1

        }
      }
    }
  }

  def cleanupStrayChains(): Unit = {
    activeDAGManager.cleanup(maxBundleMetaData.get.height.get)
    bundleCleanup()
  }

  // TODO: extract to test
  def simulateTransactions(): Unit = {

    val shouldEmit = maxBundleMetaData.exists {_.height.get >= 5}

    if (shouldEmit && memPool.size < 150) {
      randomTransaction()
      randomTransaction()
    }
  }

}
