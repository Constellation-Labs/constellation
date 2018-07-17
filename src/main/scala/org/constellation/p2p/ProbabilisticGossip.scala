package org.constellation.p2p

import java.net.InetSocketAddress

import org.constellation.Data
import org.constellation.primitives.Schema._
import org.constellation.util.ProductHash
import constellation._
import org.constellation.LevelDB.{DBDelete, DBPut}

import scala.util.Random

trait ProbabilisticGossip extends PeerAuth with LinearGossip {

  val data: Data

  import data._

  def handleGossip(gm : GossipMessage, remote: InetSocketAddress): Unit = {
    totalNumGossipMessages += 1
    val rid = signedPeerLookup.get(remote).map{_.data.id}
    gm match {
      case BatchBundleHashRequest(hashes) =>
        hashes.foreach{h =>
          /// println("Sending BundleMetaData to peer on request")
          lookupBundleDBFallbackBlocking(h).foreach{
            b =>
              // TODO: Send all meta for sync conflict detection.
              udpActor ! UDPSend(b.bundle, remote)
          }
        }
      case BatchTXHashRequest(hashes) =>
        hashes.foreach{h =>
          //  println(s"Sending tx hash response $h")
          lookupTransactionDBFallbackBlocking(h).foreach{
            b =>
              udpActor ! UDPSend(b, remote)
          }
        }
      case b: Bundle =>

        handleBundle(b)

      case tx: Transaction =>
        //  println(s"Rx tx hash ${tx.short}")
        if (lookupTransaction(tx.hash).isEmpty) {
          storeTransaction(tx)
          numSyncedTX += 1
        }
        syncPendingTXHashes -= tx.hash
        txSyncRequestTime.remove(tx.hash)

      case bb: PeerSyncHeartbeat =>

        /*        handleBundle(bb.maxBundle)
                rid.foreach{ r =>
                  peerSync(r) = bb
                }
                */
        processPeerSyncHeartbeat(bb)


      case g : Gossip[_] =>
      // handleGossipRegular(g, remote)
      case x =>
        logger.debug("Unrecognized gossip message " + x)
    }
  }

  def gossipHeartbeat(): Unit = {
    dataRequest()
    if (!downloadMode && genesisBundle.nonEmpty && maxBundleMetaData.nonEmpty && !downloadInProgress) {
      bundleHeartbeat()
    }
  }


  def bundleHeartbeat(): Unit = {

    acceptInitialDistribution()

    attemptResolvePeerBundles()

    peerSync.foreach{
      _._2.maxBundleMeta.height.foreach{
        h =>
          maxBundleMetaData.flatMap{_.height}.foreach{ h2 =>
            if (h > (h2 + 20)) {
              // logger.error("FOUND PEER 20 BUNDLES AHEAD, RESTARTING")
              // System.exit(1)
            }
          }
      }
    }

    if (generateRandomTX) simulateTransactions()

    broadcast(PeerSyncHeartbeat(maxBundleMetaData.get, validLedger.toMap, id))

    poolEmit()

    combineBundles()

    cleanupStrayChains()

  }



  def simulateTransactions(): Unit = {
    if (maxBundleMetaData.exists{_.height.get >= 5} && memPool.size < 1500) {
      //if (Random.nextDouble() < .2)
      randomTransaction()
      randomTransaction()
    }
  }

  def dataRequest(): Unit = {
    if (syncPendingBundleHashes.nonEmpty) {
      //    println("Requesting bundle sync of " + syncPendingBundleHashes.map{_.slice(0, 5)})
      broadcast(BatchBundleHashRequest(syncPendingBundleHashes))
    }
    if (syncPendingTXHashes.nonEmpty) {
      // println("Requesting data sync pending of " + syncPendingTXHashes)
      broadcast(BatchTXHashRequest(syncPendingTXHashes))
      if (syncPendingTXHashes.size > 1500) {
        val toRemove = txSyncRequestTime.toSeq.sortBy(_._2).zipWithIndex.filter{_._2 > 50}.map{_._1._1}.toSet
        syncPendingTXHashes --= toRemove
      }
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
    //val selfIsFacilitator = (BigInt(pbh.pbHash + stackSizeFilter, 16) % ids.size).toInt == 0
    val doEmit = !lastPBWasSelf && selfIsFacilitator

    if (!lastPBWasSelf || totalNumValidatedTX == 1) {

      // Emit an origin bundle. This needs to be managed by prob facil check on hash of previous + ids
      val memPoolEmit = Random.nextInt() < 0.3
      val filteredPool = memPool.diff(txInMaxBundleNotInValidation).filterNot(last10000ValidTXHash.contains)
      val memPoolSelSize = Random.nextInt(5)
      val memPoolSelection = Random.shuffle(filteredPool.toSeq)
        .slice(0, memPoolSelSize + minGenesisDistrSize + 1)


      if (memPoolEmit && filteredPool.nonEmpty) {
        // println(s"Mempool emit on ${id.short}")

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
        //     if (maybeData.isEmpty) println(pbHash)
        val pbData = maybeData.get
        updateBundleFrom(pbData, Sheaf(b))
        // Skip ids when depth below a certain amount, else tell everyone.
        // TODO : Fix ^
        broadcast(b, skipIDs = allIds)
      }
      if (bundles.size > 30) {
        val toRemoveHere = bundles.sortBy(z => z.totalScore.get).zipWithIndex.filter{_._2 < 15}.map{_._1}.toSet
        toRemove ++= toRemoveHere
      }

      /*
                val hasNewTransactions =
                  l.bundle.extractTXHash.diff(r.bundle.extractTXHash).nonEmpty
                val minTimeClose = Math.abs(l.rxTime - r.rxTime) < 40000
                if (Random.nextDouble() > 0.5 &&  hasNewTransactions && minTimeClose) { //
                }
            }*/
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

      println("Bundle cleanup")

/*
      val oldestAncestor = last100ValidBundleMetaData.head

      val ancestorsMinus100 = findAncestorsUpTo(oldestAncestor.bundle.hash, upTo = 100)

      val lastValidHashes = (last100ValidBundleMetaData.map {
        _.bundle.hash
      }.toSet ++ ancestorsMinus100.map {
        _.bundle.hash
      }).toSet
*/

      /*
      ancestorsMinus100.slice(0, 50).foreach{
        s =>
          s.bundle.extractSubBundleHashes.foreach{
            h =>
              if (!lastValidHashes.contains(h)) deleteBundle(h)
          }
          if (bundleToSheaf.contains(s.bundle.hash)) {
            bundleToSheaf.remove(s.bundle.hash)
            numValidBundleHashesRemovedFromMemory += 1
            //dbActor.foreach{ db =>
            // db ! DBPut(s.bundle.hash, s) ? Maybe necessary? to ensure it was stored the first time?
            // }
          }

          s.bundle.extractTXHash.foreach{ t =>
            removeTransactionFromMemory(t.txHash)
          }
      }
*/
      val currentHeight = maxBundleMetaData.get.height.get // last100ValidBundleMetaData.head.height.get

      /*
      val lastHeight = last100ValidBundleMetaData.last.height.get - 5
      val firstHeight = oldestAncestor.height.get + 5
*/

      var numDeleted = 0

      bundleToSheaf.foreach { case (h, s) =>

        val heightOld = s.height.exists(h => h < (currentHeight - 15))
        val partOfValidation = last100ValidBundleMetaData.contains(s) //|| ancestorsMinus100.contains(s)
        val isOld = s.rxTime < (System.currentTimeMillis() - 120 * 1000)
        val unresolved = s.height.isEmpty && isOld

        def removeTX(): Unit = s.bundle.extractTXHash.foreach { txH =>
          removeTransactionFromMemory(txH.txHash)
        //  if (!last10000ValidTXHash.contains(txH.txHash)) {
        //    dbActor.foreach {
        //      _ ! DBDelete(txH.txHash)
        //    }
        //  }
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


        /*
        if (unresolved || resolvedButOld) { //unresolved || resolvedButOld

          }
          deleteBundle(h, !lastValidHashes.contains(h))
          s.bundle.extractSubBundleHashes.foreach{
            h =>
              if (!lastValidHashes.contains(h)) deleteBundle(h)
          }
        }
      }*/
      }

      println("Bundle to sheaf cleanup done removed: " + numDeleted)

      // There should also be
    }
  }

  def cleanupStrayChains(): Unit = {
    activeDAGManager.cleanup(maxBundleMetaData.get.height.get)
    bundleCleanup()
  }

}
