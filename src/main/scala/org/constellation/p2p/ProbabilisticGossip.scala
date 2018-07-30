package org.constellation.p2p

import java.net.InetSocketAddress

import akka.http.scaladsl.model.StatusCodes
import org.constellation.Data
import org.constellation.primitives.Schema._
import org.constellation.util.{APIClient, ProductHash}
import constellation._
import org.constellation.LevelDB.{DBDelete, DBPut}

import scala.concurrent.Future
import scala.util.{Failure, Random, Success, Try}

trait ProbabilisticGossip extends PeerAuth {

  val data: Data

  import data._

  def handleGossip(gm : GossipMessage, remote: InetSocketAddress): Unit = {

    totalNumGossipMessages += 1

    //val rid = signedPeerLookup.get(remote).map{_.data.id}

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

        handleTransaction(tx)

      case bb: PeerSyncHeartbeat =>
        processPeerSyncHeartbeat(bb)

      case x =>
        logger.debug("Unrecognized gossip message " + x)
    }
  }

  //  @volatile var heartBeatInProgress = false


  def gossipHeartbeat(): Unit = {
    // Gather any missing transaction or bundle data
    dataRequest()

    if (!downloadMode && genesisBundle.nonEmpty && maxBundleMetaData.nonEmpty && !downloadInProgress) {
      //  if (!heartBeatInProgress) {
      //  heartBeatInProgress = true
      ///  Try {
      bundleHeartbeat()
      // } match {
      //   case Success(x) =>
      //   case Failure(e) => e.printStackTrace()
      // }
      // heartBeatInProgress = false
      // }
    }
  }

  def bundleHeartbeat(): Unit = {
    // Bootstrap the initial genesis tx
    acceptInitialDistribution()

    // Lookup from db or ask peers for peer bundle data that is missing
    attemptResolvePeerBundles()

    peerSync.foreach{
      _._2.maxBundleMeta.height.foreach{
        h =>
          maxBundleMetaData.flatMap{_.height}.foreach{ h2 =>
            if (h > (h2 + 20)) {
              downloadInProgress = true
              // logger.error("FOUND PEER 20 BUNDLES AHEAD, RESTARTING")
              // System.exit(1)
            }
          }
      }
    }


    //allPeersHealthy = apiBroadcast(_.getSync("health").status == StatusCodes.OK).forall(x => x)


    if (generateRandomTX) simulateTransactions()


    broadcast(PeerSyncHeartbeat(maxBundleMetaData.get, validLedger.toMap, id))
    //    apiBroadcast(_.post("peerSyncHeartbeat", PeerSyncHeartbeat(maxBundleMetaData.get, validLedger.toMap, id)))

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
      broadcastUDP(BatchBundleHashRequest(syncPendingBundleHashes))
    }

    // Request missing transaction data
    if (syncPendingTXHashes.nonEmpty) {
      // println("Requesting data sync pending of " + syncPendingTXHashes)
      broadcast(BatchTXHashRequest(syncPendingTXHashes))
      /*

            apiBroadcast({a: APIClient =>
              Future{
              a.postRead[Seq[Transaction]]("batchTXRequest", BatchTXHashRequest(syncPendingTXHashes)).foreach{
                self ! _
              } //.foreach{handleTransaction}
              }
            })
      */

      if (syncPendingTXHashes.size > 1500) {
        val toRemove = txSyncRequestTime.toSeq.sortBy(_._2).zipWithIndex.filter{_._2 > 50}.map{_._1._1}.toSet
        syncPendingTXHashes --= toRemove
      }

      // ask peers for this transaction data
      broadcastUDP(BatchTXHashRequest(syncPendingTXHashes))
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
      val memPoolEmit = Random.nextInt() < 0.04
      val filteredPool = memPool.diff(txInMaxBundleNotInValidation).filterNot(last10000ValidTXHash.contains)

      val memPoolConstantOffset = if (totalNumValidatedTX == 1) minGenesisDistrSize + 1 else 0
      val memPoolSelSize = Random.nextInt(4) + 1
      val memPoolSelection = Random.shuffle(filteredPool.toSeq)
        .slice(0, memPoolSelSize + memPoolConstantOffset)

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

        broadcastUDP(b)

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
    // val groupedBundles = activeDAGBundles.groupBy(b => b.bundle.extractParentBundleHash -> b.bundle.maxStackDepth)
    var toRemove = Set[Sheaf]()

    activeDAGManager.cellKeyToCell.filter{_._2.members.size > 1}.foreach{
      case (ck, cell) =>
        if (Random.nextDouble() > 0.1) {
          val best = cell.members.slice(0, 2)
          val allIds = best.flatMap{_.bundle.extractIds}.toSeq
          if (!allIds.contains(id)) {
            val b = Bundle(BundleData(best.map {
              _.bundle
            }.toSeq).signed())
            val maybeData = lookupBundle(ck.hashPointer)
            maybeData.foreach{ pbData =>

              updateBundleFrom(pbData, Sheaf(b))
              // Skip ids when depth below a certain amount, else tell everyone.
              // TODO : Fix ^
              broadcast(b, skipIDs = allIds)
              //apiBroadcast(_.post("rxBundle", b), skipIDs = allIds) // .foreach{println}
            }
          } else {
            toRemove ++= best.toSet
          }
        }


        if (cell.members.size > 10) {
          val toRemoveHere = cell.members.toSeq.sortBy(z => z.totalScore.get).zipWithIndex.filter{_._2 < 5}.map{_._1}.toSet
          toRemove ++= toRemoveHere
        }
    }

    if (toRemove.nonEmpty) {
      activeDAGManager.activeSheafs = activeDAGManager.activeSheafs.filterNot(toRemove.contains)
    }

  }

  def bundleCleanup(): Unit = {
    if (heartbeatRound % 30 == 0 && maxBundleMetaData.exists {
      _.height.exists {
        _ > 50
      }
    }) {
      //    println("Bundle cleanup")

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

        val heightOld = s.height.exists(h => h < (currentHeight - 35))
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
      }

      //  println("Bundle to sheaf cleanup done removed: " + numDeleted)

      // There should also be
    }
  }

  def cleanupStrayChains(): Unit = {
    activeDAGManager.cleanup(maxBundleMetaData.get.height.get)
    bundleCleanup()
  }

  // TODO: extract to test

  def simulateTransactions(): Unit = {
    val shouldEmit = maxBundleMetaData.exists {_.height.get >= 5} // || (System.currentTimeMillis() > startTime + 30000)
    if (shouldEmit && memPool.size < 500) {
      //if (Random.nextDouble() < .2)
      randomTransaction()
      randomTransaction()
      if (memPool.size < 50) Seq.fill(50)(randomTransaction())
    }
  }

}
