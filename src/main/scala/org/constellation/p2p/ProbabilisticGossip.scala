package org.constellation.p2p

import java.net.InetSocketAddress

import org.constellation.Data
import org.constellation.primitives.Schema._
import org.constellation.util.ProductHash
import constellation._

import scala.util.Random

trait ProbabilisticGossip extends PeerAuth with LinearGossip {

  val data: Data

  import data._

  def handleGossip(gm : GossipMessage, remote: InetSocketAddress): Unit = {
    totalNumGossipMessages += 1
    val rid = peerLookup.get(remote).map{_.data.id}
    gm match {
      case BatchBundleHashRequest(hashes) =>
        hashes.foreach{h =>
         /// println("Sending BundleMetaData to peer on request")
          lookupBundle(h).foreach{
            b =>
              udpActor.udpSend(b.bundle, remote)
          }
        }
      case BatchTXHashRequest(hashes) =>
        hashes.foreach{h =>
        //  println(s"Sending tx hash response $h")
          lookupTransaction(h).foreach{
            b =>
              udpActor.udpSend(b, remote)
          }
        }
      case b: Bundle =>
        handleBundle(b)
      case tx: TX =>
      //  println(s"Rx tx hash ${tx.short}")
        if (lookupTransaction(tx.hash).isEmpty) {
          storeTransaction(tx)
          numSyncedTX += 1
        }
        syncPendingTXHashes -= tx.hash
        txSyncRequestTime.remove(tx.hash)

      case bb: PeerSyncHeartbeat =>
        handleBundle(bb.maxBundle)
        rid.foreach{ r =>
          peerSync(r) = bb
        }

      case g : Gossip[ProductHash] =>
      // handleGossipRegular(g, remote)
      case x =>
        logger.debug("Unrecognized gossip message " + x)
    }
  }

  def gossipHeartbeat(): Unit = {
    if (!downloadMode && genesisBundle != null) {
      bundleHeartbeat()
    }
  }

  def simulateTransactions(): Unit = {
    if (maxBundleMetaData.height.get >= 5) {
      if (Random.nextDouble() < .2) randomTransaction()
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
      if (syncPendingTXHashes.size > 150) {
        val toRemove = txSyncRequestTime.toSeq.sortBy(_._2).zipWithIndex.filter{_._2 < 50}.map{_._1._1}.toSet
        syncPendingTXHashes --= toRemove
      }
    }
  }

  def poolEmit(): Unit = {
    // Emit an origin bundle. This needs to be managed by prob facil check on hash of previous + ids
    val memPoolEmit = Random.nextInt() < 0.3
    val filteredPool = memPool.diff(txInMaxBundleNotInValidation)
    val memPoolSelSize = 5 + Random.nextInt(50)
    val memPoolSelection = Random.shuffle(filteredPool.toSeq)
      .slice(0, memPoolSelSize + minGenesisDistrSize + 1)


    if (memPoolEmit && filteredPool.nonEmpty) {
     // println(s"Mempool emit on ${id.short}")
      val mb = maxBundle

      def pbHash = ParentBundleHash(mb.hash)

      val pbh = if (totalNumValidatedTX == 1) pbHash
      else if (mb.maxStackDepth < 2) maxBundle.extractParentBundleHash
      else pbHash

      val b = Bundle(
        BundleData(
          memPoolSelection.map {
            TransactionHash
          } :+ pbh
        ).signed()
      )
      val meta = mb.meta.get
      updateBundleFrom(meta, BundleMetaData(b))
      broadcast(b)
    }
  }

  def bundleHeartbeat(): Unit = {

    if (!downloadMode && genesisBundle != null && maxBundle != null) {


      // Force accept initial distribution
      if (totalNumValidatedTX == 1 && maxBundle.extractTX.size >= (minGenesisDistrSize - 1)) {
        maxBundle.extractTX.foreach{tx =>
          acceptTransaction(tx)
            tx.txData.data.updateLedger(memPoolLedger)
        }
        totalNumValidBundles += 1
        last100ValidBundleMetaData :+= maxBundleMetaData
      }

      peerSync.foreach{
        case (id, hb) =>
           if (hb.maxBundle.meta.exists(z => !z.isResolved)) {
             attemptResolveBundle(hb.maxBundle.meta.get, hb.maxBundle.extractParentBundleHash.pbHash)
           }
      }


      dataRequest()
      simulateTransactions()

      if (memPool.size > 500) {
        memPool = memPool.slice(0, 250)
      }

      broadcast(PeerSyncHeartbeat(maxBundle, validLedger.toMap))

      poolEmit()

      activeDAGBundles.groupBy(b => b.bundle.extractParentBundleHash -> b.bundle.maxStackDepth)
        .foreach { case (pbHash, bundles) =>
          //val sorted = bundles.sortBy(b => (b.idBelow.size, b.txBelow.size, b.hash))
          val random = Random.shuffle(bundles).slice(0, 5)
          if (bundles.size > 30) {
            //   toRemove = toRemove ++ Random.shuffle(bundles).slice(0, 10)
          }
          val iterator = random.combinations(2)
          iterator.foreach {
            case both@Seq(l, r) =>
              val hasNewTransactions =
                l.bundle.extractTXHash.diff(r.bundle.extractTXHash).nonEmpty
              val minTimeClose = Math.abs(l.rxTime - r.rxTime) < 40000
              if (Random.nextDouble() > 0.5 &&  hasNewTransactions && minTimeClose) { //
                val b = Bundle(BundleData(both.map{_.bundle}).signed())
                val pbData = lookupBundle(pbHash._1.pbHash).get
                updateBundleFrom(pbData, BundleMetaData(b))
                broadcast(b)
              }
          }
        }

      if (activeDAGBundles.size > 50) {
        activeDAGBundles = activeDAGBundles.sortBy(z => 1*z.totalScore.get).zipWithIndex.filter{_._2 < 30}.map{_._1}
      }

    }
  }


}
