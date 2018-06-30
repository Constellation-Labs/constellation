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
          db.getAs[BundleMetaData](h).foreach{
            b =>
              udpActor.udpSend(b.bundle, remote)
          }
        }
      case BatchTXHashRequest(hashes) =>
        hashes.foreach{h =>
          db.getAs[TX](h).foreach{
            b =>
              udpActor.udpSend(b, remote)
          }
        }
      case b: Bundle => handleBundle(b)
      case tx: TX =>
        if (!db.contains(tx)) {
          syncPendingTXHashes -= tx.hash
          db.put(tx)
          numSyncedTX += 1
        }
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
      if (memPool.size < 50) {
        Seq.fill(3){randomTransaction()}
      } else {
        if (Random.nextDouble() < .1) randomTransaction()
      }
    }
  }

  def dataRequest(): Unit = {
    if (syncPendingBundleHashes.nonEmpty) {
      broadcast(BatchBundleHashRequest(syncPendingBundleHashes))
    }
    if (syncPendingTXHashes.nonEmpty) {
      broadcast(BatchTXHashRequest(syncPendingTXHashes))
    }
  }

  def seedBundleZero(): Unit = {
    // Emit an origin bundle. This needs to be managed by prob facil check on hash of previous + ids
    val memPoolEmit = Random.nextInt() < 0.1
    val filteredPool = memPool.diff(txInMaxBundleNotInValidation)
    val memPoolSelSize = 5 + Random.nextInt(50)
    val memPoolSelection = Random.shuffle(filteredPool.toSeq)
      .slice(0, memPoolSelSize + minGenesisDistrSize + 1)

    if (memPoolEmit && filteredPool.nonEmpty) {
      val mb = maxBundle
      val b = Bundle(
        BundleData(
          memPoolSelection.map {
            TransactionHash
          } :+ ParentBundleHash(mb.hash)
        ).signed()
      )
      val meta = mb.meta.get
      updateBundleFrom(meta, BundleMetaData(b))
      broadcast(b)
    }
  }

  def bundleHeartbeat(): Unit = {

    if (!downloadMode && genesisBundle != null) {

      dataRequest()
      simulateTransactions()

      if (memPool.size > 500) {
        memPool = memPool.slice(0, 250)
      }

      broadcast(PeerSyncHeartbeat(maxBundle))

      seedBundleZero()

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
                val pbData = db.getAs[BundleMetaData](pbHash._1.pbHash).get
                updateBundleFrom(pbData, BundleMetaData(b))
                broadcast(b)
              }
          }
        }

      if (activeDAGBundles.size > 50) {
        activeDAGBundles = activeDAGBundles.sortBy(_.totalScore.get).zipWithIndex.filter{_._2 < 30}.map{_._1}
      }

    }
  }


}
