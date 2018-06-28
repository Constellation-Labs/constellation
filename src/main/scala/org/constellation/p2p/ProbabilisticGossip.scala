package org.constellation.p2p

import java.net.InetSocketAddress

import org.constellation.Data
import org.constellation.primitives.Schema._
import org.constellation.util.ProductHash
import constellation._

import scala.collection.concurrent.TrieMap
import scala.util.{Random, Try}

trait ProbabilisticGossip extends PeerAuth with LinearGossip {

  val data: Data

  import data._

  def gossipHeartbeat(): Int = {
    heartbeatRound += 1
    if (!downloadMode) {
      bundleHeartbeat()
    }
    0

  }
  def acceptBundle(b: Bundle): Unit = {
    validBundles :+= b
    calculateReputationsFromScratch()
    b.extractTX.foreach{ t =>
      acceptTransaction(t)
    }
    activeDAGBundles = Seq()
  }


  def genesisCheck(): Unit = {

    // First node who did genesis
    val bbExist = Option(bestBundle).exists {_.txBelow.size >= minGenesisDistrSize}
    if ( genesisAdditionCheck && bbExist) acceptBundle(bestBundle)

    // Other peers
    if (lastBundleHash.pbHash == genesisBundle.hash && !(genesisBundle.extractIds.head == id)) {
      peerSync.get(genesisBundle.extractIds.head).foreach { b =>
        if (b.validBundleHashes.size == 2) {
          bundleHashToBundle.get(b.validBundleHashes.last).foreach { vb =>
            acceptBundle(vb)
          }
        }
      }
    }
  }

  def genesisAdditionCheck: Boolean =
    lastBundleHash.pbHash == genesisBundle.hash && // This is the first bundle creation attempt after genesis
      genesisBundle.extractIds.head == id // This node created the genesis bundle

  def extractTXHashUntilValidationHashReached(b: Bundle, txs: Set[String] = Set()): Set[String] = {
    val ph = b.extractParentBundleHash.pbHash
    if (validBundles.last.hash == ph) {
      txs ++ b.txBelow
    } else {
      extractTXHashUntilValidationHashReached(
        bundleHashToBundle(b.extractParentBundleHash.pbHash),
        txs ++ b.txBelow
      )}
  }

  def randomTransaction(): Unit = {
    val peerAddresses = peers.map{_.data.id.address}
    val randomPeer = Random.shuffle(peerAddresses).head
    createTransaction(randomPeer.address, Random.nextInt(1000).toLong)
  }

  def simulateTransactions(): Unit = {
    if (validBundles.size >= 5) {
      if (memPoolTX.size < 50) {
        Seq.fill(3){randomTransaction()}
      } else {
        randomTransaction()
      }
    }
  }

  def bundleHeartbeat(): Unit = {

    if (syncPendingBundleHashes.nonEmpty) {
      broadcast(BatchBundleHashRequest(syncPendingBundleHashes))
    }
    if (syncPendingTXHashes.nonEmpty) {
      broadcast(BatchTXHashRequest(syncPendingTXHashes))
    }


    if (!downloadMode) {

      simulateTransactions()

      activeDAGBundles = activeDAGBundles.sortBy(z => (-1 * z.meta.totalScore, z.hash))

      if (activeDAGBundles.size > 50) {
        activeDAGBundles = activeDAGBundles.zipWithIndex.filter{_._2 < 20}.map{_._1}
      }

      val bb: Option[Bundle] = if (genesisAdditionCheck) activeDAGBundles.headOption else {
        activeDAGBundles.find{_.maxStackDepth >= 2}
      }

      bb.foreach{bestBundle = _}

      broadcast(
        PeerSyncHeartbeat(
          bb,
          // memPoolTX,
          validBundles.map{_.hash}
        )
      )

      val txInBestBundleNewFromValidationHash = bb.map{b => extractTXHashUntilValidationHashReached(b)}.getOrElse(Set[String]())

      // || peers have no bundles / stalled.
      val memPoolEmit = Random.nextInt() < 0.2 // && (System.currentTimeMillis() < lastBundle.maxTime + 25000)
      val filteredMempool = memPool.diff(txInBestBundleNewFromValidationHash)

      def doMempool() = {
        // Emit an origin bundle. This needs to be managed by prob facil check on hash of previous + ids
        val memPoolSelSize = Random.nextInt(45)
        val memPoolSelection = Random.shuffle(filteredMempool.toSeq).slice(0, memPoolSelSize + minGenesisDistrSize + 1)
        val b = Bundle(
          BundleData(
            memPoolSelection.map{TransactionHash} :+
              bb.map{z => ParentBundleHash(z.hash)}.getOrElse(lastBundleHash)
          ).signed()
        )
        //     if (bb.nonEmpty) {
        //       activeDAGBundles = activeDAGBundles.filterNot(_ == bb.get)
        //     }
        processNewBundleMetadata(b, memPoolSelection.toSet.flatMap{m: String => db.getAs[TX](m)})
        broadcast(b)
      }

      if (validBundles.size < 2) {
        if (genesisAdditionCheck) doMempool()
      } else if (filteredMempool.nonEmpty && memPoolEmit) doMempool()


      genesisCheck()

      //  if (System.currentTimeMillis() < (lastBundle.maxTime + 10000)) {
      val nonSelfIdCandidates = activeDAGBundles.filter { b => !b.idBelow.contains(id) }
   //   logger.debug(s"Num nonSelfIdCandidates ${id.short} ${nonSelfIdCandidates.size}")

      nonSelfIdCandidates
        .groupBy(b => b.maxStackDepth -> b.extractParentBundleHash).foreach {
        case (_, bundles) =>
          //val sorted = bundles.sortBy(b => (b.idBelow.size, b.txBelow.size, b.hash))
          val random = Random.shuffle(bundles).slice(0, 5)
          val iterator = random.combinations(2)
          iterator.foreach {
            case both@Seq(l, r) =>
              val hasNewTransactions = l.txBelow.diff(r.txBelow).nonEmpty
              val minTimeClose = Math.abs(l.minTime - r.minTime) < 25000
              if (hasNewTransactions && minTimeClose && Random.nextDouble() > 0.5) {
                val b = Bundle(BundleData(both).signed())
                processNewBundleMetadata(b, b.extractTX)
                broadcast(b)
          //      println(s"Created new depth bundle ${b.maxStackDepth}")
              }
          }
      }

      val chainConfirmationLength = 5
      val chainAddLength = 2

      bb.foreach{ b =>

        val ancestors = extractBundleAncestorsUntilValidation(b)
        if (ancestors.length > chainConfirmationLength) {
          val chainToAdd = ancestors.slice(0, chainAddLength).map{bundleHashToBundle}
          val txToAdd = extractTXHashUntilValidationHashReached(chainToAdd.last).map{a => db.getAs[TX](a).get}
          txToAdd.foreach{
            t =>
              acceptTransaction(t)
          }
          val prvSize = validBundles.size
          validBundles ++= chainToAdd
          calculateReputationsFromScratch(prvSize-1)
        }

      }

      /*
          bb.foreach { b =>
            val numPeersWithValid = peerSync.values.count{p =>
              p.bundle.contains{b} || p.validBundleHashes.contains(b.hash)
            }
            val peerFraction = numPeersWithValid.toDouble / peerSync.keys.size.toDouble

            if (System.currentTimeMillis() > (b.minTime + 20000) && peerFraction >= 0.5) {
              acceptBundle(b)
            }
          }

          val missingHashes = peerSync.flatMap{
            case (id, ps) =>
              ps.validBundleHashes
          }.toSet.filter{ z =>
            !validBundles.map{_.hash}.contains(z)
          }

          missingHashes.foreach{ z =>
            broadcast(RequestBundleData(z))
          }
      */

    }
  }

  def squashBundle(b: Bundle): Bundle = {
    logger.debug(s"Squashing bundle - ${b.pretty}")
    val squashed = Bundle(BundleData(Seq(b.bundleHash)).signed())
    validBundles :+= b
    bestBundleBase = b
    bestBundleCandidateHashes += b.bundleHash
    lastSquashed = Some(squashed)
    // broadcast(squashed)
    squashed
  }

  // def handleGossip(g: Gossip[ProductHash], remote: InetSocketAddress): Unit = {
  def handleGossip(gm : GossipMessage, remote: InetSocketAddress): Unit = {

    val rid = peerLookup.get(remote).map{_.data.id}

    gm match {

      case BatchBundleHashRequest(hashes) =>

        hashes.foreach{h =>
          db.getAs[Bundle](h).foreach{
            b =>
              udpActor.udpSend(b, remote)
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
          numSyncedTX += 1
        }
        syncPendingTXHashes -= tx.hash
        db.put(tx)

      case bb: PeerSyncHeartbeat =>
        //    println(s"RECEIVED PEER SYNC OF MEMPOOL SIZE ${bb.memPool.size}")
        bb.bundle.foreach{b => handleBundle(b)}
        rid.foreach{ r =>
          peerSync(r) = bb
        }
      //  if (validateTXBatch(bb.memPool)) {
      //    bb.memPool.foreach{updateMempool}
      //}


      case g : Gossip[ProductHash] =>
      // handleGossipRegular(g, remote)
      case x =>

        logger.debug("Unrecognized gossip message " + x)
    }
  }



}
